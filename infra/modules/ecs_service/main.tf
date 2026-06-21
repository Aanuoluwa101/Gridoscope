# ecs_service module main.tf

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

locals {
  name_prefix = "gridoscope-${var.environment}-${var.service_name}"

  # MSK resource ARNs for topics/groups are derived from the cluster ARN by
  # swapping the resource type segment — there's no separate "topic ARN"
  # attribute exposed anywhere, this is the documented pattern.
  topic_arn_pattern = "${replace(var.msk_cluster_arn, ":cluster/", ":topic/")}/*"
  group_arn_pattern = "${replace(var.msk_cluster_arn, ":cluster/", ":group/")}/*"

  kafka_policy_statements = concat(
    [{
      Effect   = "Allow"
      Action   = ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster"]
      Resource = [var.msk_cluster_arn]
    }],
    length(var.kafka_topic_actions) > 0 ? [{
      Effect   = "Allow"
      Action   = var.kafka_topic_actions
      Resource = [local.topic_arn_pattern]
    }] : [],
    length(var.kafka_group_actions) > 0 ? [{
      Effect   = "Allow"
      Action   = var.kafka_group_actions
      Resource = [local.group_arn_pattern]
    }] : [],
  )

  container_env = merge(
    {
      KAFKA_BOOTSTRAP_SERVERS = var.msk_bootstrap_brokers
      AWS_REGION              = var.aws_region
    },
    var.environment_variables,
  )
}

resource "aws_cloudwatch_log_group" "this" {
  name              = "/ecs/${local.name_prefix}"
  retention_in_days = var.log_retention_days

  tags = {
    Name = local.name_prefix
  }
}

# Per-service task role — this is what the application code itself can do
# at runtime (distinct from the shared execution role, which is what the
# ECS agent does to launch the task). Producer gets write-only Kafka
# actions, consumer gets read + consumer-group actions.
resource "aws_iam_role" "task" {
  name = "${local.name_prefix}-task"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = {
    Name = "${local.name_prefix}-task"
  }
}

resource "aws_iam_role_policy" "kafka" {
  name = "${local.name_prefix}-kafka"
  role = aws_iam_role.task.id

  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = local.kafka_policy_statements
  })
}

resource "aws_ecs_task_definition" "this" {
  family                   = local.name_prefix
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.cpu
  memory                   = var.memory
  execution_role_arn       = var.execution_role_arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([{
    name      = var.service_name
    image     = var.image_uri
    essential = true
    command   = var.command

    environment = [
      for k, v in local.container_env : { name = k, value = v }
    ]

    secrets = [
      for s in var.secrets : { name = s.name, valueFrom = s.value_from }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.this.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = var.service_name
      }
    }
  }])

  tags = {
    Name = local.name_prefix
  }
}

resource "aws_ecs_service" "this" {
  name            = local.name_prefix
  cluster         = var.cluster_id
  task_definition = aws_ecs_task_definition.this.arn
  desired_count   = var.desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.subnet_ids
    security_groups  = [var.security_group_id]
    assign_public_ip = false
  }

  tags = {
    Name = local.name_prefix
  }
}
