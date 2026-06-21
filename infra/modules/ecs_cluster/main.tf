# ecs_cluster module main.tf

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

data "aws_caller_identity" "current" {}

resource "aws_ecs_cluster" "this" {
  name = "gridoscope-${var.environment}"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = {
    Name = "gridoscope-${var.environment}"
  }
}

# Shared by both the producer and consumer task definitions — this is what
# the ECS agent itself assumes to pull images and ship logs, distinct from
# each service's own task role (which is what the application code can do).
resource "aws_iam_role" "execution" {
  name = "gridoscope-${var.environment}-ecs-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = {
    Name = "gridoscope-${var.environment}-ecs-execution"
  }
}

resource "aws_iam_role_policy_attachment" "execution_managed" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# Shared by both Fargate services — no inbound rules at all, since nothing
# calls the producer or consumer. The ingress side of the MSK conversation
# (allowing this SG to reach the broker port) is wired from the root module,
# where both this SG's id and the MSK SG's id are visible without creating
# a module dependency cycle.
resource "aws_security_group" "ecs_tasks" {
  name        = "gridoscope-${var.environment}-ecs-tasks"
  description = "Gridoscope ECS Fargate tasks (producer, consumer) - egress only"
  vpc_id      = var.vpc_id

  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "gridoscope-${var.environment}-ecs-tasks"
  }
}

# The managed policy above covers ECR pulls and log creation, but not reading
# secrets injected via the task definition's `secrets` block (e.g. the Power
# BI push URL) — that needs an explicit grant scoped to this project's params.
resource "aws_iam_role_policy" "execution_ssm" {
  name = "gridoscope-${var.environment}-ecs-execution-ssm"
  role = aws_iam_role.execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "ssm:GetParameters",
        "ssm:GetParameter",
      ]
      Resource = "arn:aws:ssm:${var.aws_region}:${data.aws_caller_identity.current.account_id}:parameter${var.ssm_parameter_path_prefix}/*"
    }]
  })
}
