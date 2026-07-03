terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

locals {
  name_prefix = "gridoscope-${var.environment}-s3-sink"

  # Same ARN-substitution pattern as modules/ecs_service — there's no
  # separate "topic ARN" attribute, you derive it from the cluster ARN.
  topic_arn_pattern = "${replace(var.msk_cluster_arn, ":cluster/", ":topic/")}/*"
  group_arn_pattern = "${replace(var.msk_cluster_arn, ":cluster/", ":group/")}/*"
}

# No inbound rules — Connect workers only make outbound connections to MSK
# and S3. The ingress side (allowing this SG to reach the MSK broker port)
# is wired from the root module alongside the equivalent ECS tasks rule.
resource "aws_security_group" "connect" {
  name        = "${local.name_prefix}-connect"
  description = "Gridoscope MSK Connect workers - egress only"
  vpc_id      = var.vpc_id

  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${local.name_prefix}-connect"
  }
}

resource "aws_cloudwatch_log_group" "connect" {
  name              = "/msk-connect/${local.name_prefix}"
  retention_in_days = var.log_retention_days

  tags = {
    Name = local.name_prefix
  }
}

resource "aws_iam_role" "connect" {
  name = "${local.name_prefix}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "kafkaconnect.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = {
    Name = "${local.name_prefix}-role"
  }
}

resource "aws_iam_role_policy" "connect_kafka" {
  name = "${local.name_prefix}-kafka"
  role = aws_iam_role.connect.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster"]
        Resource = [var.msk_cluster_arn]
      },
      {
        Effect = "Allow"
        Action = [
          "kafka-cluster:ReadData",
          "kafka-cluster:WriteData",
          "kafka-cluster:DescribeTopic",
          "kafka-cluster:CreateTopic",
          "kafka-cluster:AlterTopic",
        ]
        Resource = [local.topic_arn_pattern]
      },
      {
        Effect   = "Allow"
        Action   = ["kafka-cluster:AlterGroup", "kafka-cluster:DescribeGroup"]
        Resource = [local.group_arn_pattern]
      },
    ]
  })
}

resource "aws_iam_role_policy" "connect_s3" {
  name = "${local.name_prefix}-s3"
  role = aws_iam_role.connect.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [var.bucket_arn]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]
        Resource = ["${var.bucket_arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["arn:aws:s3:::${var.plugin_s3_bucket}/${var.plugin_s3_key}"]
      },
    ]
  })
}

resource "aws_iam_role_policy" "connect_logs" {
  name = "${local.name_prefix}-logs"
  role = aws_iam_role.connect.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["logs:CreateLogStream", "logs:PutLogEvents", "logs:CreateLogGroup"]
      Resource = ["${aws_cloudwatch_log_group.connect.arn}:*"]
    }]
  })
}

# The plugin zip itself is not managed by Terraform — it's a build artifact
# (the Confluent S3 connector jar bundle), not config. Upload it to
# plugin_s3_bucket/plugin_s3_key once before the first apply; see the infra
# README for the exact confluent-hub download + zip steps.
resource "aws_mskconnect_custom_plugin" "s3_sink" {
  name         = "${local.name_prefix}-plugin"
  content_type = "ZIP"

  location {
    s3 {
      bucket_arn = "arn:aws:s3:::${var.plugin_s3_bucket}"
      file_key   = var.plugin_s3_key
    }
  }
}

resource "aws_mskconnect_worker_configuration" "this" {
  name = "${local.name_prefix}-worker"

  properties_file_content = <<-EOT
    key.converter=org.apache.kafka.connect.storage.StringConverter
    value.converter=org.apache.kafka.connect.storage.StringConverter
  EOT
}

resource "aws_mskconnect_connector" "s3_sink" {
  name                       = "${local.name_prefix}-readings"
  kafkaconnect_version       = var.kafkaconnect_version
  service_execution_role_arn = aws_iam_role.connect.arn

  capacity {
    autoscaling {
      mcu_count        = var.mcu_count
      min_worker_count = var.min_worker_count
      max_worker_count = var.max_worker_count

      scale_in_policy {
        cpu_utilization_percentage = 20
      }
      scale_out_policy {
        cpu_utilization_percentage = 80
      }
    }
  }

  # Carried over almost verbatim from the local s3_sink_readings.json —
  # same connector class, partitioner, and path layout. tasks.max=5 matches
  # one task per zone/partition, same as the local dev connector.
  connector_configuration = {
    "connector.class"       = "io.confluent.connect.s3.S3SinkConnector"
    "tasks.max"             = "5"
    "topics"                = var.readings_topic
    "s3.region"             = var.aws_region
    "s3.bucket.name"        = var.bucket_name
    "s3.part.size"          = "67108864"
    "storage.class"         = "io.confluent.connect.s3.storage.S3Storage"
    "format.class"          = "io.confluent.connect.s3.format.json.JsonFormat"
    "flush.size"                   = "10"
    "rotate.interval.ms"           = "30000"
    "rotate.schedule.interval.ms"  = "30000"
    "partitioner.class"     = "io.confluent.connect.storage.partitioner.TimeBasedPartitioner"
    "timestamp.extractor"   = "Record"
    "path.format"           = "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH"
    "partition.duration.ms" = "3600000"
    "topics.dir"            = "raw"
    "key.converter"         = "org.apache.kafka.connect.storage.StringConverter"
    "value.converter"       = "org.apache.kafka.connect.storage.StringConverter"
    "locale"                = "en-US"
    "timezone"              = "UTC"
  }

  kafka_cluster {
    apache_kafka_cluster {
      bootstrap_servers = var.msk_bootstrap_brokers_iam

      vpc {
        security_groups = [aws_security_group.connect.id]
        subnets         = var.private_subnet_ids
      }
    }
  }

  kafka_cluster_client_authentication {
    authentication_type = "IAM"
  }

  kafka_cluster_encryption_in_transit {
    encryption_type = "TLS"
  }

  plugin {
    custom_plugin {
      arn      = aws_mskconnect_custom_plugin.s3_sink.arn
      revision = aws_mskconnect_custom_plugin.s3_sink.latest_revision
    }
  }

  worker_configuration {
    arn      = aws_mskconnect_worker_configuration.this.arn
    revision = aws_mskconnect_worker_configuration.this.latest_revision
  }

  log_delivery {
    worker_log_delivery {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.connect.name
      }
    }
  }

  depends_on = [
    aws_iam_role_policy.connect_kafka,
    aws_iam_role_policy.connect_s3,
    aws_iam_role_policy.connect_logs,
  ]
}
