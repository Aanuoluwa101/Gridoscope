# mwaa module main.tf

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  name = "gridoscope-${var.environment}"
}

# ---------------------------------------------------------------------------
# Security Group
# ---------------------------------------------------------------------------

resource "aws_security_group" "mwaa" {
  name        = "${local.name}-mwaa"
  description = "MWAA workers - self-referencing ingress for worker-to-worker traffic"
  vpc_id      = var.vpc_id

  # Workers communicate internally (Celery broker SQS calls, metadata DB).
  # MWAA also requires the SG to allow all intra-SG traffic.
  ingress {
    description = "Worker-to-worker"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  egress {
    description = "All outbound - Snowflake (via NAT), S3 endpoint, CloudWatch"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${local.name}-mwaa"
  }
}

# ---------------------------------------------------------------------------
# IAM execution role
# ---------------------------------------------------------------------------

resource "aws_iam_role" "mwaa" {
  name = "${local.name}-mwaa-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = ["airflow.amazonaws.com", "airflow-env.amazonaws.com"] }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = { Name = "${local.name}-mwaa-execution" }
}

resource "aws_iam_role_policy" "mwaa" {
  name = "${local.name}-mwaa-execution"
  role = aws_iam_role.mwaa.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # Required for MWAA to publish environment health metrics.
        Effect   = "Allow"
        Action   = "airflow:PublishMetrics"
        Resource = "arn:aws:airflow:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:environment/${local.name}"
      },
      {
        # Required for MWAA to verify bucket public-access settings before
        # reading DAG/requirements files.
        Effect   = "Allow"
        Action   = "s3:GetAccountPublicAccessBlock"
        Resource = "*"
      },
      {
        # DAGs, requirements.txt, and the dbt project all live in this bucket.
        Effect   = "Allow"
        Action   = ["s3:GetObject*", "s3:GetBucket*", "s3:List*"]
        Resource = [var.bucket_arn, "${var.bucket_arn}/*"]
      },
      {
        # Broad log permissions — MWAA creates log groups with the prefix
        # "airflow-{environment-name}".
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:GetLogEvents",
          "logs:GetLogRecord",
          "logs:DescribeLogGroups",
          "logs:GetLogDelivery",
          "logs:ListLogDeliveries",
          "logs:UpdateLogDelivery",
          "logs:CreateLogDelivery",
          "logs:PutRetentionPolicy",
        ]
        Resource = [
          "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:airflow-${local.name}-*",
          "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:airflow-${local.name}-*:log-stream:*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = "cloudwatch:PutMetricData"
        Resource = "*"
      },
      {
        # MWAA uses SQS internally for the Celery task queue.
        Effect = "Allow"
        Action = [
          "sqs:ChangeMessageVisibility",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:GetQueueUrl",
          "sqs:ReceiveMessage",
          "sqs:SendMessage",
        ]
        Resource = "arn:aws:sqs:${data.aws_region.current.name}:*:airflow-celery-*"
      },
      {
        # Secrets Manager backend — Snowflake connection lives at
        # airflow/connections/gridoscope_snowflake_prod.
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret",
          "secretsmanager:ListSecrets",
        ]
        Resource = "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:airflow/*"
      },
    ]
  })
}

# ---------------------------------------------------------------------------
# Secrets Manager — Snowflake connection
# ---------------------------------------------------------------------------

# MWAA SecretsManagerBackend looks up connections at
# {connections_prefix}/{conn_id} — here: airflow/connections/gridoscope_snowflake_prod.
resource "aws_secretsmanager_secret" "snowflake_conn" {
  name        = "airflow/connections/gridoscope_snowflake_prod"
  description = "Airflow Snowflake connection for Gridoscope MWAA (${var.environment})"

  tags = { Name = "${local.name}-snowflake-conn" }
}

resource "aws_secretsmanager_secret_version" "snowflake_conn" {
  secret_id = aws_secretsmanager_secret.snowflake_conn.id

  secret_string = jsonencode({
    conn_type = "snowflake"
    login     = var.snowflake_airflow_user  # User used for copy into raw. We call it airflow_user because airflow itself connects to snowflake and runs the command, not through dbt
    password  = var.snowflake_airflow_password  
    host      = "${var.snowflake_organization}-${var.snowflake_account}"
    schema    = "RAW"
    extra = jsonencode({
      account   = "${var.snowflake_organization}-${var.snowflake_account}"
      warehouse = "COMPUTE_WH"
      role      = "GRIDOSCOPE_LOADER"
      database  = var.snowflake_database
    })
  })
}

resource "aws_secretsmanager_secret" "snowflake_dbt_conn" {
  name        = "airflow/connections/gridoscope_snowflake_dbt_prod"
  description = "Airflow Snowflake connection for dbt (GRIDOSCOPE_TRANSFORMER role)"

  tags = { Name = "${local.name}-snowflake-dbt-conn" }
}

resource "aws_secretsmanager_secret_version" "snowflake_dbt_conn" {
  secret_id = aws_secretsmanager_secret.snowflake_dbt_conn.id

  secret_string = jsonencode({
    conn_type = "snowflake"
    login     = var.snowflake_dbt_user       # User used for running dbt commands. Airflow still does the using but it is specifically for dbt commands
    password  = var.snowflake_dbt_password   
    host      = "${var.snowflake_organization}-${var.snowflake_account}"
    schema    = "STAGING"
    extra = jsonencode({
      account   = "${var.snowflake_organization}-${var.snowflake_account}"
      warehouse = "COMPUTE_WH"
      role      = "GRIDOSCOPE_TRANSFORMER"
      database  = var.snowflake_database
    })
  })
}

# ---------------------------------------------------------------------------
# MWAA Environment
# ---------------------------------------------------------------------------

resource "aws_mwaa_environment" "this" {
  name              = local.name
  airflow_version   = var.airflow_version
  environment_class = var.environment_class
  execution_role_arn = aws_iam_role.mwaa.arn

  source_bucket_arn    = var.bucket_arn
  dag_s3_path          = "mwaa/dags"
  requirements_s3_path = "mwaa/requirements.txt"

  min_workers = var.min_workers
  max_workers = var.max_workers
  schedulers  = var.schedulers

  # PUBLIC_ONLY: web server accessible via a public HTTPS URL (AWS-managed).
  # Workers still run in private subnets — only the web server is public.
  webserver_access_mode = "PUBLIC_ONLY"

  network_configuration {
    security_group_ids = [aws_security_group.mwaa.id]
    # MWAA requires exactly 2 private subnets in different AZs.
    subnet_ids = slice(var.private_subnet_ids, 0, 2)
  }

  airflow_configuration_options = {
    # Resolve Airflow connections and variables from Secrets Manager.
    "secrets.backend"        = "airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend"
    "secrets.backend_kwargs" = jsonencode({ connections_prefix = "airflow/connections", variables_prefix = "airflow/variables" })
  }

  logging_configuration {
    dag_processing_logs {
      enabled   = true
      log_level = "INFO"
    }
    scheduler_logs {
      enabled   = true
      log_level = "INFO"
    }
    task_logs {
      enabled   = true
      log_level = "INFO"
    }
    webserver_logs {
      enabled   = true
      log_level = "INFO"
    }
    worker_logs {
      enabled   = true
      log_level = "INFO"
    }
  }

  tags = { Name = local.name }

  depends_on = [aws_iam_role_policy.mwaa]
}
