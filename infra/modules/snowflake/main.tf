terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 1.0"
    }
  }
}

data "aws_caller_identity" "current" {}


resource "snowflake_storage_integration" "s3_int" {
  name                      = "S3_GRIDOSCOPE_INT_${upper(var.environment)}"
  type                      = "EXTERNAL_STAGE"
  enabled                   = true
  storage_provider          = "S3"
  storage_allowed_locations = ["s3://${var.bucket_name}/"]
  storage_aws_role_arn      = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.iam_role_name}"
}


resource "snowflake_stage" "gridoscope_stage" {
  name                = "GRIDOSCOPE_STAGE_${upper(var.environment)}"
  database            = var.snowflake_database
  schema              = var.snowflake_schema
  storage_integration = snowflake_storage_integration.s3_int.name
  url                 = "s3://${var.bucket_name}/"
}


resource "aws_iam_role" "gridoscope_snowflake_role" {
  name = var.iam_role_name

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        AWS = snowflake_storage_integration.s3_int.storage_aws_iam_user_arn
      }
      Action = "sts:AssumeRole"
      Condition = {
        StringEquals = {
          "sts:ExternalId" = snowflake_storage_integration.s3_int.storage_aws_external_id
        }
      }
    }]
  })
}


resource "aws_iam_role_policy" "gridoscope_snowflake_s3_policy" {
  role = aws_iam_role.gridoscope_snowflake_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:ListBucket"
      ]
      Resource = [
        var.bucket_arn,
        "${var.bucket_arn}/*"
      ]
    }]
  })
}