terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

resource "aws_s3_bucket" "gridoscope" {
  bucket = var.bucket_name

  tags = {
    Name = var.bucket_name
  }
}

# Block all public access — this bucket is only accessed by Kafka Connect
# (writes), Airflow/Snowflake (reads), and the MSK Connect custom plugin.
resource "aws_s3_bucket_public_access_block" "gridoscope" {
  bucket = aws_s3_bucket.gridoscope.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# SSE-S3 (AES256) — encrypts at rest without the cost/complexity of KMS.
resource "aws_s3_bucket_server_side_encryption_configuration" "gridoscope" {
  bucket = aws_s3_bucket.gridoscope.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Raw event data is loaded into Snowflake within hours of landing.
# After 90 days it's cold — transition to IA to cut storage cost.
resource "aws_s3_bucket_lifecycle_configuration" "gridoscope" {
  bucket = aws_s3_bucket.gridoscope.id

  rule {
    id     = "raw-to-ia"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }
  }
}
