terraform {
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 1.0"
    }
  }
}


# The S3 bucket
resource "aws_s3_bucket" "gridoscope" {
  bucket = var.bucket_name
}