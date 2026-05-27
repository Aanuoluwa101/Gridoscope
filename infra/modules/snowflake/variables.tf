variable "environment" {
  description = "Deployment environment (dev, prod)"
  type        = string
}

variable "bucket_name" {
  description = "S3 bucket name — passed from storage module"
  type        = string
}

variable "bucket_arn" {
  description = "S3 bucket ARN — passed from storage module"
  type        = string
}

variable "snowflake_database" {
  description = "Snowflake database name"
  type        = string
}

variable "snowflake_schema" {
  description = "Snowflake schema for the external stage"
  type        = string
  default     = "RAW"
}

variable "iam_role_name" {
  description = "Name of the IAM role Snowflake assumes to access S3"
  type        = string
}

