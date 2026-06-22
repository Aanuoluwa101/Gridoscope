variable "environment" {
  description = "Deployment environment (dev, prod)"
  type        = string
}

variable "aws_region" {
  description = "AWS region — used to build ARNs in IAM policies"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID for the MWAA security group"
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs — MWAA uses the first 2 (must be in different AZs)"
  type        = list(string)
}

variable "bucket_arn" {
  description = "ARN of the S3 bucket holding DAGs, requirements.txt, and the dbt project"
  type        = string
}

variable "snowflake_organization" {
  description = "Snowflake organization name — combined with account to form the connection host"
  type        = string
}

variable "snowflake_account" {
  description = "Snowflake account identifier"
  type        = string
}

variable "snowflake_airflow_user" {
  description = "Snowflake user for the Airflow Snowflake connection"
  type        = string
}


variable "snowflake_airflow_password" {
  description = "password for snowflake_airflow_user"
  type        = string
  sensitive   = true
}

variable "snowflake_dbt_user" {
  description = "Snowflake user for the Airflow Snowflake dbt connection"
  type        = string
}


variable "snowflake_dbt_password" {
  description = "password for snowflake_dbt_user"
  type        = string
  sensitive   = true
}

variable "snowflake_database" {
  description = "Snowflake database name (e.g. GRIDOSCOPE_PROD)"
  type        = string
}

variable "airflow_version" {
  description = "Apache Airflow version supported by MWAA"
  type        = string
  default     = "2.10.3"
}

variable "environment_class" {
  description = "MWAA environment class — mw1.small is sufficient for a single-pipeline project"
  type        = string
  default     = "mw1.small"
}

variable "min_workers" {
  description = "Minimum number of Celery workers"
  type        = number
  default     = 1
}

variable "max_workers" {
  description = "Maximum number of Celery workers"
  type        = number
  default     = 2
}

variable "schedulers" {
  description = "Number of Airflow schedulers"
  type        = number
  default     = 2
}
