variable "environment" {
  description = "Deployment environment (dev, prod)"
  type        = string
}

variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID — used to create the Connect worker security group"
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs the Connect workers' ENIs are placed in"
  type        = list(string)
}

variable "msk_cluster_arn" {
  description = "MSK cluster ARN — scopes the connector's Kafka IAM policy"
  type        = string
}

variable "msk_bootstrap_brokers_iam" {
  description = "MSK IAM bootstrap broker string"
  type        = string
}

variable "readings_topic" {
  description = "Kafka topic the connector reads from"
  type        = string
  default     = "meter.readings"
}

variable "bucket_name" {
  description = "S3 bucket the connector writes raw readings to (from the storage module)"
  type        = string
}

variable "bucket_arn" {
  description = "ARN of the S3 bucket the connector writes to (from the storage module)"
  type        = string
}

variable "plugin_s3_bucket" {
  description = "S3 bucket holding the Confluent S3 sink connector plugin zip — must be uploaded before terraform apply, see README"
  type        = string
}

variable "plugin_s3_key" {
  description = "S3 key of the connector plugin zip within plugin_s3_bucket"
  type        = string
}

variable "kafkaconnect_version" {
  description = "MSK Connect framework version (distinct from the connector plugin version) — check AWS docs for currently supported values"
  type        = string
  default     = "2.7.1"
}

variable "mcu_count" {
  description = "MSK Connect Capacity Units (MCU) per worker"
  type        = number
  default     = 1
}

variable "min_worker_count" {
  description = "Minimum number of Connect workers"
  type        = number
  default     = 1
}

variable "max_worker_count" {
  description = "Maximum number of Connect workers"
  type        = number
  default     = 2
}

variable "log_retention_days" {
  description = "CloudWatch log group retention for Connect worker logs"
  type        = number
  default     = 7
}
