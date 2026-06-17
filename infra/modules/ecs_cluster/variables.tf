variable "environment" {
  description = "Deployment environment (dev, prod)"
  type        = string
}

variable "ssm_parameter_path_prefix" {
  description = "SSM Parameter Store path prefix the task execution role may read secrets from, e.g. /gridoscope/dev"
  type        = string
}

variable "aws_region" {
  description = "AWS region — used to scope the SSM read policy's ARN"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID — used to create the security group shared by producer/consumer Fargate tasks"
  type        = string
}
