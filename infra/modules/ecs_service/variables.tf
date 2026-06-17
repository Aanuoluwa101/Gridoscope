variable "environment" {
  description = "Deployment environment (dev, prod)"
  type        = string
}

variable "service_name" {
  description = "Short service name, e.g. \"producer\" or \"consumer\" — used in naming everything"
  type        = string
}

variable "image_uri" {
  description = "Full ECR image URI including tag"
  type        = string
}

variable "command" {
  description = "Container command, e.g. [\"python\", \"producers/engine.py\"]"
  type        = list(string)
}

variable "cpu" {
  description = "Fargate task CPU units (256, 512, 1024, ...)"
  type        = string
}

variable "memory" {
  description = "Fargate task memory (MB)"
  type        = string
}

variable "desired_count" {
  description = "Number of task instances to run"
  type        = number
}

variable "cluster_id" {
  description = "ECS cluster ID"
  type        = string
}

variable "execution_role_arn" {
  description = "Shared ECS task execution role ARN"
  type        = string
}

variable "subnet_ids" {
  description = "Private subnet IDs the task ENIs are placed in"
  type        = list(string)
}

variable "security_group_id" {
  description = "Security group ID attached to the task ENI"
  type        = string
}

variable "aws_region" {
  description = "AWS region — passed into the container as an env var and used for the log group"
  type        = string
}

variable "msk_cluster_arn" {
  description = "MSK cluster ARN — scopes the Connect/DescribeCluster IAM grant"
  type        = string
}

variable "msk_bootstrap_brokers" {
  description = "MSK IAM bootstrap broker string — injected as KAFKA_BOOTSTRAP_SERVERS"
  type        = string
}

variable "kafka_topic_actions" {
  description = "kafka-cluster:* actions this service needs on topic resources, e.g. [\"kafka-cluster:WriteData\"]"
  type        = list(string)
  default     = []
}

variable "kafka_group_actions" {
  description = "kafka-cluster:* actions this service needs on consumer-group resources, e.g. [\"kafka-cluster:ReadData\", \"kafka-cluster:AlterGroup\"]"
  type        = list(string)
  default     = []
}

variable "environment_variables" {
  description = "Additional plaintext env vars for the container, merged with the Kafka connection vars"
  type        = map(string)
  default     = {}
}

variable "secrets" {
  description = "Secrets injected from SSM Parameter Store, e.g. [{name = \"POWERBI_PUSH_URL\", value_from = aws_ssm_parameter.x.arn}]"
  type = list(object({
    name       = string
    value_from = string
  }))
  default = []
}

variable "log_retention_days" {
  description = "CloudWatch log group retention"
  type        = number
  default     = 7
}
