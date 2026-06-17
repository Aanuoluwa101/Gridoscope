variable "environment" {
  description = "Deployment environment (dev, prod)"
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs — one broker is placed per subnet/AZ"
  type        = list(string)
}

variable "vpc_id" {
  description = "VPC ID — used to create the MSK broker security group"
  type        = string
}

variable "kafka_version" {
  description = "MSK Kafka version"
  type        = string
  default     = "3.6.0"
}

variable "broker_instance_type" {
  description = "MSK broker instance type"
  type        = string
  default     = "kafka.t3.small"
}

variable "broker_count" {
  description = "Number of broker nodes — must be a multiple of the number of subnets, one broker per AZ"
  type        = number
  default     = 3
}

variable "ebs_volume_size" {
  description = "EBS volume size (GB) per broker"
  type        = number
  default     = 20
}
