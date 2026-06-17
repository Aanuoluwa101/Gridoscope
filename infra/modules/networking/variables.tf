variable "environment" {
  description = "Deployment environment (dev, prod)"
  type        = string
}

variable "aws_region" {
  description = "AWS region — used to name the S3 VPC endpoint service"
  type        = string
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.20.0.0/16"
}

variable "azs" {
  description = "Availability zones to spread subnets across — one MSK broker and one set of ECS tasks per AZ"
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b", "us-east-1c"]
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets (one per AZ) — only used by the NAT Gateway"
  type        = list(string)
  default     = ["10.20.0.0/24", "10.20.1.0/24", "10.20.2.0/24"]
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private subnets (one per AZ) — MSK brokers and ECS tasks live here"
  type        = list(string)
  default     = ["10.20.10.0/24", "10.20.11.0/24", "10.20.12.0/24"]
}
