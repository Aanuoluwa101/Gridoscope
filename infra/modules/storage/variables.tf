variable "bucket_name" {
  description = "S3 bucket name for raw event data and Kafka Connect plugin artifacts"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, prod)"
  type        = string
}
