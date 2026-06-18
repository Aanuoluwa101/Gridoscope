variable "aws_region" {
  description = "AWS region to deploy resources in"
  type        = string
}


variable "snowflake_organization" {
  description = "Snowflake organization identifier"
  type        = string
}


variable "snowflake_account" {
  description = "Snowflake account identifier"
  type        = string
}

variable "snowflake_username" {
  description = "Snowflake username with permissions to create storage integrations and stages"
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


variable "environment" {
  description = "Deployment environment (dev, prod)"
  type        = string
}

variable "bucket_name" {
  description = "S3 bucket name — passed from storage module"
  type        = string
}


# ---------------------------------------------------------------------------
# MSK
# ---------------------------------------------------------------------------

variable "msk_kafka_version" {
  description = "MSK Kafka version"
  type        = string
  default     = "3.6.0"
}

variable "msk_broker_instance_type" {
  description = "MSK broker instance type"
  type        = string
  default     = "kafka.t3.small"
}

variable "msk_broker_count" {
  description = "Number of MSK broker nodes — one per AZ"
  type        = number
  default     = 3
}

variable "msk_ebs_volume_size" {
  description = "EBS volume size (GB) per MSK broker"
  type        = number
  default     = 20
}


# ---------------------------------------------------------------------------
# MSK Connect — S3 sink
# ---------------------------------------------------------------------------

variable "msk_connect_plugin_s3_key" {
  description = "S3 key (within the storage bucket) of the Confluent S3 sink connector plugin zip — upload this manually before the first apply, see infra README"
  type        = string
  default     = "kafka-connect-plugins/confluentinc-kafka-connect-s3-12.1.0.zip"
}

variable "msk_connect_kafkaconnect_version" {
  description = "MSK Connect framework version — check AWS docs for currently supported values"
  type        = string
  default     = "2.7.1"
}


# ---------------------------------------------------------------------------
# ECS — producer service
# ---------------------------------------------------------------------------

variable "producer_image_tag" {
  description = "Tag suffix for the producer image — resolved as producer-<tag> against the shared ECR repo"
  type        = string
  default     = "latest"
}

variable "producer_cpu" {
  description = "Fargate CPU units for the producer task"
  type        = string
  default     = "256"
}

variable "producer_memory" {
  description = "Fargate memory (MB) for the producer task"
  type        = string
  default     = "512"
}

variable "producer_desired_count" {
  description = "Number of producer task instances — engine.py runs all meters in one process, so 1 is normal"
  type        = number
  default     = 1
}

variable "total_meters" {
  description = "Total number of simulated meters"
  type        = string
  default     = "500"
}

variable "random_seed" {
  description = "Random seed for reproducible simulation runs — empty string means non-deterministic"
  type        = string
  default     = ""
}


# ---------------------------------------------------------------------------
# ECS — consumer service
# ---------------------------------------------------------------------------

variable "consumer_image_tag" {
  description = "Tag suffix for the consumer image — resolved as consumer-<tag> against the shared ECR repo"
  type        = string
  default     = "latest"
}

variable "consumer_cpu" {
  description = "Fargate CPU units for the consumer task"
  type        = string
  default     = "512"
}

variable "consumer_memory" {
  description = "Fargate memory (MB) for the consumer task"
  type        = string
  default     = "1024"
}

variable "consumer_desired_count" {
  description = "Number of consumer task instances — runner.py runs all 5 zone consumers in one process, so 1 is normal"
  type        = number
  default     = 1
}

variable "speed_multiplier" {
  description = "Simulation speed multiplier — must be identical for producer and consumer"
  type        = string
  default     = "10"
}


# ---------------------------------------------------------------------------
# Power BI
# ---------------------------------------------------------------------------

variable "powerbi_enabled" {
  description = "Whether the consumer pushes aggregates to the Power BI streaming dataset"
  type        = bool
  default     = false
}

variable "powerbi_push_url" {
  description = "Power BI streaming dataset push URL — stored in SSM SecureString, never in state-readable plaintext output"
  type        = string
  default     = ""
  sensitive   = true
}


