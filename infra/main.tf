terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }

    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 1.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      App = "Gridoscope"
    }
  }
}


provider "snowflake" {
  organization_name = var.snowflake_organization
  account_name      = var.snowflake_account
  user              = var.snowflake_username
  role              = "ACCOUNTADMIN"
  authenticator     = "SNOWFLAKE_JWT"
  private_key       = file("snowflake_tf_key.p8")

  preview_features_enabled = [
    "snowflake_storage_integration_resource",
    "snowflake_stage_resource"
  ]
}



module "storage" {
  source      = "./modules/storage"
  bucket_name = var.bucket_name
  environment = var.environment
}

module "snowflake" {
  source             = "./modules/snowflake"
  environment        = var.environment
  bucket_name        = module.storage.bucket_name
  bucket_arn         = module.storage.bucket_arn
  snowflake_database = var.snowflake_database
  snowflake_schema   = var.snowflake_schema
  iam_role_name      = var.iam_role_name
}


# ---------------------------------------------------------------------------
# Phase 1: MSK cluster + ECS Fargate producer/consumer
# ---------------------------------------------------------------------------

locals {
  ssm_path_prefix = "/gridoscope/${var.environment}"
}

module "networking" {
  source      = "./modules/networking"
  environment = var.environment
  aws_region  = var.aws_region
}

module "ecr" {
  source      = "./modules/ecr"
  environment = var.environment
}

module "ecs_cluster" {
  source                    = "./modules/ecs_cluster"
  environment               = var.environment
  ssm_parameter_path_prefix = local.ssm_path_prefix
  aws_region                = var.aws_region
  vpc_id                    = module.networking.vpc_id
}

module "msk" {
  source               = "./modules/msk"
  environment          = var.environment
  vpc_id               = module.networking.vpc_id
  private_subnet_ids   = module.networking.private_subnet_ids
  kafka_version        = var.msk_kafka_version
  broker_instance_type = var.msk_broker_instance_type
  broker_count         = var.msk_broker_count
  ebs_volume_size      = var.msk_ebs_volume_size
}

# Cross-SG ingress rules — added at root, not inside the msk/ecs_cluster/
# msk_connect modules, because each of those modules' SG is an *input* to
# the other two. Wiring them together inside any one module would create a
# module dependency cycle; at root, both ends of each rule are already
# visible as plain outputs.
resource "aws_security_group_rule" "msk_ingress_from_ecs" {
  type                     = "ingress"
  from_port                = 9098
  to_port                  = 9098
  protocol                 = "tcp"
  security_group_id        = module.msk.security_group_id
  source_security_group_id = module.ecs_cluster.ecs_tasks_security_group_id
  description              = "Kafka IAM-auth port from ECS producer/consumer tasks"
}

resource "aws_security_group_rule" "msk_ingress_from_connect" {
  type                     = "ingress"
  from_port                = 9098
  to_port                  = 9098
  protocol                 = "tcp"
  security_group_id        = module.msk.security_group_id
  source_security_group_id = module.msk_connect.security_group_id
  description              = "Kafka IAM-auth port from MSK Connect S3 sink"
}

module "msk_connect" {
  source                    = "./modules/msk_connect"
  environment               = var.environment
  aws_region                = var.aws_region
  vpc_id                    = module.networking.vpc_id
  private_subnet_ids        = module.networking.private_subnet_ids
  msk_cluster_arn           = module.msk.cluster_arn
  msk_bootstrap_brokers_iam = module.msk.bootstrap_brokers_iam
  bucket_name               = module.storage.bucket_name
  bucket_arn                = module.storage.bucket_arn
  plugin_s3_bucket          = module.storage.bucket_name
  plugin_s3_key             = var.msk_connect_plugin_s3_key
  kafkaconnect_version      = var.msk_connect_kafkaconnect_version
}

# Power BI push URL — read by the consumer task via the `secrets` block in
# its task definition, never as a plaintext env var or a value committed to
# source. dev.tfvars (gitignored) is where the real value lives locally.
resource "aws_ssm_parameter" "powerbi_push_url" {
  name  = "${local.ssm_path_prefix}/powerbi_push_url"
  type  = "SecureString"
  value = var.powerbi_push_url
}

module "producer_service" {
  source                = "./modules/ecs_service"
  environment           = var.environment
  service_name          = "producer"
  image_uri             = "${module.ecr.repository_url}:producer-${var.producer_image_tag}"
  command               = ["python", "producers/engine.py"]
  cpu                   = var.producer_cpu
  memory                = var.producer_memory
  desired_count         = var.producer_desired_count
  cluster_id            = module.ecs_cluster.cluster_id
  execution_role_arn    = module.ecs_cluster.execution_role_arn
  subnet_ids            = module.networking.private_subnet_ids
  security_group_id     = module.ecs_cluster.ecs_tasks_security_group_id
  aws_region            = var.aws_region
  msk_cluster_arn       = module.msk.cluster_arn
  msk_bootstrap_brokers = module.msk.bootstrap_brokers_iam
  kafka_topic_actions   = ["kafka-cluster:DescribeTopic", "kafka-cluster:CreateTopic", "kafka-cluster:WriteData"]

  environment_variables = {
    TOTAL_METERS              = var.total_meters
    SPEED_MULTIPLIER          = var.speed_multiplier
    RANDOM_SEED               = var.random_seed
    KAFKA_SECURITY_PROTOCOL   = "SASL_SSL"
    ZONE_NORTH_PARTITION      = tostring(var.zone_north_partition)
    ZONE_SOUTH_PARTITION      = tostring(var.zone_south_partition)
    ZONE_EAST_PARTITION       = tostring(var.zone_east_partition)
    ZONE_WEST_PARTITION       = tostring(var.zone_west_partition)
    ZONE_CENTRAL_PARTITION    = tostring(var.zone_central_partition)
  }
}

module "consumer_service" {
  source                = "./modules/ecs_service"
  environment           = var.environment
  service_name          = "consumer"
  image_uri             = "${module.ecr.repository_url}:consumer-${var.consumer_image_tag}"
  command               = ["python", "consumers/runner.py"]
  cpu                   = var.consumer_cpu
  memory                = var.consumer_memory
  desired_count         = var.consumer_desired_count
  cluster_id            = module.ecs_cluster.cluster_id
  execution_role_arn    = module.ecs_cluster.execution_role_arn
  subnet_ids            = module.networking.private_subnet_ids
  security_group_id     = module.ecs_cluster.ecs_tasks_security_group_id
  aws_region            = var.aws_region
  msk_cluster_arn       = module.msk.cluster_arn
  msk_bootstrap_brokers = module.msk.bootstrap_brokers_iam
  kafka_topic_actions   = ["kafka-cluster:DescribeTopic", "kafka-cluster:ReadData"]
  kafka_group_actions   = ["kafka-cluster:DescribeGroup", "kafka-cluster:AlterGroup"]

  environment_variables = {
    SPEED_MULTIPLIER          = var.speed_multiplier
    POWERBI_ENABLED           = tostring(var.powerbi_enabled)
    KAFKA_SECURITY_PROTOCOL   = "SASL_SSL"
    ZONE_NORTH_PARTITION      = tostring(var.zone_north_partition)
    ZONE_SOUTH_PARTITION      = tostring(var.zone_south_partition)
    ZONE_EAST_PARTITION       = tostring(var.zone_east_partition)
    ZONE_WEST_PARTITION       = tostring(var.zone_west_partition)
    ZONE_CENTRAL_PARTITION    = tostring(var.zone_central_partition)
  }

  secrets = [
    { name = "POWERBI_PUSH_URL", value_from = aws_ssm_parameter.powerbi_push_url.arn }
  ]
}