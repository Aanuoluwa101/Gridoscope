terraform {
    required_providers {
        aws = {
            source = "hashicorp/aws"
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
  organization_name =  var.snowflake_organization
  account_name      =  var.snowflake_account
  user              =  var.snowflake_username
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