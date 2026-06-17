output "vpc_id" {
  description = "Gridoscope VPC ID"
  value       = module.networking.vpc_id
}

output "msk_bootstrap_brokers_iam" {
  description = "MSK bootstrap broker string for SASL/IAM clients"
  value       = module.msk.bootstrap_brokers_iam
}

output "ecs_cluster_name" {
  description = "ECS cluster name"
  value       = module.ecs_cluster.cluster_name
}

output "producer_repository_url" {
  description = "ECR repository URL to push producer images to"
  value       = module.ecr.producer_repository_url
}

output "consumer_repository_url" {
  description = "ECR repository URL to push consumer images to"
  value       = module.ecr.consumer_repository_url
}

output "msk_connect_connector_arn" {
  description = "MSK Connect S3 sink connector ARN"
  value       = module.msk_connect.connector_arn
}
