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

output "ecr_repository_url" {
  description = "ECR repository URL — push as producer-<tag> and consumer-<tag>"
  value       = module.ecr.repository_url
}

output "msk_connect_connector_arn" {
  description = "MSK Connect S3 sink connector ARN"
  value       = module.msk_connect.connector_arn
}

output "mwaa_webserver_url" {
  description = "MWAA Airflow UI URL"
  value       = module.mwaa.webserver_url
}
