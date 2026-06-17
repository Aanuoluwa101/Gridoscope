output "cluster_arn" {
  description = "MSK cluster ARN — used to scope IAM policies for producer/consumer task roles"
  value       = aws_msk_cluster.this.arn
}

output "cluster_name" {
  description = "MSK cluster name"
  value       = aws_msk_cluster.this.cluster_name
}

output "bootstrap_brokers_iam" {
  description = "Bootstrap broker connection string for SASL/IAM clients (port 9098)"
  value       = data.aws_msk_bootstrap_brokers.this.bootstrap_brokers_sasl_iam
}

output "security_group_id" {
  description = "Security group ID attached to MSK brokers"
  value       = aws_security_group.msk.id
}
