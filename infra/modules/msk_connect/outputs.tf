output "connector_arn" {
  description = "MSK Connect connector ARN"
  value       = aws_mskconnect_connector.s3_sink.arn
}

output "security_group_id" {
  description = "Security group ID attached to Connect workers — needed by root to wire MSK ingress"
  value       = aws_security_group.connect.id
}
