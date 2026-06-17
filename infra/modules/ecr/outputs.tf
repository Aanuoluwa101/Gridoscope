output "producer_repository_url" {
  description = "ECR repository URL for the producer image"
  value       = aws_ecr_repository.producer.repository_url
}

output "consumer_repository_url" {
  description = "ECR repository URL for the consumer image"
  value       = aws_ecr_repository.consumer.repository_url
}
