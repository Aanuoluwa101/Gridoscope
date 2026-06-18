output "repository_url" {
  description = "ECR repository URL — tag producer images as producer-<tag>, consumer images as consumer-<tag>"
  value       = aws_ecr_repository.this.repository_url
}
