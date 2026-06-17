output "vpc_id" {
  description = "ID of the Gridoscope VPC"
  value       = aws_vpc.this.id
}

output "private_subnet_ids" {
  description = "Private subnet IDs — used by MSK brokers and ECS Fargate tasks"
  value       = aws_subnet.private[*].id
}

output "public_subnet_ids" {
  description = "Public subnet IDs — used only by the NAT Gateway"
  value       = aws_subnet.public[*].id
}
