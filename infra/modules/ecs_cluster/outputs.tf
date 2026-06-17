output "cluster_id" {
  description = "ECS cluster ID"
  value       = aws_ecs_cluster.this.id
}

output "cluster_name" {
  description = "ECS cluster name"
  value       = aws_ecs_cluster.this.name
}

output "execution_role_arn" {
  description = "Shared ECS task execution role ARN"
  value       = aws_iam_role.execution.arn
}

output "ecs_tasks_security_group_id" {
  description = "Security group ID attached to producer/consumer Fargate tasks"
  value       = aws_security_group.ecs_tasks.id
}
