output "service_name" {
  description = "ECS service name"
  value       = aws_ecs_service.this.name
}

output "task_role_arn" {
  description = "Task role ARN for this service"
  value       = aws_iam_role.task.arn
}

output "log_group_name" {
  description = "CloudWatch log group name"
  value       = aws_cloudwatch_log_group.this.name
}
