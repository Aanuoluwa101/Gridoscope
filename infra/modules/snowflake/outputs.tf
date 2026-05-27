output "storage_integration_name" {
  description = "Name of the Snowflake storage integration"
  value       = snowflake_storage_integration.s3_int.name
}

output "stage_name" {
  description = "Name of the Snowflake external stage"
  value       = snowflake_stage.gridoscope_stage.name
}

output "snowflake_iam_user_arn" {
  description = "IAM user ARN Snowflake uses to assume the role — useful for debugging"
  value       = snowflake_storage_integration.s3_int.storage_aws_iam_user_arn
}

output "snowflake_external_id" {
  description = "External ID Snowflake uses — useful for debugging"
  value       = snowflake_storage_integration.s3_int.storage_aws_external_id
}

output "iam_role_arn" {
  description = "ARN of the IAM role Snowflake assumes to access S3"
  value       = aws_iam_role.gridoscope_snowflake_role.arn
}