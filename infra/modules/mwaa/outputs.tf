output "webserver_url" {
  description = "MWAA Airflow web server URL — open this to access the Airflow UI"
  value       = aws_mwaa_environment.this.webserver_url
}

output "environment_arn" {
  description = "ARN of the MWAA environment"
  value       = aws_mwaa_environment.this.arn
}
