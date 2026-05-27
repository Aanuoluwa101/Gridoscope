output "bucket_arn" {
    value = aws_s3_bucket.gridoscope.arn
}

output "bucket_name" {
    value = aws_s3_bucket.gridoscope.bucket
}