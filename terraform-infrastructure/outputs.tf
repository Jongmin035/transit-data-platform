output "data_lake_bucket_name" {
    value = aws_s3_bucket.data_lake.bucket
    description = "The ID of the S3 bucket used as the data lake"
}