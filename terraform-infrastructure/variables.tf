variable "bucket_prefix" {
    description = "The prefix for the S3 bucket used as the data lake"
    type = string
    default = "jh-transit-datalake-dev-"
}