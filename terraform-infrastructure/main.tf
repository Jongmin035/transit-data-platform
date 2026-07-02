terraform {
    required_providers {
        aws = {
            source      = "hashicorp/aws"
            version     = "~> 6.0"
        }
    }
}

provider "aws" {
    region = "us-east-1"
    profile = "data-eng"
}

resource "aws_s3_bucket" "data_lake" {
    bucket_prefix = var.bucket_prefix

    lifecycle {
        prevent_destroy = true
    }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
    bucket = aws_s3_bucket.data_lake.bucket

    block_public_acls = true
    block_public_policy = true
    ignore_public_acls = true
    restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake_encryption" {
    bucket = aws_s3_bucket.data_lake.bucket

    rule {
        apply_server_side_encryption_by_default {
            sse_algorithm = "AES256"
        }
    }
}

resource "aws_s3_bucket_versioning" "data_lake_versioning" {
    bucket = aws_s3_bucket.data_lake.bucket
    versioning_configuration {
        status = "Enabled"
    }
}