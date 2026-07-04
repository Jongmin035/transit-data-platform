terraform {
    required_providers {
        aws = {
            source  = "hashicorp/aws"
            version = "~> 6.0"
        }
    }
}

provider "aws" {
    region = "us-east-1"
    profile = "data-eng"
}

resource "aws_s3_bucket" "my_bucket" {
    bucket = "jh-transit-s3-backend-bucket"

    tags = {
        Name = "jh-transit-s3-backend-bucket"
    }
}

resource "aws_s3_bucket_versioning" "my_bucket_versioning" {
    bucket = aws_s3_bucket.my_bucket.id
    versioning_configuration {
        status = "Enabled"
    }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "my_bucket_encryption" {
    bucket = aws_s3_bucket.my_bucket.id

    rule {
        apply_server_side_encryption_by_default {
            sse_algorithm = "AES256"
        }
    }
}

resource "aws_s3_bucket_public_access_block" "my_bucket_public_access" {
    bucket = aws_s3_bucket.my_bucket.id

    block_public_acls       = true
    ignore_public_acls       = true
    block_public_policy      = true
    restrict_public_buckets  = true
}

resource "aws_s3_bucket_policy" "my_bucket_policy" {
    bucket = aws_s3_bucket.my_bucket.id

    policy = jsonencode({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Principal": {
                    "AWS": "arn:aws:iam::152125349337:root"
                },
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": aws_s3_bucket.my_bucket.arn
            },
            {
                "Principal": {
                    "AWS": "arn:aws:iam::152125349337:root"
                },
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:PutObject"],
                "Resource": [
                    "${aws_s3_bucket.my_bucket.arn}/*.tfstate"
                ]
            },
            {
                "Principal": {
                    "AWS": "arn:aws:iam::152125349337:root"
                },
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                "Resource": [
                    "${aws_s3_bucket.my_bucket.arn}/*.tflock"
                ]
            }
        ]
    })
}