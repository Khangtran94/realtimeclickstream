provider "aws" {
  region = "ap-southeast-1"
}

data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  # Make S3 bucket names globally unique
  raw_bucket_name       = "realtimeclickstream-raw-${local.account_id}"
  processed_bucket_name = "realtimeclickstream-processed-${local.account_id}"
}

# ---------------------------------------------------------------------------
# Kinesis Stream
# ---------------------------------------------------------------------------

resource "aws_kinesis_stream" "ecommerce_stream" {
  name             = "ecommerce-stream"
  shard_count      = 1
  retention_period = 24

  tags = {
    Project = "realtimeclickstream"
    Layer   = "ingest"
  }
}

# ---------------------------------------------------------------------------
# DynamoDB — active sessions store (Speed Layer)
# ---------------------------------------------------------------------------

resource "aws_dynamodb_table" "sessions" {
  name         = "clickstream-sessions"
  billing_mode = "PAY_PER_REQUEST"   # no capacity planning needed for dev
  hash_key     = "session_id"

  attribute {
    name = "session_id"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  tags = {
    Project = "realtimeclickstream"
    Layer   = "speed"
  }
}

# ---------------------------------------------------------------------------
# IAM Role for Lambda
# ---------------------------------------------------------------------------

resource "aws_iam_role" "sessionizer_role" {
  name = "sessionizer-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "sessionizer_policy" {
  name = "sessionizer-lambda-policy"
  role = aws_iam_role.sessionizer_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # Read from Kinesis
        Effect = "Allow"
        Action = [
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:DescribeStream",
          "kinesis:ListStreams",
          "kinesis:ListShards"
        ]
        Resource = aws_kinesis_stream.ecommerce_stream.arn
      },
      {
        # Write to DynamoDB
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem"
        ]
        Resource = aws_dynamodb_table.sessions.arn
      },
      {
        # Write logs to CloudWatch
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# Lambda function — package code as zip
# ---------------------------------------------------------------------------

data "archive_file" "sessionizer_zip" {
  type        = "zip"
  source_file = "${path.module}/../src/lambda/sessionizer.py"
  output_path = "${path.module}/sessionizer.zip"
}

resource "aws_lambda_function" "sessionizer" {
  function_name    = "clickstream-sessionizer"
  role             = aws_iam_role.sessionizer_role.arn
  runtime          = "python3.12"
  handler          = "sessionizer.handler"    # filename.function_name
  filename         = data.archive_file.sessionizer_zip.output_path
  source_code_hash = data.archive_file.sessionizer_zip.output_base64sha256

  timeout     = 60     # seconds — enough for a batch of 100 records
  memory_size = 256    # MB

  environment {
    variables = {
      DYNAMODB_TABLE = aws_dynamodb_table.sessions.name
      AWS_REGION     = "ap-southeast-1"
    }
  }

  tags = {
    Project = "realtimeclickstream"
    Layer   = "speed"
  }
}

# ---------------------------------------------------------------------------
# Event Source Mapping — Kinesis triggers Lambda
# ---------------------------------------------------------------------------

resource "aws_lambda_event_source_mapping" "kinesis_to_sessionizer" {
  event_source_arn                   = aws_kinesis_stream.ecommerce_stream.arn
  function_name                      = aws_lambda_function.sessionizer.arn
  starting_position                  = "LATEST"       # only process new records, not backfill
  batch_size                         = 100            # records per Lambda invocation
  tumbling_window_in_seconds         = 30
  maximum_batching_window_in_seconds = 0              # we use tumbling_window instead
}

# ---------------------------------------------------------------------------
# S3 Buckets — Batch Layer storage
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "raw_events" {
  bucket = local.raw_bucket_name

  tags = {
    Project = "realtimeclickstream"
    Layer   = "batch"
  }
}

resource "aws_s3_bucket_public_access_block" "raw_events" {
  bucket                  = aws_s3_bucket.raw_events.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket" "processed_events" {
  bucket = local.processed_bucket_name

  tags = {
    Project = "realtimeclickstream"
    Layer   = "batch"
  }
}

resource "aws_s3_bucket_public_access_block" "processed_events" {
  bucket                  = aws_s3_bucket.processed_events.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ---------------------------------------------------------------------------
# IAM Role for Firehose
# ---------------------------------------------------------------------------

resource "aws_iam_role" "firehose_role" {
  name = "realtimeclickstream-firehose-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "firehose.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "firehose_policy" {
  name = "realtimeclickstream-firehose-policy"
  role = aws_iam_role.firehose_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:DescribeStream",
          "kinesis:ListShards"
        ]
        Resource = aws_kinesis_stream.ecommerce_stream.arn
      },
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:PutObjectAcl",
          "s3:AbortMultipartUpload",
          "s3:GetBucketLocation",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.raw_events.arn,
          "${aws_s3_bucket.raw_events.arn}/*"
        ]
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# Kinesis Firehose — reads from Kinesis, writes raw JSON to S3
# ---------------------------------------------------------------------------

resource "aws_kinesis_firehose_delivery_stream" "to_s3" {
  name        = "realtimeclickstream-firehose"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.ecommerce_stream.arn
    role_arn           = aws_iam_role.firehose_role.arn
  }

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose_role.arn
    bucket_arn = aws_s3_bucket.raw_events.arn

    # Partition by time so Spark can read one day/hour at a time
    prefix              = "raw/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
    error_output_prefix = "errors/!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"

    buffering_interval = 300   # 5 minutes
    buffering_size     = 64    # MB

    compression_format = "UNCOMPRESSED"
  }
}

# ---------------------------------------------------------------------------
# Outputs — copy these values after terraform apply
# ---------------------------------------------------------------------------

output "kinesis_stream_name" {
  value       = aws_kinesis_stream.ecommerce_stream.name
  description = "Kinesis stream name — put this in .env as KINESIS_STREAM_NAME"
}

output "dynamodb_table_name" {
  value       = aws_dynamodb_table.sessions.name
  description = "DynamoDB table name — put this in .env as DYNAMODB_TABLE"
}

output "lambda_function_name" {
  value       = aws_lambda_function.sessionizer.function_name
  description = "Lambda function name"
}

output "raw_events_bucket" {
  value       = aws_s3_bucket.raw_events.bucket
  description = "S3 bucket where Firehose dumps raw events"
}

output "processed_events_bucket" {
  value       = aws_s3_bucket.processed_events.bucket
  description = "S3 bucket for Spark corrected sessions"
}

output "firehose_name" {
  value       = aws_kinesis_firehose_delivery_stream.to_s3.name
  description = "Kinesis Firehose delivery stream name"
}
