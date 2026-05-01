provider "aws" {
  region = "ap-southeast-1"
}

# ---------------------------------------------------------------------------
# Kinesis Stream (already deployed)
# ---------------------------------------------------------------------------

resource "aws_kinesis_stream" "ecommerce_stream" {
  name             = "ecommerce-stream"
  shard_count      = 1
  retention_period = 24
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
  event_source_arn  = aws_kinesis_stream.ecommerce_stream.arn
  function_name     = aws_lambda_function.sessionizer.arn
  starting_position = "LATEST"       # only process new records, not backfill
  batch_size        = 100            # records per Lambda invocation
}

# ---------------------------------------------------------------------------
# S3 Buckets — Batch Layer storage
# ---------------------------------------------------------------------------

# Bucket 1: Raw events — Firehose dumps raw JSON here
resource "aws_s3_bucket" "raw_events" {
  bucket = "realtimeclickstream-raw-events"

  tags = {
    Project = "realtimeclickstream"
    Layer   = "batch"
  }
}

# Block all public access — this is private pipeline data
resource "aws_s3_bucket_public_access_block" "raw_events" {
  bucket                  = aws_s3_bucket.raw_events.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Bucket 2: Processed events — Spark writes corrected Parquet sessions here
resource "aws_s3_bucket" "processed_events" {
  bucket = "realtimeclickstream-processed-events"

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
# IAM Role — Firehose needs permission to:
#   1. Read from Kinesis Data Stream
#   2. Write to S3 raw-events bucket
# ---------------------------------------------------------------------------

resource "aws_iam_role" "firehose_role" {
  name = "realtimeclickstream-firehose-role"

  # Trust policy: allow Firehose service to assume this role
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
      # Permission 1: Read from Kinesis stream
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
      # Permission 2: Write to S3 raw-events bucket
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:PutObjectAcl"
        ]
        Resource = "${aws_s3_bucket.raw_events.arn}/*"
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# Kinesis Firehose — reads from Kinesis stream, writes raw JSON to S3
# Partitioned by date: s3://raw-events/year=.../month=.../day=.../hour=.../
# ---------------------------------------------------------------------------

resource "aws_kinesis_firehose_delivery_stream" "to_s3" {
  name        = "realtimeclickstream-firehose"
  destination = "extended_s3"

  # Source: read from Kinesis Data Stream
  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.ecommerce_stream.arn
    role_arn           = aws_iam_role.firehose_role.arn
  }

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose_role.arn
    bucket_arn = aws_s3_bucket.raw_events.arn

    # Partition raw events by date so Spark can read just one day at a time
    prefix              = "raw/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
    error_output_prefix = "errors/!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"

    # Buffer: flush to S3 every 5 minutes OR when buffer hits 64MB
    # (whichever comes first — keeps S3 files reasonably sized)
    buffering_interval = 300
    buffering_size     = 64

    compression_format = "UNCOMPRESSED" # keep as plain JSON for now — Spark can read it easily
  }
}

# ---------------------------------------------------------------------------
# Outputs — useful to copy when configuring Spark job later
# ---------------------------------------------------------------------------

output "raw_events_bucket" {
  value       = aws_s3_bucket.raw_events.bucket
  description = "S3 bucket where Firehose dumps raw events"
}

output "processed_events_bucket" {
  value       = aws_s3_bucket.processed_events.bucket
  description = "S3 bucket where Spark writes corrected Parquet sessions"
}

output "firehose_name" {
  value       = aws_kinesis_firehose_delivery_stream.to_s3.name
  description = "Kinesis Firehose delivery stream name"
}
