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
