provider "aws" {
  region = "ap-southeast-1"
}

resource "aws_kinesis_stream" "ecommerce_stream" {
  name             = "ecommerce-stream"
  shard_count      = 1
  retention_period = 24
}