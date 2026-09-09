# Real-Time Clickstream — Lambda Architecture on AWS

A hands-on project to learn **Lambda Architecture** and core AWS streaming services by building a real-time e-commerce clickstream pipeline.

**Dataset:** [REES46 E-Commerce Behaviour](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) (CSV events: view / cart / purchase).

---

## What You Will Learn

| Concept | How this project teaches it |
|---------|-----------------------------|
| **Lambda Architecture** | Speed Layer (approximate, low latency) + Batch Layer (correct, higher latency) |
| **Kinesis Data Streams** | Ingest, partitioning by `user_id`, batching, retries |
| **AWS Lambda** | Event source mapping, tumbling window, sessionization logic |
| **DynamoDB** | Single-table design, TTL for auto-cleanup, GetItem + PutItem |
| **Kinesis Firehose + S3** | Durable raw event landing zone (partitioned by time) |
| **Spark (PySpark)** | Batch recompute of sessions from scratch (ground truth) |
| **Terraform** | Infrastructure as Code for the whole pipeline |
| **Sessionization** | 30-minute inactivity boundary |

---

## Architecture (Lambda Architecture)

```
┌─────────────────────────────────────────────────────────────────┐
│                     EVENT SIMULATOR                              │
│   CSV (REES46) → clean → enrich → Kinesis put_records            │
│   (src/event_simulator/ingest_to_kinesis.py)                     │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
              ┌──────────────────────────────┐
              │   Kinesis Data Stream        │
              │   ecommerce-stream           │
              │   (partition key = user_id)  │
              └──────────────┬───────────────┘
                             │
              ┌──────────────┴───────────────┐
              │                              │
              ▼                              ▼
┌─────────────────────────┐    ┌─────────────────────────────┐
│   SPEED LAYER           │    │   BATCH LAYER               │
│                         │    │                             │
│  Lambda Sessionizer     │    │  Firehose → S3 (raw)        │
│  (30s tumbling window)  │    │                             │
│  30-min inactivity rule │    │  Spark batch job            │
│  DynamoDB (hot sessions)│    │  (recompute all sessions)   │
│  + 2-hour TTL           │    │  → corrected Parquet        │
└─────────────────────────┘    └─────────────────────────────┘
```

### Speed Layer (real-time)
- **Goal:** Low-latency approximate sessions
- **Path:** Kinesis → Lambda → DynamoDB
- **Key logic:**
  - AWS buffers records for 30 seconds (tumbling window)
  - Lambda groups by `session_id` → 1 DynamoDB write per session (not per event)
  - If gap > 30 minutes since last event → start a new session
  - DynamoDB items auto-expire after 2 hours (TTL)

### Batch Layer (correctness)
- **Goal:** Exact sessions including late arrivals
- **Path:** Firehose dumps every event to S3 → Spark job re-sessionizes everything
- **Key logic:** Window functions + lag to detect 30-min gaps per user

This is the classic Lambda Architecture tradeoff: **speed gives approximate answers fast; batch gives correct answers later**.

---

## Project Structure

```
realtimeclickstream/
├── src/
│   ├── event_simulator/
│   │   └── ingest_to_kinesis.py   # CSV → clean/enrich → Kinesis
│   ├── lambda/
│   │   └── sessionizer.py         # Speed Layer: Kinesis → DynamoDB
│   └── batch/
│       └── batch_session_job.py   # Batch Layer: Spark recompute
├── terraform/
│   └── main.tf                    # Kinesis, DynamoDB, Lambda, Firehose, S3
├── Notes/                         # Architecture diagrams
├── pyproject.toml
├── .env.example
└── README.md
```

---

## Prerequisites

- Python 3.12+
- AWS account + CLI configured (`aws configure`)
- Terraform >= 1.5
- Java 11+ (for local PySpark)
- [uv](https://github.com/astral-sh/uv) (recommended) or pip

---

## Setup

### 1. Clone & install

```bash
git clone https://github.com/Khangtran94/realtimeclickstream.git
cd realtimeclickstream
git checkout Khang_branch

# with uv
uv sync

# or pip
pip install -e .
```

### 2. Environment variables

```bash
cp .env.example .env
# edit .env with your values
```

Required:
```
KINESIS_STREAM_NAME=ecommerce-stream
AWS_REGION=ap-southeast-1
DYNAMODB_TABLE=clickstream-sessions
```

### 3. Deploy infrastructure

```bash
cd terraform
terraform init
terraform apply
```

This creates:
- Kinesis stream `ecommerce-stream`
- DynamoDB table `clickstream-sessions` (with TTL)
- Lambda `clickstream-sessionizer` + event source mapping (30s tumbling window)
- Firehose → S3 raw events bucket
- S3 processed events bucket

### 4. Download sample data

See `Notes/` or download a small slice of the REES46 dataset from Kaggle and place it under `data/`.

---

## How to Run

### Speed Layer (real-time)

```bash
# Ingest events into Kinesis
python src/event_simulator/ingest_to_kinesis.py --file data/2019-Oct.csv --max-rows 50000

# Lambda is triggered automatically by the Kinesis event source mapping.
# Watch CloudWatch Logs for the sessionizer function.
```

### Batch Layer (local Spark)

```bash
python src/batch/batch_session_job.py \
  --input data/2019-Oct.csv \
  --output data/output/sessions \
  --max-rows 100000
```

On EMR later you can point `--input` / `--output` to the S3 buckets created by Terraform.

---

## Key Design Decisions (Learning Points)

| Decision | Choice | Why |
|----------|--------|-----|
| Partition key | `user_id` | All events of one user stay on the same shard → ordering |
| Tumbling window | 30 seconds | AWS buffers → Lambda processes fewer, larger batches |
| Session timeout | 30 minutes | Industry standard (Google Analytics style) |
| DynamoDB TTL | 2 hours | Speed layer only needs *hot* sessions; keeps cost near free tier |
| Batch recompute | Full re-sessionize | Corrects any mistakes the speed layer made (late arrivals, out-of-order) |
| Firehose | Yes | Cheap, reliable way to land every raw event in S3 for the batch layer |

---

## AWS Services Used

1. **Amazon Kinesis Data Streams** – durable ordered stream of events
2. **AWS Lambda** – serverless compute for the speed layer
3. **Amazon DynamoDB** – low-latency key-value store for live sessions
4. **Amazon Kinesis Data Firehose** – fully managed delivery of raw events to S3
5. **Amazon S3** – durable storage for raw + processed data
6. **Apache Spark (local / EMR)** – batch processing engine

---

## Next Steps / Extensions (for deeper learning)

- Add intentional late-arrival injection in the simulator
- Export DynamoDB snapshot and compare with Spark output (accuracy report)
- Move the batch job to EMR Serverless
- Add Athena tables + a simple accuracy view
- Add CloudWatch metrics / alarms
- Package Lambda with a proper build script

---

## References

- [Lambda Architecture (Martin Fowler)](https://martinfowler.com/bliki/LambdaArchitecture.html)
- [Kinesis Developer Guide](https://docs.aws.amazon.com/streams/latest/dev/introduction.html)
- [DynamoDB TTL](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html)
- REES46 dataset on Kaggle

---

**Branch:** `Khang_branch`  
This is the focused, learning-oriented version of the project.
