# Learning Notes — Lambda Architecture & AWS

## 1. Why Lambda Architecture?

Real-time systems must choose between two goals that conflict:

- **Low latency** → answers arrive fast but may be incomplete/wrong (late data, out-of-order)
- **Correctness** → answers are accurate but arrive later

Lambda Architecture runs **both** paths and lets the business decide which answer to use for each use-case.

In this project:

- **Speed Layer** = Lambda + DynamoDB → “What is happening right now?”
- **Batch Layer** = Spark over all historical events → “What actually happened?”

## 2. Sessionization Rules Used Here

- A session ends when the user is inactive for **> 30 minutes**
- New session starts on the next event after that gap
- This rule is applied in both layers (so they can be compared later)

## 3. How the Speed Layer Works (step by step)

1. Simulator reads CSV → cleans → enriches → `put_records` to Kinesis
2. Kinesis event source mapping buffers records for **30 seconds** (tumbling window)
3. Lambda receives a batch of records
4. Code groups the batch by `session_id`
5. For each session:
   - Read current state from DynamoDB (or start new)
   - Check 30-min gap → maybe reset the session
   - Update counters (event_count, products, total_value…)
   - Write back **one** item to DynamoDB
6. DynamoDB TTL automatically deletes sessions after 2 hours of inactivity

**Key efficiency trick:** Grouping by session_id inside the 30-second window means we do far fewer DynamoDB writes.

## 4. How the Batch Layer Works

1. Firehose continuously dumps every Kinesis record to S3 (raw JSON, time-partitioned)
2. Nightly (or on-demand) Spark job reads the raw events
3. Spark uses window functions + `lag()` to detect 30-minute gaps **per user**
4. Produces clean session summaries as Parquet

Because the batch job sees **all** data (including late arrivals), its answer is the ground truth.

## 5. AWS Services Mapping

| Service              | Role in this project                          |
|----------------------|-----------------------------------------------|
| Kinesis Data Streams | Ordered, durable event bus                    |
| Lambda               | Serverless speed-layer processor              |
| DynamoDB             | Low-latency store for live sessions           |
| Firehose             | Zero-code delivery of raw events to S3        |
| S3                   | Cheap durable storage for raw + batch output  |
| Spark (local/EMR)    | Batch recompute engine                        |
| Terraform            | Reproducible infrastructure                   |

## 6. Suggested Learning Path

1. Read `terraform/main.tf` and understand every resource
2. Run the simulator with a small `--max-rows` and watch CloudWatch Logs of the Lambda
3. Query DynamoDB and see the live session items
4. Run the local Spark job and inspect the Parquet output
5. Think about what happens when events arrive late — why the speed layer can be wrong and how the batch layer fixes it

## 7. Diagrams

See the two images in this folder (`image.png` and `image-1.png`) for the visual architecture you drew.
