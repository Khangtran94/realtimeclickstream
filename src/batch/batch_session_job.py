"""
batch_session_job.py
--------------------
Batch Layer — reads raw click events, recomputes sessions from scratch.

This is the "ground truth" layer that corrects the Speed Layer's sessions
by processing ALL events including late arrivals.

Local usage:
    python batch_session_job.py --input data/2019-Oct.csv --output data/output/sessions

EMR usage (later):
    spark-submit batch_session_job.py \
        --input s3://realtimeclickstream-raw-events/raw/ \
        --output s3://realtimeclickstream-processed-events/sessions/
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SESSION_TIMEOUT_SECONDS = 1800  # 30 minutes — new session if gap exceeds this

# ---------------------------------------------------------------------------
# Step 1 — Read raw events
# ---------------------------------------------------------------------------

def read_events(spark: SparkSession, input_path: str, max_rows: int = None):
    """
    Read raw events from either a local CSV or S3 JSON (same code, different path).
    """
    print(f"\n[Step 1] Reading events from: {input_path}")

    if input_path.endswith(".csv"):
        df = spark.read.option("header", "true").csv(input_path)
    else:
        df = spark.read.json(input_path)

    # Cast columns to correct types (everything is string when first read)
    df = df.select(
        F.col("user_id").cast("string"),
        F.col("product_id").cast("string"),
        F.col("event_type").cast("string"),
        F.col("price").cast("double"),
        F.col("brand").cast("string"),
        F.col("category_code").cast("string"),
        F.col("user_session").cast("string").alias("session_id"),
        # event_time is the REAL time the event happened (used for session logic)
        F.to_timestamp("event_time").alias("event_time"),
        # arrival_time: fallback to event_time for local CSV testing
        # On EMR with real Firehose data, this will be the actual arrival_time
        F.to_timestamp("event_time").alias("arrival_time"),
    ).filter(
        F.col("user_id").isNotNull() &
        F.col("event_time").isNotNull()
    )

    count = df.count()
    print(f"[Step 1] Loaded {count:,} events")
    return df

# ---------------------------------------------------------------------------
# Step 2 — Assign session IDs based on 30-minute gap rule
# ---------------------------------------------------------------------------

def assign_sessions(df):
    """
    Group events by user, then split into sessions based on time gaps.

    Rule: if gap between two consecutive events > 30 minutes → new session.

    Example:
        user_A  10:00  →  session_1
        user_A  10:05  →  session_1  (gap = 5 min, same session)
        user_A  10:40  →  session_2  (gap = 35 min > 30 min, NEW session)
    """
    print("\n[Step 2] Assigning session IDs based on 30-minute gap rule...")

    # Window: look at each user's events in chronological order
    user_window = Window.partitionBy("user_id").orderBy("event_time")

    # For each event, get the previous event time (same user)
    df = df.withColumn(
        "prev_event_time",
        F.lag("event_time").over(user_window)
    )

    # Calculate gap in seconds from previous event
    df = df.withColumn(
        "gap_seconds",
        F.when(
            F.col("prev_event_time").isNull(), 0
        ).otherwise(
            F.unix_timestamp("event_time") - F.unix_timestamp("prev_event_time")
        )
    )

    # Flag where a new session starts
    df = df.withColumn(
        "is_new_session",
        F.when(F.col("prev_event_time").isNull(), 1)          # first event ever
         .when(F.col("gap_seconds") > SESSION_TIMEOUT_SECONDS, 1)  # big gap
         .otherwise(0)
    )

    # Cumulative sum gives each session a unique number per user
    # [1, 0, 0, 1, 0] → cumsum → [1, 1, 1, 2, 2]
    df = df.withColumn(
        "session_number",
        F.sum("is_new_session").over(user_window)
    )

    # Final session ID = user_id + session_number
    df = df.withColumn(
        "batch_session_id",
        F.concat_ws("_", F.col("user_id"), F.col("session_number").cast("string"))
    )

    print("[Step 2] Session IDs assigned")
    return df

# ---------------------------------------------------------------------------
# Step 3 — Flag late arriving events
# ---------------------------------------------------------------------------

def flag_late_arrivals(df):
    """
    For local CSV testing: placeholder — no arrival_time column in raw CSV.
    On EMR with real Firehose data, this will compare arrival_time vs event_time.
    """
    print("\n[Step 3] Flagging late arriving events...")

    # Placeholder for local testing
    # On EMR replace with: F.col("arrival_delay_seconds") > 300
    df = df.withColumn("is_late_arrival", F.lit(False))

    print("[Step 3] Done (placeholder for local CSV — no arrival_time column)")
    return df

# ---------------------------------------------------------------------------
# Step 4 — Aggregate into session summaries and write Parquet
# ---------------------------------------------------------------------------

def compute_session_summary(df):
    """
    Aggregate individual events into one row per session.
    """
    print("\n[Step 4] Computing session summaries...")

    session_summary = df.groupBy("user_id", "batch_session_id").agg(
        F.min("event_time").alias("session_start"),
        F.max("event_time").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum(
            F.when(F.col("event_type") == "purchase", 1).otherwise(0)
        ).alias("purchase_count"),
        F.max("price").alias("max_price"),
        F.collect_set("event_type").alias("event_types"),
        # Session duration in minutes
        (
            (F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60
        ).alias("session_duration_minutes"),
    )

    count = session_summary.count()
    print(f"[Step 4] Computed {count:,} sessions")
    return session_summary


def write_output(session_summary, output_path: str):
    """
    Write corrected sessions to Parquet, partitioned by date.
    """
    print(f"\n[Step 4] Writing Parquet output to: {output_path}")

    session_summary \
        .withColumn("session_date", F.to_date("session_start")) \
        .write \
        .mode("overwrite") \
        .partitionBy("session_date") \
        .parquet(output_path)

    print(f"[Step 4] Done! Output written to: {output_path}")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(input_path: str, output_path: str, max_rows: int = None):
    spark = SparkSession.builder \
        .appName("BatchSessionJob") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 50)
    print("Batch Layer — Session Recomputation Job")
    print("=" * 50)

    df = read_events(spark, input_path, max_rows=max_rows)
    df = assign_sessions(df)
    df = flag_late_arrivals(df)
    session_summary = compute_session_summary(df)

    print("\n[Preview] Sample session output:")
    session_summary.show(5, truncate=False)

    write_output(session_summary, output_path)

    print("\n✅ Batch job complete!")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch Layer — Session Recomputation")
    parser.add_argument("--input",  required=True, help="Input path (local CSV or S3 path)")
    parser.add_argument("--output", required=True, help="Output path (local folder or S3 path)")
    parser.add_argument("--max-rows", type=int, default=None,
                        help="Limit rows for local testing, e.g. --max-rows 100000")
    args = parser.parse_args()

    main(args.input, args.output, max_rows=args.max_rows)
