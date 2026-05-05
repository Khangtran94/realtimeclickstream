"""
ingest_to_kinesis.py
--------------------
Reads REES46 eCommerce CSV data, enriches each row,
and sends it to AWS Kinesis Data Streams in batches.

Usage:
    python ingest_to_kinesis.py --file data/2019-Oct.csv
    python ingest_to_kinesis.py --file data/2019-Nov.csv
"""

import argparse
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import os

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Config — loaded from .env file (never hardcoded)
# ---------------------------------------------------------------------------

load_dotenv()  # reads .env file if present

STREAM_NAME = os.environ["KINESIS_STREAM_NAME"]          # required — will raise if missing
AWS_REGION  = os.environ.get("AWS_REGION", "ap-southeast-1")  # optional — defaults to us-east-1

CHUNK_SIZE = 100000    # rows read from CSV at a time (memory-safe)
BATCH_SIZE = 500     # max records per Kinesis put_records() call
# LOG_EVERY = 100000   # print progress every N records

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Step 2 — CLEAN
# ---------------------------------------------------------------------------

REQUIRED_FIELDS = ["user_id", "product_id", "price"]

def clean_row(row: dict) -> dict | None:
    """
    Validate and clean a single CSV row.
    Returns None if the row should be dropped.
    """
    # Drop rows missing required fields
    for field in REQUIRED_FIELDS:
        if pd.isna(row.get(field)):
            return None

    # Convert price to float
    try:
        row["price"] = float(row["price"])
    except (ValueError, TypeError):
        return None

    # Convert event_time to ISO string (handle nulls gracefully)
    if pd.isna(row.get("event_time")):
        row["event_time"] = None
    else:
        try:
            row["event_time"] = pd.to_datetime(row["event_time"]).isoformat()
        except Exception:
            row["event_time"] = None

    # Normalize nullable string fields
    for field in ["category_id", "category_code", "brand"]:
        if pd.isna(row.get(field)):
            row[field] = None
        else:
            row[field] = str(row[field])

    # Ensure user_id and product_id are strings
    row["user_id"] = str(row["user_id"])
    row["product_id"] = str(row["product_id"])

    return row

# ---------------------------------------------------------------------------
# Step 3 — ENRICH
# ---------------------------------------------------------------------------

def enrich_row(row: dict) -> dict:
    """
    Add pipeline metadata columns that don't exist in the raw CSV.
    """
    return {
        # --- original CSV fields ---
        "event_time":               row.get("event_time"),
        "event_type":               row.get("event_type"),
        "product_id":               row["product_id"],
        "category_id":              row.get("category_id"),
        "category_code":            row.get("category_code"),
        "brand":                    row.get("brand"),
        "price":                    row["price"],
        "user_id":                  row["user_id"],

        # --- renamed field ---
        "session_id":               str(row.get("user_session", "")),

        # --- new pipeline metadata fields ---
        "event_id":                 str(uuid.uuid4()),
        "arrival_time":             datetime.now(timezone.utc).isoformat(),
        "is_late_arrival":          False,       # hardcoded for CSV ingestion
        "late_arrival_delay_seconds": None,      # hardcoded for CSV ingestion
    }

# ---------------------------------------------------------------------------
# Step 4+5 — BATCH & SEND
# ---------------------------------------------------------------------------

def format_kinesis_record(enriched_row: dict) -> dict:
    """
    Wrap an enriched row into the format Kinesis put_records() expects.
    Partition key = user_id to preserve ordering per user.
    """
    return {
        "Data": json.dumps(enriched_row).encode("utf-8"),
        "PartitionKey": enriched_row["user_id"],
    }


def send_batch(kinesis_client, records: list, retry_count: int = 3) -> int:
    """
    Send a batch of up to 500 records to Kinesis.
    Retries failed records up to retry_count times.
    Returns the number of successfully sent records.
    """
    remaining = records
    success_count = 0

    for attempt in range(1, retry_count + 1):
        try:
            response = kinesis_client.put_records(
                StreamName=STREAM_NAME,
                Records=remaining,
            )
        except ClientError as e:
            log.error("Kinesis ClientError: %s", e)
            break

        failed_count = response.get("FailedRecordCount", 0)
        success_count += len(remaining) - failed_count

        if failed_count == 0:
            break  # all records sent successfully

        # Collect only the failed records for retry
        failed_records = [
            remaining[i]
            for i, rec in enumerate(response["Records"])
            if "ErrorCode" in rec
        ]

        log.warning(
            "Attempt %d: %d/%d records failed — retrying...",
            attempt, failed_count, len(remaining)
        )

        remaining = failed_records
        time.sleep(2 ** attempt)  # exponential backoff: 2s, 4s, 8s

    return success_count

# ---------------------------------------------------------------------------
# Step 6 — LOG PROGRESS
# ---------------------------------------------------------------------------

def log_progress(total_sent: int, total_dropped: int, start_time: float):
    elapsed = time.time() - start_time
    log.info(
        "Progress — sent: %d | dropped: %d | elapsed: %.1fs",
        total_sent, total_dropped, elapsed,
    )

# ---------------------------------------------------------------------------
# Main — wire all steps together
# ---------------------------------------------------------------------------

def ingest_file(kinesis_client, path: Path, max_rows: int = None) -> tuple[int, int]:
    """
    Process a single CSV file — clean, enrich, batch, send.
    Returns (total_sent, total_dropped).
    """
    # Count total rows upfront so tqdm can show % complete + ETA
    log.info("Counting rows in %s (one-time scan)...", path.name)
    total_rows = sum(1 for _ in open(path)) - 1  # minus header
    if max_rows is not None:
        total_rows = min(total_rows, max_rows)
    log.info("Total rows to process: %d", total_rows)

    total_sent = 0
    total_dropped = 0
    batch = []
    start_time = time.time()

    # Step 1 — READ in chunks (memory-safe for 14GB files)
    progress = tqdm(total=total_rows, unit="rows", desc=path.name, dynamic_ncols=True)
    
    read_kwargs = {"chunksize": CHUNK_SIZE, "low_memory": False}
    if max_rows is not None:
        read_kwargs["nrows"] = max_rows

    for chunk in pd.read_csv(path, **read_kwargs):
        for raw_row in chunk.itertuples(index=False):
            row_dict = raw_row._asdict()

            # Step 2 — CLEAN
            cleaned = clean_row(row_dict)
            if cleaned is None:
                total_dropped += 1
                progress.update(1)
                continue

            # Step 3 — ENRICH
            enriched = enrich_row(cleaned)

            # Step 4 — BATCH
            batch.append(format_kinesis_record(enriched))

            # Step 5 — SEND when batch is full
            if len(batch) >= BATCH_SIZE:
                total_sent += send_batch(kinesis_client, batch)
                batch = []

            # Step 6 — LOG progress
            # if (total_sent + total_dropped) % LOG_EVERY == 0 and (total_sent + total_dropped) > 0:
            #     log_progress(total_sent, total_dropped, start_time)

            progress.update(1)
            progress.set_postfix(sent=total_sent, dropped=total_dropped)

    progress.close()

    # Send any remaining records that didn't fill a full batch
    if batch:
        total_sent += send_batch(kinesis_client, batch)

    elapsed = time.time() - start_time
    log.info("File done — sent: %d | dropped: %d | elapsed: %.1fs", total_sent, total_dropped, elapsed)

    return total_sent, total_dropped


def ingest(file_path: str = None, folder_path: str = None, max_rows: int = None):
    """
    Entry point — accepts either a single file or a folder of CSVs.
    """
    kinesis_client = boto3.client("kinesis", region_name=AWS_REGION)

    # Collect the list of CSV files to process
    if folder_path:
        folder = Path(folder_path)
        if not folder.exists():
            raise FileNotFoundError(f"Folder not found: {folder_path}")
        csv_files = sorted(folder.glob("*.csv"))
        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in: {folder_path}")
    else:
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"CSV not found: {file_path}")
        csv_files = [path]

    # Summary across all files
    log.info("Found %d CSV file(s) to process:", len(csv_files))
    for i, f in enumerate(csv_files, 1):
        log.info("  %d. %s", i, f.name)

    grand_sent = 0
    grand_dropped = 0
    grand_start = time.time()

    for i, csv_path in enumerate(csv_files, 1):
        log.info("=" * 50)
        log.info("Processing file %d/%d: %s", i, len(csv_files), csv_path.name)
        log.info("=" * 50)
        sent, dropped = ingest_file(kinesis_client, csv_path, max_rows=max_rows)
        grand_sent += sent
        grand_dropped += dropped

    # Final summary across all files
    grand_elapsed = time.time() - grand_start
    log.info("=" * 50)
    log.info("All files complete!")
    log.info("  Total sent:    %d", grand_sent)
    log.info("  Total dropped: %d", grand_dropped)
    log.info("  Total elapsed: %.1f seconds", grand_elapsed)
    log.info("  Avg rate:      %.0f records/sec", grand_sent / grand_elapsed if grand_elapsed > 0 else 0)
    log.info("=" * 50)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest REES46 CSV data into Kinesis")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--file",
        help="Single CSV file, e.g. data/2019-Oct.csv",)
    group.add_argument(
        "--folder",
        help="Folder containing CSV files, e.g. data",)

    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Max rows to ingest per file (default: all rows). e.g. --max-rows 10000",
    )
    args = parser.parse_args()
    ingest(file_path=args.file, folder_path=args.folder, max_rows=args.max_rows)