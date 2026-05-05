"""
sessionizer.py
--------------
Lambda function triggered by Kinesis Data Streams.
Reads click events, builds/updates active sessions in DynamoDB.

Session logic:
- 30-second tumbling window: AWS buffers Kinesis records for 30s before invoking
  Lambda. All records in the window are grouped by session_id in Python before
  writing to DynamoDB → minimizes DynamoDB writes (1 write per session, not per event)
- 30-minute inactivity boundary: if gap between last known event and new event
  exceeds 30 minutes, close the old session and open a new one
- 2-hour TTL: DynamoDB auto-deletes sessions after 2h of inactivity
"""

import base64
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime
from decimal import Decimal

import boto3

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DYNAMODB_TABLE          = os.environ["DYNAMODB_TABLE"]
AWS_REGION              = os.environ.get("AWS_REGION", "ap-southeast-1")
SESSION_TTL_SECONDS     = 2 * 60 * 60    # 2 hours auto-cleanup
SESSION_TIMEOUT_SECONDS = 30 * 60        # 30-min inactivity = new session

log = logging.getLogger()
log.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table    = dynamodb.Table(DYNAMODB_TABLE)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def now_epoch() -> int:
    return int(time.time())


def decode_kinesis_record(record: dict) -> dict | None:
    """Base64-decode and JSON-parse a single Kinesis record."""
    try:
        raw = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
        return json.loads(raw)
    except Exception as e:
        log.warning("Failed to decode record: %s", e)
        return None


def parse_event_time(event: dict) -> int:
    """
    Parse event_time to Unix epoch seconds.
    Falls back to arrival_time, then current time.
    """
    for field in ("event_time", "arrival_time"):
        val = event.get(field)
        if val:
            try:
                dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
                return int(dt.timestamp())
            except Exception:
                continue
    return now_epoch()


def float_to_decimal(value) -> Decimal:
    """DynamoDB does not accept float — convert to Decimal."""
    if isinstance(value, float):
        return Decimal(str(value))
    return value

# ---------------------------------------------------------------------------
# Step 1 — Tumbling window: group records by session_id
# ---------------------------------------------------------------------------

def group_records_by_session(records: list) -> dict[str, list]:
    """
    30-second tumbling window implementation.

    AWS buffers records for 30s (configured in Terraform), then delivers
    the entire window as one batch to Lambda. Here we group by session_id
    so DynamoDB is written once per session, not once per event.

    Example:
      500 records, 120 unique sessions → 120 DynamoDB reads + 120 writes
      (vs 500 reads + 500 writes without grouping)
    """
    window: dict[str, list] = defaultdict(list)

    for record in records:
        event = decode_kinesis_record(record)
        if event is None:
            continue
        session_id = event.get("session_id")
        if not session_id:
            log.warning("Event missing session_id, skipping: %s", event.get("event_id"))
            continue
        window[session_id].append(event)

    log.info(
        "Tumbling window: %d records → %d unique sessions",
        len(records), len(window)
    )
    return window

# ---------------------------------------------------------------------------
# Step 2 — 30-min inactivity boundary check
# ---------------------------------------------------------------------------

def is_session_expired(existing: dict, new_event_epoch: int) -> bool:
    """
    Returns True if gap between existing session's last event
    and the new event exceeds the 30-minute inactivity threshold.
    """
    last_event_at = existing.get("last_event_at")
    if not last_event_at:
        return False

    try:
        last_epoch = int(datetime.fromisoformat(
            last_event_at.replace("Z", "+00:00")
        ).timestamp())
        gap = new_event_epoch - last_epoch
        if gap > SESSION_TIMEOUT_SECONDS:
            log.info(
                "Session %s expired — gap %ds > %ds, resetting",
                existing.get("session_id"), gap, SESSION_TIMEOUT_SECONDS
            )
            return True
    except Exception as e:
        log.warning("Could not parse last_event_at '%s': %s", last_event_at, e)

    return False

# ---------------------------------------------------------------------------
# Step 3 — Build / update session item
# ---------------------------------------------------------------------------

def build_session(existing: dict | None, events: list) -> dict:
    """
    Merge a list of events (same session_id, same 30s window) into
    either a new or existing session item ready for DynamoDB.

    Events are sorted chronologically so the 30-min boundary check
    is applied in the correct order.
    """
    now = now_epoch()

    # Sort events chronologically within the window
    events_sorted = sorted(events, key=parse_event_time)

    if existing is None:
        first = events_sorted[0]
        session = {
            "session_id":    first["session_id"],
            "user_id":       first["user_id"],
            "started_at":    first.get("event_time") or first.get("arrival_time"),
            "last_event_at": first.get("event_time") or first.get("arrival_time"),
            "event_count":   0,
            "event_types":   set(),
            "products_seen": set(),
            "total_value":   Decimal("0"),
            "ttl":           now + SESSION_TTL_SECONDS,
        }
    else:
        session = {
            **existing,
            "event_types":   set(existing.get("event_types", [])),
            "products_seen": set(existing.get("products_seen", [])),
            "total_value":   float_to_decimal(float(existing.get("total_value", 0))),
            "event_count":   int(existing.get("event_count", 0)),
        }

    for ev in events_sorted:
        ev_epoch = parse_event_time(ev)

        # 30-min inactivity check — reset session if expired
        if existing and is_session_expired(session, ev_epoch):
            session["started_at"]    = ev.get("event_time") or ev.get("arrival_time")
            session["event_count"]   = 0
            session["event_types"]   = set()
            session["products_seen"] = set()
            session["total_value"]   = Decimal("0")

        session["last_event_at"] = ev.get("event_time") or ev.get("arrival_time")
        session["event_count"]   += 1
        session["event_types"].add(ev.get("event_type", "unknown"))
        session["products_seen"].add(ev.get("product_id", "unknown"))
        session["total_value"]   += float_to_decimal(float(ev.get("price") or 0.0))
        session["ttl"]            = now + SESSION_TTL_SECONDS

    # DynamoDB doesn't support Python sets — convert to list
    session["event_types"]   = list(session["event_types"])
    session["products_seen"] = list(session["products_seen"])

    return session


def upsert_session(session_id: str, events: list):
    """
    1 read + 1 write per session (not per event).
    """
    response = table.get_item(Key={"session_id": session_id})
    existing = response.get("Item")

    updated = build_session(existing, events)
    table.put_item(Item=updated)

    log.debug(
        "Upserted session %s — events in window: %d | total: %d",
        session_id, len(events), updated["event_count"]
    )

# ---------------------------------------------------------------------------
# Lambda handler
# ---------------------------------------------------------------------------

def handler(event: dict, context) -> dict:
    """
    Entry point called by AWS Lambda.

    Flow:
      1. Receive batch of Kinesis records (AWS buffered for 30s)
      2. Group by session_id  →  30-sec tumbling window
      3. For each session: check 30-min inactivity boundary, merge events
      4. Write one DynamoDB item per session
    """
    records = event.get("Records", [])
    log.info("Invocation received — %d Kinesis record(s)", len(records))

    # Step 1 — Tumbling window grouping
    window = group_records_by_session(records)

    success = 0
    failed  = 0

    # Step 2+3 — Process each session group
    for session_id, session_events in window.items():
        try:
            upsert_session(session_id, session_events)
            success += 1
        except Exception as e:
            log.error("Failed to upsert session %s: %s", session_id, e)
            failed += 1

    log.info(
        "Done — sessions updated: %d | failed: %d | records processed: %d",
        success, failed, len(records)
    )
    return {
        "statusCode": 200,
        "sessions_updated":  success,
        "sessions_failed":   failed,
        "records_processed": len(records),
    }
