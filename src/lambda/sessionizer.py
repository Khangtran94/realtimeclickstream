"""
sessionizer.py
--------------
Lambda function triggered by Kinesis Data Streams.
Reads click events, builds/updates active sessions in DynamoDB.

Session logic:
- Group events by session_id
- Store per-session summary in DynamoDB with TTL = 2 hours from last event
"""

import base64
import json
import logging
import os
import time
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DYNAMODB_TABLE = os.environ["DYNAMODB_TABLE"]          # e.g. "clickstream-sessions"
AWS_REGION     = os.environ.get("AWS_REGION", "ap-southeast-1")
SESSION_TTL_SECONDS = 2 * 60 * 60                      # 2 hours

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


def float_to_decimal(value):
    """DynamoDB does not accept float — convert to Decimal."""
    if isinstance(value, float):
        return Decimal(str(value))
    return value

# ---------------------------------------------------------------------------
# Core session logic
# ---------------------------------------------------------------------------

def build_session_update(existing: dict | None, event: dict) -> dict:
    """
    Merge a new event into an existing session (or create a new one).
    Returns the updated session item ready for DynamoDB.
    """
    now = now_epoch()

    if existing is None:
        # Brand new session
        session = {
            "session_id":    event["session_id"],
            "user_id":       event["user_id"],
            "started_at":    event.get("event_time") or event["arrival_time"],
            "last_event_at": event.get("event_time") or event["arrival_time"],
            "event_count":   1,
            "event_types":   {event.get("event_type", "unknown")},
            "products_seen": {event["product_id"]},
            "total_value":   float_to_decimal(event.get("price") or 0.0),
            "ttl":           now + SESSION_TTL_SECONDS,
        }
    else:
        # Update existing session
        event_types   = set(existing.get("event_types", []))
        products_seen = set(existing.get("products_seen", []))
        event_types.add(event.get("event_type", "unknown"))
        products_seen.add(event["product_id"])

        session = {
            **existing,
            "last_event_at": event.get("event_time") or event["arrival_time"],
            "event_count":   int(existing.get("event_count", 0)) + 1,
            "event_types":   event_types,
            "products_seen": products_seen,
            "total_value":   float_to_decimal(
                float(existing.get("total_value", 0)) + float(event.get("price") or 0.0)
            ),
            "ttl": now + SESSION_TTL_SECONDS,   # reset TTL on activity
        }

    # DynamoDB doesn't support Python sets natively — convert to list
    session["event_types"]   = list(session["event_types"])
    session["products_seen"] = list(session["products_seen"])

    return session


def upsert_session(event: dict):
    """
    Read the current session from DynamoDB, merge the new event, write back.
    """
    session_id = event.get("session_id")
    if not session_id:
        log.warning("Event missing session_id, skipping: %s", event.get("event_id"))
        return

    # Read existing session (may not exist yet)
    response = table.get_item(Key={"session_id": session_id})
    existing = response.get("Item")

    updated = build_session_update(existing, event)
    table.put_item(Item=updated)
    log.debug("Upserted session %s (events: %d)", session_id, updated["event_count"])

# ---------------------------------------------------------------------------
# Lambda handler
# ---------------------------------------------------------------------------

def handler(event: dict, context) -> dict:
    """
    Entry point called by AWS Lambda.
    `event["Records"]` contains a batch of Kinesis records.
    """
    records = event.get("Records", [])
    log.info("Received %d Kinesis record(s)", len(records))

    success = 0
    failed  = 0

    for record in records:
        click_event = decode_kinesis_record(record)
        if click_event is None:
            failed += 1
            continue

        try:
            upsert_session(click_event)
            success += 1
        except Exception as e:
            log.error("Failed to upsert session for event %s: %s",
                      click_event.get("event_id"), e)
            failed += 1

    log.info("Done — success: %d | failed: %d", success, failed)
    return {"statusCode": 200, "success": success, "failed": failed}
