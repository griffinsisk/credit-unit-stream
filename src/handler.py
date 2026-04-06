import os
import re
from decimal import Decimal

import boto3
from requests.exceptions import HTTPError

import cloudzero_client
import csv_parser
from logger import get_logger

logger = get_logger(__name__)

_FILENAME_RE = re.compile(r"credits-(\d{4})-(\d{2})\.csv$")


def _build_telemetry_records(credits: list[dict], timestamp: str) -> list[dict]:
    records = []
    for credit in credits:
        amount = credit["amount_usd"]
        records.append(
            {
                "timestamp": timestamp,
                "value": float(amount),
                "granularity": "MONTHLY",
                "associated_cost": {
                    "accounts": credit["account_id"],
                },
            }
        )
    return records


def lambda_handler(event: dict, context) -> dict:
    api_key = os.environ["CLOUDZERO_API_KEY"]
    metric_name = os.environ["CLOUDZERO_METRIC_NAME"]

    try:
        record = event["Records"][0]["s3"]
        bucket = record["bucket"]["name"]
        key = record["object"]["key"]
    except (KeyError, IndexError) as exc:
        logger.error("malformed S3 event", extra={"error": str(exc)})
        raise ValueError(f"Malformed S3 event: {exc}") from exc

    filename = key.split("/")[-1]
    match = _FILENAME_RE.search(filename)
    if not match:
        logger.error(
            "filename does not match expected pattern credits-YYYY-MM.csv",
            extra={"key": key},
        )
        raise ValueError(f"Filename '{filename}' does not match pattern credits-YYYY-MM.csv")

    year = int(match.group(1))
    month = int(match.group(2))
    timestamp = f"{year:04d}-{month:02d}-01T00:00:00Z"

    logger.info("processing credits file", extra={"bucket": bucket, "key": key, "billing_month": timestamp})

    try:
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=bucket, Key=key)
        csv_text = obj["Body"].read().decode("utf-8")
    except Exception as exc:
        logger.error("failed to download file from S3", extra={"bucket": bucket, "key": key, "error": str(exc)})
        raise

    try:
        credits = csv_parser.parse_credits_csv(csv_text)
    except Exception as exc:
        logger.error("CSV parsing failed", extra={"error": str(exc)})
        raise

    if not credits:
        logger.error("no valid rows parsed from CSV", extra={"key": key})
        raise ValueError(f"No valid rows parsed from '{key}'")

    telemetry_records = _build_telemetry_records(credits, timestamp)

    total_credit = sum(c["amount_usd"] for c in credits)
    unique_accounts = [c["account_id"] for c in credits]

    try:
        cloudzero_client.post_telemetry(api_key, metric_name, telemetry_records)
    except (ValueError, HTTPError) as exc:
        logger.error("telemetry post failed", extra={"error": str(exc), "billing_month": timestamp})
        raise
    except Exception as exc:
        logger.error("unexpected error posting telemetry", extra={"error": str(exc)})
        raise

    logger.info(
        "credits pipeline complete",
        extra={
            "file": key,
            "billing_month": timestamp,
            "row_count": len(credits),
            "unique_accounts": len(unique_accounts),
            "total_credit_usd": str(total_credit),
        },
    )

    return {
        "statusCode": 200,
        "billing_month": timestamp,
        "row_count": len(credits),
        "unique_accounts": len(unique_accounts),
        "total_credit_usd": str(total_credit),
    }
