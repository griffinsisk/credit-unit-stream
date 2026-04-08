# Architecture

## Overview

```
┌─────────────────┐     S3 ObjectCreated     ┌──────────────────┐
│   S3 Bucket     │ ─────────────────────── ▶ │  AWS Lambda      │
│  credits-YYYY   │    (prefix: credits-,     │  (Python 3.12)   │
│  -MM.csv drop   │     suffix: .csv)         └────────┬─────────┘
└─────────────────┘                                    │
                                     ┌─────────────────┤
                                     │                 │
                                     ▼                 ▼
                           ┌──────────────────┐  ┌────────────────────┐
                           │  CloudZero API   │  │  CloudWatch Logs   │
                           │  Unit Metric     │  │  (structured JSON) │
                           │  Telemetry       │  └────────────────────┘
                           └──────────────────┘
```

## Components

### S3 Bucket (`aws-credits-pipeline-{AccountId}-{Region}`)
- Receives the monthly credits CSV file.
- Naming uses AccountId + Region to ensure global uniqueness.
- All public access blocked.
- Event filter: prefix `credits-`, suffix `.csv` — prevents accidental triggers from other files.

### Lambda (`aws-credits-processor`)
- Runtime: Python 3.12
- Timeout: 60s, Memory: 256 MB
- Triggered by S3 `ObjectCreated` events matching the filter above.
- Reads the file from S3, parses it, calls CloudZero, logs structured JSON to CloudWatch.

### CloudZero Unit Metric Telemetry (`/unit-cost/v1/telemetry/metric/{metric_name}/replace`)
- Operation: `replace` — replaces all data for the billing month.
- Auth: `Authorization: <api_key>` (no Bearer prefix).
- Idempotent: re-uploading the same file for a month replaces rather than appends.

### CloudWatch Logs (`/aws/lambda/aws-credits-processor`)
- Structured JSON via `python-json-logger`.
- Final summary log includes: file key, billing month, row count, unique accounts, total credit USD.

## Module Breakdown

| Module | Responsibility |
|---|---|
| `handler.py` | Lambda entry point; S3 event parsing; orchestration; summary log |
| `csv_parser.py` | Pure CSV parsing; duplicate handling; Decimal amounts |
| `cloudzero_client.py` | HTTP POST to CloudZero Unit Metric Telemetry; retry on 429/5xx; size guard |
| `logger.py` | Structured JSON logger factory |

## Data Flow

1. File uploaded: `s3://aws-credits-pipeline-{account}-{region}/credits-2025-01.csv`
2. Lambda triggered; billing month extracted from filename (`credits-YYYY-MM.csv`).
3. CSV downloaded; parsed into `[{account_id, amount_usd}]` with 23 unique rows.
4. Each row converted to a telemetry record: value (positive), granularity MONTHLY, associated_cost with account.
5. Single `POST` to `/replace` endpoint; ensures idempotency.
6. Structured summary logged to CloudWatch.

## Security

- API key stored as CloudFormation `NoEcho` parameter; passed as Lambda env var. Never hardcoded.
- Lambda IAM role scoped to `s3:GetObject` on `credits-*.csv` objects only.
- No VPC required; all outbound via Lambda's default internet access.
