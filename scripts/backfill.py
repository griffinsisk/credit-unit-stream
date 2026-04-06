#!/usr/bin/env python3
"""Backfill historical credits to CloudZero Unit Metric Telemetry.

One-time CLI script that posts credits for multiple billing months.
Supports two CSV formats:

  1. Multi-column (one column per month):
       account_id, Jan amt, Feb amt, ..., Dec amt
     Use with --start-month and --end-month matching the column count.

  2. Two-column (single month per file):
       account_id, amount
     Posts the same data for every month in the range.

Usage:
    # Multi-column CSV (13 months of data, one column per month)
    python scripts/backfill.py \
        --csv credits-history.csv \
        --start-month 2025-01 \
        --end-month 2026-01 \
        --dry-run

    # Two-column CSV (same data for all months)
    python scripts/backfill.py \
        --csv credits-2025-01.csv \
        --start-month 2025-01 \
        --end-month 2026-01 \
        --dry-run

Requires CLOUDZERO_API_KEY and CLOUDZERO_METRIC_NAME environment variables.
"""

import argparse
import csv
import io
import os
import re
import sys
import time
from decimal import Decimal, InvalidOperation

# Allow imports from src/ when run from repo root or scripts/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import cloudzero_client
import csv_parser
from handler import _build_telemetry_records

_ACCOUNT_RE = re.compile(r"^\d{12}$")


def _clean_amount(raw: str) -> Decimal:
    """Parse a dollar amount string into a positive Decimal.

    Handles: "$6,407.96", "$ 3.48", "$ -", "", empty strings.
    Returns Decimal("0") for zero-like values.
    Raises InvalidOperation for unparseable values.
    """
    cleaned = raw.replace("$", "").replace(",", "").strip()
    if cleaned in ("", "-"):
        return Decimal("0")
    return abs(Decimal(cleaned))


def parse_multicolumn_csv(csv_text: str, num_months: int) -> list[list[dict]]:
    """Parse a multi-column CSV where each column after account_id is a month.

    Returns a list of num_months lists, each containing
    [{"account_id": str, "amount_usd": Decimal}, ...] for that month.
    """
    months_data = [[] for _ in range(num_months)]

    reader = csv.reader(io.StringIO(csv_text))
    for line_num, row in enumerate(reader, start=1):
        if len(row) < 2:
            continue

        raw_account = row[0].strip()
        account_id = raw_account.zfill(12)

        if not _ACCOUNT_RE.match(account_id):
            continue

        # Determine how many amount columns this row has
        amount_cols = row[1:]

        for col_idx in range(num_months):
            if col_idx < len(amount_cols):
                raw = amount_cols[col_idx].strip()
                try:
                    amount = _clean_amount(raw)
                except InvalidOperation:
                    amount = Decimal("0")
            else:
                amount = Decimal("0")

            months_data[col_idx].append({
                "account_id": account_id,
                "amount_usd": amount,
            })

    return months_data


def detect_csv_format(csv_text: str) -> int:
    """Return the number of columns in the CSV. 2 = simple, >2 = multi-column."""
    reader = csv.reader(io.StringIO(csv_text))
    for row in reader:
        if len(row) >= 2:
            return len(row)
    return 0


def generate_months(start: str, end: str) -> list[str]:
    """Generate YYYY-MM strings from start to end inclusive."""
    sy, sm = (int(x) for x in start.split("-"))
    ey, em = (int(x) for x in end.split("-"))

    months = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        months.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months


def run_backfill(
    csv_path: str,
    start_month: str,
    end_month: str,
    dry_run: bool = False,
    delay: float = 2.0,
) -> list[dict]:
    """Run the backfill, returning a list of result dicts per month."""
    api_key = os.environ.get("CLOUDZERO_API_KEY", "")
    metric_name = os.environ.get("CLOUDZERO_METRIC_NAME", "")

    if not dry_run:
        if not api_key:
            print("ERROR: CLOUDZERO_API_KEY environment variable not set")
            sys.exit(1)
        if not metric_name:
            print("ERROR: CLOUDZERO_METRIC_NAME environment variable not set")
            sys.exit(1)

    with open(csv_path, "r") as f:
        csv_text = f.read()

    months = generate_months(start_month, end_month)
    num_cols = detect_csv_format(csv_text)
    is_multicolumn = num_cols > 2

    if is_multicolumn:
        amount_cols = num_cols - 1  # subtract the account_id column
        if amount_cols != len(months):
            print(f"WARNING: CSV has {amount_cols} amount columns but {len(months)} months requested")
            print(f"  Will use min({amount_cols}, {len(months)}) months")
            effective_months = min(amount_cols, len(months))
            months = months[:effective_months]

        months_data = parse_multicolumn_csv(csv_text, len(months))
        print(f"CSV: {csv_path} (multi-column: {amount_cols} months, {len(months_data[0])} accounts)")
    else:
        credits = csv_parser.parse_credits_csv(csv_text)
        if not credits:
            print(f"ERROR: No valid rows parsed from {csv_path}")
            sys.exit(1)
        months_data = [credits] * len(months)
        print(f"CSV: {csv_path} (2-column: {len(credits)} accounts, same data for all months)")

    print(f"Months: {months[0]} through {months[-1]} ({len(months)} months)")
    if dry_run:
        print("MODE: dry-run (no API calls)")
    print()

    results = []
    failures = []

    for i, month_str in enumerate(months, start=1):
        year, month = (int(x) for x in month_str.split("-"))
        timestamp = f"{year:04d}-{month:02d}-01T00:00:00Z"

        credits = months_data[i - 1]
        # Filter out zero-amount accounts for this month
        nonzero = [c for c in credits if c["amount_usd"] != Decimal("0")]
        total_credit = sum(c["amount_usd"] for c in nonzero)

        telemetry_records = _build_telemetry_records(nonzero, timestamp)

        if dry_run:
            print(f"[{i}/{len(months)}] {month_str} — {len(nonzero)} accounts, ${total_credit:,.2f} — DRY RUN")
            results.append({"month": month_str, "status": "dry_run", "accounts": len(nonzero)})
            continue

        if not nonzero:
            print(f"[{i}/{len(months)}] {month_str} — 0 accounts, $0.00 — SKIPPED (no data)")
            results.append({"month": month_str, "status": "skipped"})
            continue

        try:
            cloudzero_client.post_telemetry(api_key, metric_name, telemetry_records)
            print(f"[{i}/{len(months)}] {month_str} — {len(nonzero)} accounts, ${total_credit:,.2f} — OK")
            results.append({"month": month_str, "status": "ok"})
        except Exception as exc:
            print(f"[{i}/{len(months)}] {month_str} — FAILED: {exc}")
            results.append({"month": month_str, "status": "failed", "error": str(exc)})
            failures.append(month_str)

        if i < len(months):
            time.sleep(delay)

    print()
    if failures:
        print(f"DONE — {len(failures)} failure(s): {', '.join(failures)}")
    elif dry_run:
        print(f"DRY RUN COMPLETE — {len(months)} months validated")
    else:
        print(f"DONE — {len(months)} months posted successfully")

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Backfill historical credits to CloudZero Unit Metric Telemetry"
    )
    parser.add_argument("--csv", required=True, help="Path to credits CSV file")
    parser.add_argument("--start-month", required=True, help="First billing month (YYYY-MM)")
    parser.add_argument("--end-month", required=True, help="Last billing month (YYYY-MM)")
    parser.add_argument("--dry-run", action="store_true", help="Validate without posting to API")
    parser.add_argument("--delay", type=float, default=2.0, help="Seconds between API calls (default: 2)")

    args = parser.parse_args()
    run_backfill(args.csv, args.start_month, args.end_month, args.dry_run, args.delay)


if __name__ == "__main__":
    main()
