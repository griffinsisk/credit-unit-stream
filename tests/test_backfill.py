import os
import sys
from decimal import Decimal
from unittest.mock import patch, MagicMock

import pytest

# Ensure scripts/ is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from backfill import generate_months, run_backfill, parse_multicolumn_csv, _clean_amount, detect_csv_format


# ---------------------------------------------------------------------------
# Month range generation
# ---------------------------------------------------------------------------

def test_generate_single_month():
    assert generate_months("2025-01", "2025-01") == ["2025-01"]


def test_generate_full_year():
    months = generate_months("2025-01", "2025-12")
    assert len(months) == 12
    assert months[0] == "2025-01"
    assert months[-1] == "2025-12"


def test_generate_13_months_cross_year():
    months = generate_months("2025-01", "2026-01")
    assert len(months) == 13
    assert months[0] == "2025-01"
    assert months[-1] == "2026-01"


def test_generate_mid_year_to_mid_year():
    months = generate_months("2025-06", "2025-09")
    assert months == ["2025-06", "2025-07", "2025-08", "2025-09"]


# ---------------------------------------------------------------------------
# Amount cleaning
# ---------------------------------------------------------------------------

def test_clean_amount_normal():
    assert _clean_amount("$100.00") == Decimal("100.00")


def test_clean_amount_with_spaces():
    assert _clean_amount("$ 3.48") == Decimal("3.48")


def test_clean_amount_with_commas_and_spaces():
    assert _clean_amount("$ 6,407.96") == Decimal("6407.96")


def test_clean_amount_dash_is_zero():
    assert _clean_amount("$ -") == Decimal("0")


def test_clean_amount_empty_is_zero():
    assert _clean_amount("") == Decimal("0")


def test_clean_amount_negative():
    assert _clean_amount("-$100.00") == Decimal("100.00")


# ---------------------------------------------------------------------------
# CSV format detection
# ---------------------------------------------------------------------------

def test_detect_two_column():
    assert detect_csv_format("123456789012,$100.00\n") == 2


def test_detect_multi_column():
    assert detect_csv_format("123456789012,$10.00,$20.00,$30.00\n") == 4


# ---------------------------------------------------------------------------
# Multi-column parsing
# ---------------------------------------------------------------------------

def test_parse_multicolumn_basic():
    csv_text = "123456789012,$ 10.00,$ 20.00,$ 30.00\n"
    result = parse_multicolumn_csv(csv_text, 3)
    assert len(result) == 3
    assert result[0][0]["amount_usd"] == Decimal("10.00")
    assert result[1][0]["amount_usd"] == Decimal("20.00")
    assert result[2][0]["amount_usd"] == Decimal("30.00")


def test_parse_multicolumn_empty_cells():
    csv_text = "123456789012,,,$ 5.00\n"
    result = parse_multicolumn_csv(csv_text, 3)
    assert result[0][0]["amount_usd"] == Decimal("0")
    assert result[1][0]["amount_usd"] == Decimal("0")
    assert result[2][0]["amount_usd"] == Decimal("5.00")


def test_parse_multicolumn_dash():
    csv_text = "123456789012,$ -,$ 10.00\n"
    result = parse_multicolumn_csv(csv_text, 2)
    assert result[0][0]["amount_usd"] == Decimal("0")
    assert result[1][0]["amount_usd"] == Decimal("10.00")


# ---------------------------------------------------------------------------
# Dry run — no API calls
# ---------------------------------------------------------------------------

def test_dry_run_does_not_call_api(tmp_path):
    csv_file = tmp_path / "credits.csv"
    csv_file.write_text("123456789012,$100.00\n")

    with patch("backfill.cloudzero_client.post_telemetry") as mock_post:
        results = run_backfill(
            csv_path=str(csv_file),
            start_month="2025-01",
            end_month="2025-03",
            dry_run=True,
        )

    mock_post.assert_not_called()
    assert len(results) == 3
    assert all(r["status"] == "dry_run" for r in results)


def test_dry_run_multicolumn(tmp_path):
    csv_file = tmp_path / "credits.csv"
    csv_file.write_text("123456789012,$ 10.00,$ 20.00,$ 30.00\n")

    with patch("backfill.cloudzero_client.post_telemetry") as mock_post:
        results = run_backfill(
            csv_path=str(csv_file),
            start_month="2025-01",
            end_month="2025-03",
            dry_run=True,
        )

    mock_post.assert_not_called()
    assert len(results) == 3
    assert all(r["status"] == "dry_run" for r in results)


# ---------------------------------------------------------------------------
# Failure and continue
# ---------------------------------------------------------------------------

def test_continues_on_failure(tmp_path, monkeypatch):
    csv_file = tmp_path / "credits.csv"
    csv_file.write_text("123456789012,$100.00\n")

    monkeypatch.setenv("CLOUDZERO_API_KEY", "test-key")
    monkeypatch.setenv("CLOUDZERO_METRIC_NAME", "test-metric")

    call_count = 0

    def mock_post(api_key, metric_name, records):
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise Exception("API error on month 2")
        return {}

    with patch("backfill.cloudzero_client.post_telemetry", side_effect=mock_post), \
         patch("backfill.time.sleep"):
        results = run_backfill(
            csv_path=str(csv_file),
            start_month="2025-01",
            end_month="2025-03",
            dry_run=False,
            delay=0,
        )

    assert len(results) == 3
    assert results[0]["status"] == "ok"
    assert results[1]["status"] == "failed"
    assert results[2]["status"] == "ok"
