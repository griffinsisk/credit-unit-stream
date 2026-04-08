import io
import json
import logging
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

import handler


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SAMPLE_CSV = """\
062951470133,$0.00
073933415963,"$6,407.96"
233466153464,"$23,436.83"
235494797904,"$1,828.24"
284954390955,"$32,728.09"
331238891350,$453.84
339712813083,"$32,258.79"
400210543042,$412.07
497723851829,"$116,319.27"
546885038385,"$1,000.54"
568064661016,"$3,233.09"
585876524959,"$13,154.71"
605134457544,"$16,883.68"
654654353366,"$25,087.88"
767397766098,"$4,812.64"
785080043467,"$6,743.73"
054736553085,"$11,845.35"
197545325174,"$22,393.16"
332999210940,$132.10
832337494075,"$2,016.40"
869578675382,$624.05
992382782732,"$11,124.29"
664220444271,"$3,938.27"
054736553085,"$11,845.35"
"""


def make_s3_event(key: str, bucket: str = "my-credits-bucket") -> dict:
    return {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": bucket},
                    "object": {"key": key},
                }
            }
        ]
    }


def make_s3_client_mock(csv_text: str):
    body_mock = MagicMock()
    body_mock.read.return_value = csv_text.encode("utf-8")
    s3_mock = MagicMock()
    s3_mock.get_object.return_value = {"Body": body_mock}
    return s3_mock


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

def test_happy_path_returns_summary(monkeypatch):
    monkeypatch.setenv("CLOUDZERO_API_KEY", "test-key")
    monkeypatch.setenv("CLOUDZERO_METRIC_NAME", "map-credit-estimates")

    s3_mock = make_s3_client_mock(SAMPLE_CSV)

    with patch("handler.boto3.client", return_value=s3_mock), \
         patch("handler.cloudzero_client.post_telemetry", return_value={}) as mock_post:
        result = handler.lambda_handler(make_s3_event("credits-2025-01.csv"), None)

    assert result["statusCode"] == 200
    assert result["billing_month"] == "2025-01-01T00:00:00Z"
    assert result["row_count"] == 23
    assert result["unique_accounts"] == 23
    mock_post.assert_called_once()


# ---------------------------------------------------------------------------
# Billing month extraction
# ---------------------------------------------------------------------------

def test_billing_month_from_plain_filename(monkeypatch):
    monkeypatch.setenv("CLOUDZERO_API_KEY", "k")
    monkeypatch.setenv("CLOUDZERO_METRIC_NAME", "m")

    s3_mock = make_s3_client_mock("123456789012,$10.00\n")

    with patch("handler.boto3.client", return_value=s3_mock), \
         patch("handler.cloudzero_client.post_telemetry", return_value={}):
        result = handler.lambda_handler(make_s3_event("credits-2025-01.csv"), None)

    assert result["billing_month"] == "2025-01-01T00:00:00Z"


def test_billing_month_from_prefixed_key(monkeypatch):
    monkeypatch.setenv("CLOUDZERO_API_KEY", "k")
    monkeypatch.setenv("CLOUDZERO_METRIC_NAME", "m")

    s3_mock = make_s3_client_mock("123456789012,$10.00\n")

    with patch("handler.boto3.client", return_value=s3_mock), \
         patch("handler.cloudzero_client.post_telemetry", return_value={}):
        result = handler.lambda_handler(make_s3_event("uploads/credits-2025-01.csv"), None)

    assert result["billing_month"] == "2025-01-01T00:00:00Z"


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------

def test_raises_on_bad_filename(monkeypatch):
    monkeypatch.setenv("CLOUDZERO_API_KEY", "k")
    monkeypatch.setenv("CLOUDZERO_METRIC_NAME", "m")

    s3_mock = make_s3_client_mock("123456789012,$10.00\n")

    with patch("handler.boto3.client", return_value=s3_mock):
        with pytest.raises(ValueError, match="does not match pattern"):
            handler.lambda_handler(make_s3_event("random-file.csv"), None)


def test_raises_on_empty_csv(monkeypatch):
    monkeypatch.setenv("CLOUDZERO_API_KEY", "k")
    monkeypatch.setenv("CLOUDZERO_METRIC_NAME", "m")

    s3_mock = make_s3_client_mock("")

    with patch("handler.boto3.client", return_value=s3_mock):
        with pytest.raises(ValueError, match="No valid rows"):
            handler.lambda_handler(make_s3_event("credits-2025-01.csv"), None)


def test_raises_when_api_call_fails(monkeypatch):
    monkeypatch.setenv("CLOUDZERO_API_KEY", "k")
    monkeypatch.setenv("CLOUDZERO_METRIC_NAME", "m")

    s3_mock = make_s3_client_mock("123456789012,$10.00\n")

    from requests.exceptions import HTTPError
    mock_response = MagicMock()
    mock_response.status_code = 500
    api_error = HTTPError(response=mock_response)

    with patch("handler.boto3.client", return_value=s3_mock), \
         patch("handler.cloudzero_client.post_telemetry", side_effect=api_error):
        with pytest.raises(HTTPError):
            handler.lambda_handler(make_s3_event("credits-2025-01.csv"), None)


# ---------------------------------------------------------------------------
# Structured summary log
# ---------------------------------------------------------------------------

def test_summary_log_emitted(monkeypatch, caplog):
    monkeypatch.setenv("CLOUDZERO_API_KEY", "k")
    monkeypatch.setenv("CLOUDZERO_METRIC_NAME", "m")

    s3_mock = make_s3_client_mock("123456789012,$50.00\n234567890123,$25.00\n")

    with caplog.at_level(logging.INFO, logger="handler"), \
         patch("handler.boto3.client", return_value=s3_mock), \
         patch("handler.cloudzero_client.post_telemetry", return_value={}):
        handler.lambda_handler(make_s3_event("credits-2025-03.csv"), None)

    messages = [r.message for r in caplog.records]
    assert any("credits pipeline complete" in m for m in messages)


# ---------------------------------------------------------------------------
# Telemetry record field values
# ---------------------------------------------------------------------------

def test_telemetry_records_have_correct_fields(monkeypatch):
    monkeypatch.setenv("CLOUDZERO_API_KEY", "k")
    monkeypatch.setenv("CLOUDZERO_METRIC_NAME", "m")

    s3_mock = make_s3_client_mock("123456789012,$10.00\n")
    captured = {}

    def capture_post(api_key, metric_name, records):
        captured["records"] = records
        return {}

    with patch("handler.boto3.client", return_value=s3_mock), \
         patch("handler.cloudzero_client.post_telemetry", side_effect=capture_post):
        handler.lambda_handler(make_s3_event("credits-2025-01.csv"), None)

    record = captured["records"][0]
    assert record["timestamp"] == "2025-01-01T00:00:00Z"
    assert record["value"] == "10.00"
    assert "granularity" not in record
    assert record["associated_cost"] == {"accounts": "123456789012"}


def test_telemetry_values_are_positive_strings(monkeypatch):
    """Unit metric values must be positive numeric strings per the API spec."""
    monkeypatch.setenv("CLOUDZERO_API_KEY", "k")
    monkeypatch.setenv("CLOUDZERO_METRIC_NAME", "m")

    s3_mock = make_s3_client_mock("123456789012,$10.00\n234567890123,$25.00\n")
    captured = {}

    def capture_post(api_key, metric_name, records):
        captured["records"] = records
        return {}

    with patch("handler.boto3.client", return_value=s3_mock), \
         patch("handler.cloudzero_client.post_telemetry", side_effect=capture_post):
        handler.lambda_handler(make_s3_event("credits-2025-01.csv"), None)

    for record in captured["records"]:
        assert isinstance(record["value"], str)
        assert float(record["value"]) > 0
