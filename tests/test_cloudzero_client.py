import json

import pytest
import responses as responses_lib
from requests.exceptions import HTTPError

from cloudzero_client import post_telemetry, _MAX_BODY_BYTES

API_KEY = "test-api-key-no-bearer"
METRIC_NAME = "map-credit-estimates"
URL = f"https://api.cloudzero.com/unit-cost/v1/telemetry/metric/{METRIC_NAME}/replace"

SAMPLE_RECORDS = [
    {
        "timestamp": "2025-01-01T00:00:00Z",
        "value": "50.00",
        "associated_cost": {
            "accounts": "123456789012",
        },
    }
]


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

@responses_lib.activate
def test_happy_path_200():
    responses_lib.add(responses_lib.POST, URL, json={"ok": True}, status=200)
    result = post_telemetry(API_KEY, METRIC_NAME, SAMPLE_RECORDS)
    assert result == {"ok": True}


@responses_lib.activate
def test_authorization_header_no_bearer():
    """Auth header must be exactly the api_key — no 'Bearer ' prefix."""
    responses_lib.add(responses_lib.POST, URL, json={}, status=200)
    post_telemetry(API_KEY, METRIC_NAME, SAMPLE_RECORDS)
    sent = responses_lib.calls[0].request
    assert sent.headers["Authorization"] == API_KEY
    assert "Bearer" not in sent.headers["Authorization"]


@responses_lib.activate
def test_url_contains_metric_name():
    responses_lib.add(responses_lib.POST, URL, json={}, status=200)
    post_telemetry(API_KEY, METRIC_NAME, SAMPLE_RECORDS)
    assert METRIC_NAME in responses_lib.calls[0].request.url


@responses_lib.activate
def test_payload_has_records_key():
    responses_lib.add(responses_lib.POST, URL, json={}, status=200)
    post_telemetry(API_KEY, METRIC_NAME, SAMPLE_RECORDS)
    body = json.loads(responses_lib.calls[0].request.body)
    assert "records" in body
    assert body["records"] == SAMPLE_RECORDS


@responses_lib.activate
def test_uses_replace_endpoint():
    responses_lib.add(responses_lib.POST, URL, json={}, status=200)
    post_telemetry(API_KEY, METRIC_NAME, SAMPLE_RECORDS)
    assert "/replace" in responses_lib.calls[0].request.url


@responses_lib.activate
def test_timeout_30_passed():
    responses_lib.add(responses_lib.POST, URL, json={}, status=200)
    result = post_telemetry(API_KEY, METRIC_NAME, SAMPLE_RECORDS)
    assert result == {}


# ---------------------------------------------------------------------------
# Retry behaviour
# ---------------------------------------------------------------------------

@responses_lib.activate
def test_retries_on_429():
    responses_lib.add(responses_lib.POST, URL, json={}, status=429)
    responses_lib.add(responses_lib.POST, URL, json={}, status=429)
    responses_lib.add(responses_lib.POST, URL, json={"ok": True}, status=200)
    result = post_telemetry(API_KEY, METRIC_NAME, SAMPLE_RECORDS)
    assert result == {"ok": True}
    assert len(responses_lib.calls) == 3


@responses_lib.activate
def test_retries_on_500():
    responses_lib.add(responses_lib.POST, URL, json={}, status=500)
    responses_lib.add(responses_lib.POST, URL, json={"ok": True}, status=200)
    result = post_telemetry(API_KEY, METRIC_NAME, SAMPLE_RECORDS)
    assert result == {"ok": True}
    assert len(responses_lib.calls) == 2


@responses_lib.activate
def test_raises_after_3_retries_on_500():
    for _ in range(3):
        responses_lib.add(responses_lib.POST, URL, json={}, status=500)
    with pytest.raises(HTTPError):
        post_telemetry(API_KEY, METRIC_NAME, SAMPLE_RECORDS)
    assert len(responses_lib.calls) == 3


# ---------------------------------------------------------------------------
# No retry on client errors
# ---------------------------------------------------------------------------

@responses_lib.activate
def test_no_retry_on_401():
    responses_lib.add(responses_lib.POST, URL, json={"error": "unauthorized"}, status=401)
    with pytest.raises(HTTPError) as exc_info:
        post_telemetry(API_KEY, METRIC_NAME, SAMPLE_RECORDS)
    assert exc_info.value.response.status_code == 401
    assert len(responses_lib.calls) == 1


@responses_lib.activate
def test_no_retry_on_400():
    responses_lib.add(responses_lib.POST, URL, json={"error": "bad request"}, status=400)
    with pytest.raises(HTTPError) as exc_info:
        post_telemetry(API_KEY, METRIC_NAME, SAMPLE_RECORDS)
    assert exc_info.value.response.status_code == 400
    assert len(responses_lib.calls) == 1


# ---------------------------------------------------------------------------
# Size guard
# ---------------------------------------------------------------------------

def test_size_guard_raises_before_http_call(monkeypatch):
    """Payload over _MAX_BODY_BYTES should raise ValueError without making HTTP call."""
    import cloudzero_client
    calls = []

    def fake_post(*args, **kwargs):
        calls.append(1)
        raise AssertionError("HTTP call should not have been made")

    monkeypatch.setattr("cloudzero_client.requests.post", fake_post)

    big_record = {"x": "A" * 1000}
    big_data = [big_record] * 5000

    with pytest.raises(ValueError, match="exceeds limit"):
        post_telemetry(API_KEY, METRIC_NAME, big_data)

    assert calls == []
