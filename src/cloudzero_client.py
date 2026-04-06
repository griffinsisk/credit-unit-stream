import json

import requests
from requests.exceptions import HTTPError
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from logger import get_logger

logger = get_logger(__name__)

_BASE_URL = "https://api.cloudzero.com"
_MAX_BODY_BYTES = 4_500_000  # conservative limit below the 5 MB API cap


def _is_retryable(exc: BaseException) -> bool:
    """Retry on 429 or 5xx; raise immediately on other HTTP errors."""
    if isinstance(exc, HTTPError):
        status = exc.response.status_code
        return status == 429 or status >= 500
    return False


@retry(
    retry=retry_if_exception(_is_retryable),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(3),
    reraise=True,
)
def post_telemetry(
    api_key: str,
    metric_name: str,
    records: list[dict],
) -> dict:
    """POST unit metric telemetry to the CloudZero /replace endpoint.

    Args:
        api_key: CloudZero API key (no Bearer prefix).
        metric_name: Unit metric stream name.
        records: List of telemetry record dicts with timestamp, value,
                 granularity, and associated_cost.

    Returns:
        Parsed JSON response body.

    Raises:
        ValueError: If the payload exceeds the size guard.
        HTTPError: On non-retryable 4xx, or after exhausting retries on 429/5xx.
    """
    payload = {"records": records}

    serialised = json.dumps(payload)
    if len(serialised.encode("utf-8")) > _MAX_BODY_BYTES:
        raise ValueError(
            f"Payload size {len(serialised)} bytes exceeds limit of {_MAX_BODY_BYTES} bytes"
        )

    url = f"{_BASE_URL}/unit-cost/v1/telemetry/metric/{metric_name}/replace"
    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json",
    }

    logger.info(
        "posting unit metric telemetry",
        extra={
            "metric_name": metric_name,
            "record_count": len(records),
        },
    )

    response = requests.post(url, json=payload, headers=headers, timeout=30)
    if not response.ok:
        logger.error(
            "API error response",
            extra={
                "status_code": response.status_code,
                "body": response.text[:500],
                "metric_name": metric_name,
            },
        )
    response.raise_for_status()

    logger.info(
        "telemetry accepted",
        extra={"status_code": response.status_code, "metric_name": metric_name},
    )
    return response.json() if response.content else {}
