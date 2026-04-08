# Changelog

## [Unit Cost Migration] — 2026-04-08

### Changed
- **Documentation updated for Unit Cost:** All references to "AnyCost Stream" and
  `CLOUDZERO_CONNECTION_ID` updated to reflect the actual Unit Metric Telemetry API
  (`/unit-cost/v1/telemetry/metric/{metric_name}/replace`) and `CLOUDZERO_METRIC_NAME`.
- Updated README, architecture docs, `.env.example`, `samconfig.toml` comments.

## [Post-MVP] — 2026-02-20

### Fixed
- **Negative credits bug:** CSV values already negative (e.g. `-$100.00`) were double-negated
  to positive. Parser now uses `abs()` so credits are always negative in CloudZero.
- **samconfig.toml:** Removed hardcoded connection ID and broken shell variable expansion
  from guided deploy. Reset to clean template.

### Changed
- **CBF row labels:** `lineitem/type` changed from "Credit" to "MAP Credit (Monthly)";
  `lineitem/description` changed from "AWS promotional credit" to "Monthly MAP Credit".
- **Repo structure:** Flattened `aws-credits-pipeline/` to repo root. Internal planning
  artifacts moved to `.internal/` (gitignored). Example CSV moved to `examples/`.

### Added
- `scripts/backfill.py` — CLI tool for one-time historical ingest. Takes `--csv`,
  `--start-month`, `--end-month`, `--dry-run`, `--delay`. Calls CloudZero API directly.
- `tests/test_backfill.py` — 6 tests covering month range generation, dry-run, and
  failure-and-continue behavior.
- README: SSO/federated user guidance, NoEcho explanation, `--parameter-overrides` syntax,
  troubleshooting section, backfill documentation.

## [MVP] — 2026-02-20

### Added
- `src/csv_parser.py` — Parses headerless credits CSV; handles `$`/comma amounts, leading-zero
  account IDs, duplicates (keep first, warn), bad rows (warn and skip).
- `src/cloudzero_client.py` — Posts telemetry to CloudZero Unit Metric API; retries on 429/5xx
  (tenacity, 3 attempts, exponential backoff 2–30s); raises immediately on 4xx; 4.5 MB size guard.
- `src/handler.py` — Lambda entry point; extracts billing month from filename
  (`credits-YYYY-MM.csv`); builds CBF rows with negated amounts; logs structured JSON summary.
- `src/logger.py` — JSON logger factory via `python-json-logger`.
- `template.yaml` — SAM template: S3 bucket, Lambda function, IAM role (least privilege).
- `samconfig.toml` — SAM deploy config; reads API key + connection ID from shell env vars.
- `events/s3_put_event.json` — Sample S3 event for `sam local invoke`.
- `tests/test_csv_parser.py` — 13 tests; 90%+ coverage of csv_parser.
- `tests/test_cloudzero_client.py` — 11 tests; covers happy path, auth header, retry, no-retry, size guard.
- `tests/test_handler.py` — 8 tests; covers billing month extraction, error cases, summary log.
