# AWS MAP Credits Pipeline

Automates the monthly push of AWS MAP credits into CloudZero. Drop a credits CSV into S3 — Lambda parses it and posts the data to CloudZero's Unit Metric Telemetry API automatically.

## How it works

```
credits-YYYY-MM.csv  →  S3 bucket  →  Lambda  →  CloudZero Unit Metric Telemetry
                                          ↓
                                    CloudWatch Logs
```

---

## Before you start — install the tools

You need two CLI tools installed. Run these commands to check if they're already installed:

```bash
aws --version
sam --version
```

If either is missing:

```bash
# Install AWS CLI (macOS)
brew install awscli

# Install SAM CLI (macOS)
brew install aws-sam-cli
```

### AWS credentials

You need your AWS CLI configured with credentials for the account you're deploying into.

**Standard IAM user (access key):**

```bash
aws configure
```

It will prompt you for your AWS Access Key ID, Secret Access Key, default region (e.g. `us-east-1`), and output format (just press Enter for default).

**SSO / federated users:**

If your organization uses AWS SSO (IAM Identity Center), configure SSO first:

```bash
aws configure sso
```

Follow the prompts to set up your SSO profile. Then log in before deploying:

```bash
aws sso login --profile your-sso-profile
```

Alternatively, you can copy temporary credentials from the AWS Console: click your username in the top-right → "Command line or programmatic access" → copy the `export` commands for your terminal.

---

## Step 1 — Set your CloudZero credentials

You need two values from CloudZero. Open a terminal and run both export commands with your real values pasted in:

```bash
export CLOUDZERO_API_KEY=paste_your_api_key_here
export CLOUDZERO_METRIC_NAME=paste_your_metric_name_here
```

> **Where to find these:** API key is under Settings > API Keys. The metric name is the name of the Unit Metric you created in CloudZero (e.g. `map-credit-estimates`).

> **Important:** These exports only last for your current terminal session. If you close the terminal and reopen it, you'll need to run these again before deploying.

To confirm they're set:

```bash
echo $CLOUDZERO_API_KEY
echo $CLOUDZERO_METRIC_NAME
```

Both should print your values (not blank).

---

## Step 2 — Build the Lambda package

```bash
sam build
```

This packages your Python code and dependencies for Lambda. You should see `Build Succeeded` at the end.

---

## Step 3 — Deploy to AWS

### First time only — run the guided deploy

```bash
sam deploy --guided
```

SAM will walk you through a series of prompts. Here's what to enter:

| Prompt | What to enter |
|---|---|
| Stack Name | `aws-credits-pipeline` (or press Enter to accept) |
| AWS Region | Your target region, e.g. `us-east-1` |
| Parameter CloudZeroApiKey | Paste your API key (text will be invisible — this is normal, see note below) |
| Parameter CloudZeroMetricName | Your unit metric stream name (e.g. `map-credit-estimates`) |
| Confirm changes before deploy | `y` |
| Allow SAM CLI IAM role creation | `y` |
| Save arguments to configuration file | `y` |
| SAM configuration file | Press Enter to accept `samconfig.toml` |
| SAM configuration environment | Press Enter to accept `default` |

> **Note: The API key prompt is invisible.** Because `CloudZeroApiKey` is a `NoEcho` parameter, SAM hides your input as you type — no characters will appear on screen. This is expected. Just paste your key and press Enter.

After the prompts, SAM will show you a changeset (what it's about to create) and ask:

```
Deploy this changeset? [y/N]
```

Enter `y` to proceed.

### What gets created

- An S3 bucket named `aws-credits-pipeline-<AccountId>-<Region>`
- A Lambda function named `aws-credits-processor`
- An IAM role with least-privilege permissions

### If you are doing subsequent deploys...

After the first deploy, you must pass credentials explicitly via `--parameter-overrides` (SAM won't re-prompt for `NoEcho` parameters):

```bash
sam deploy \
  --parameter-overrides "CloudZeroApiKey=$CLOUDZERO_API_KEY CloudZeroMetricName=$CLOUDZERO_METRIC_NAME"
```

This reads the values from your shell environment variables and passes them to CloudFormation.

---

## Step 4 — Upload a credits file

> **Loading multiple months at once?** Skip to [Backfill historical credits](#backfill-historical-credits) — the backfill script handles multi-month CSVs and calls the API directly (no S3 upload needed).

For ongoing monthly uploads, the filename must follow the pattern `credits-YYYY-MM.csv`. The billing month is read from the filename, not the upload date.

### Option A: Upload via the AWS Console (easiest)

1. Open the [AWS Console](https://console.aws.amazon.com) and go to **S3**
2. Find the bucket named `aws-credits-pipeline-<AccountId>-<Region>` (you can find this in the deploy output)
3. Click **Upload**
4. Click **Add files** and select your `credits-YYYY-MM.csv` file
5. Click **Upload** at the bottom of the page
6. Wait for the upload to complete — the Lambda triggers automatically within a few seconds

### Option B: Upload via the CLI

Replace `<AccountId>` and `<Region>` with your values:

```bash
aws s3 cp credits-2025-01.csv s3://aws-credits-pipeline-<AccountId>-<Region>/credits-2025-01.csv
```

The Lambda triggers automatically within a few seconds of the upload.

---

## Step 5 — Verify it worked

### Check CloudWatch Logs

1. Open the [AWS Console](https://console.aws.amazon.com)
2. Go to **CloudWatch → Log groups**
3. Find `/aws/lambda/aws-credits-processor`
4. Open the most recent log stream
5. Look for a structured JSON summary log with fields like `billing_month`, `row_count`, `total_credit_usd`

### Check CloudZero

Credits should appear in your Unit Metric in CloudZero for the uploaded billing month within a few minutes.

### Re-uploading the same file is safe

The pipeline uses the `/replace` endpoint — re-uploading the same month replaces the existing data rather than adding duplicates.

---

## Backfill historical credits

For the initial load of historical months, use the backfill script instead of uploading files to S3 one at a time. The script calls the CloudZero API directly (no Lambda involved).

The script auto-detects two CSV formats:

**Multi-column** — one column per month (best for backfills):
```
account_id, Jan amt, Feb amt, Mar amt, ...
123456789012,$ 50.00,$ 75.00,$ 100.00
234567890123,"$ 1,200.00","$ 1,500.00","$ 1,800.00"
345678901234,$ -,$ 25.00,$ 50.00
```

**Two-column** — single amount, posted the same for every month:
```
123456789012,$100.00
234567890123,"$1,500.00"
```

See `examples/credits-multimonth-example.csv` and `examples/credits-example.csv` for reference.

### Usage

```bash
# Dry run — validate without posting
python3 scripts/backfill.py \
  --csv your-credits-file.csv \
  --start-month 2025-01 \
  --end-month 2026-01 \
  --dry-run

# Real run — post all months
python3 scripts/backfill.py \
  --csv your-credits-file.csv \
  --start-month 2025-01 \
  --end-month 2026-01
```

Options:
- `--dry-run` — validate CSV and month range without calling the API
- `--delay <seconds>` — pause between API calls (default: 2 seconds)

The backfill and the S3 pipeline both use the `/replace` endpoint, so they don't conflict. Re-posting any month via either path safely replaces the data.

---

## Credits CSV format

No header row. Two columns:

| Column | Description | Example |
|---|---|---|
| 0 | AWS Account ID (12 digits, leading zeros matter) | `054736553085` |
| 1 | USD credit amount | `$0.00`, `"$6,407.96"`, `"$116,319.27"` |

Amounts can be positive or negative — the pipeline always treats them as credits (negative values in CloudZero). If an account ID appears more than once, the first row is used and duplicates are skipped with a warning in the logs.

---

## Running tests locally

```bash
pip install -r src/requirements.txt -r tests/requirements.txt
pytest tests/ --cov=src --cov-report=term-missing
```

---

## Local smoke test (optional — no deploy needed)

```bash
sam build
sam local invoke CreditsProcessorFunction -e events/s3_put_event.json \
  --env-vars '{"CreditsProcessorFunction": {"CLOUDZERO_API_KEY": "test", "CLOUDZERO_METRIC_NAME": "test"}}'
```

---

## Troubleshooting

### API key appears doubled in CloudFormation

If you previously ran `sam deploy --guided` and it wrote a `parameter_overrides` line with a literal `$CLOUDZERO_API_KEY` into `samconfig.toml`, SAM may pass the shell variable expansion concatenated with the value from the prompt. Fix: open `samconfig.toml` and remove any `parameter_overrides` line. Then redeploy with:

```bash
sam deploy \
  --parameter-overrides "CloudZeroApiKey=$CLOUDZERO_API_KEY CloudZeroMetricName=$CLOUDZERO_METRIC_NAME"
```

### "Unable to locate credentials" or SSO errors

If you use AWS SSO, make sure you're logged in:

```bash
aws sso login --profile your-sso-profile
export AWS_PROFILE=your-sso-profile
```

Or copy temporary credentials from the AWS Console (your username → "Command line or programmatic access").

### Nothing appears when typing the API key

This is expected. The `CloudZeroApiKey` parameter is `NoEcho`, which hides your input. Just paste your key and press Enter.

---

## Project structure

```
├── src/
│   ├── handler.py             # Lambda entry point
│   ├── csv_parser.py          # CSV parsing
│   ├── cloudzero_client.py    # CloudZero API client
│   ├── logger.py              # Structured JSON logging
│   └── requirements.txt
├── tests/
│   ├── test_csv_parser.py
│   ├── test_cloudzero_client.py
│   ├── test_handler.py
│   ├── test_backfill.py
│   └── requirements.txt
├── scripts/
│   └── backfill.py            # Historical backfill CLI
├── docs/
│   ├── architecture.md
│   └── changelog.md
├── events/
│   └── s3_put_event.json      # Sample event for sam local invoke
├── examples/
│   └── credits-example.csv    # Sample credits CSV
├── template.yaml              # SAM template
├── samconfig.toml             # SAM deploy config
└── .env.example               # Reference for required env vars
```
