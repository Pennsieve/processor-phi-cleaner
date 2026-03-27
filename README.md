# processor-phi-clean

Scans Pennsieve datasets for restricted words (potential PHI) in `.lay` file `[Comments]` sections. Can run in **report** mode (scan only) or **clean** mode (scan, strip, and re-upload).

## How It Works

The processor downloads `.lay` files from a Pennsieve dataset, parses the `[Comments]` section, and checks each comment line's text field for restricted words using case-insensitive whole-word matching.

- **Report mode** (`PROCESS_MODE=report`): Logs matches to CSV reports in `output/`. No files are modified.
- **Clean mode** (`PROCESS_MODE=clean`): Removes entire comment lines containing restricted words, re-uploads the cleaned file, and deletes the original package.

> **Note:** Cleaning removes the entire comment line, not just the restricted word. This is intentional — partial redaction could leave identifiable context.

## Setup

1. Python 3.11+
2. Docker (for `make run` / clean mode — the Pennsieve agent runs inside the container)
3. Copy the example env file and fill in your credentials:

```bash
cp .env.example .env
```

Then edit `.env` with your Pennsieve API key, secret, and target dataset ID. The `.env` file is gitignored and will not be committed.

## Makefile Targets

| Target | Command | Description |
|--------|---------|-------------|
| `make run` | `docker compose down && build && up` | Runs the processor in Docker using settings from `.env`. Mode depends on `PROCESS_MODE` in `.env`. Starts the Pennsieve agent automatically (required for clean mode uploads). |
| `make report` | `PROCESS_MODE=report python process.py` | Runs a scan locally (no Docker) in report-only mode. Outputs CSV reports to `output/`. |
| `make test` | `python -m pytest tests/ -v` | Runs the test suite. |
| `make clean-local` | `python clean_local.py example_lay/ex1.lay` | Dry-run clean on a local `.lay` file. Shows what would be removed. No API calls. |

### When to use which

- **`make report`** — quick local scan, no Docker needed. Good for checking a dataset before committing to a clean. Forces `PROCESS_MODE=report` regardless of what's in `.env`.
- **`make run`** — use this for both report and clean mode against a real dataset. The mode is controlled by `PROCESS_MODE` in your `.env` file — set it to `report` to scan only, or `clean` to strip PHI and re-upload. Docker is required for clean mode because the Pennsieve agent (needed for file uploads) runs inside the container.
- **`make clean-local`** — test cleaning logic on a local file without any API calls.

## Configuration

All configuration is via environment variables (or `.env` file for local/Docker runs).

### Required

| Variable | Description |
|----------|-------------|
| `PENNSIEVE_API_KEY` | API key for authentication |
| `PENNSIEVE_API_SECRET` | API secret for authentication |
| `DATASET_ID` | Target dataset ID (e.g., `N:dataset:abc123`) |

Or, when running via the Pennsieve workflow system:

| Variable | Description |
|----------|-------------|
| `SESSION_TOKEN` | Session token (used instead of API key/secret) |
| `REFRESH_TOKEN` | Refresh token (paired with session token) |
| `INTEGRATION_ID` | Workflow instance ID (resolves to a dataset ID) |

### Optional

| Variable | Default | Description |
|----------|---------|-------------|
| `PROCESS_MODE` | `report` | `report` (scan only) or `clean` (scan + strip + re-upload) |
| `FILE_EXTENSIONS` | `.lay` | Comma-separated list of file extensions to scan |
| `RESTRICTED_WORDS` | `MRN,DOB` | Comma-separated list of words to flag as PHI (see below) |
| `VERBOSE` | `false` | Set to `true`, `1`, or `yes` for detailed logging |
| `PENNSIEVE_API_HOST` | `https://api.pennsieve.net` | API host |
| `PENNSIEVE_API_HOST2` | `https://api2.pennsieve.net` | API v2 host |
| `OUTPUT_DIR` | `./output` | Directory for report CSVs and run logs |
| `DATASET_NAME` | *(resolved from API)* | Override display name for the dataset in reports |

### RESTRICTED_WORDS

A comma-separated list of words or phrases to flag as potential PHI. Matching is **case-insensitive** and uses **whole-word boundaries**, so `MRN` matches "MRN: 12345" but not "WMRN" or "MRNs".

Multi-word phrases work too — `date of birth` will match "date of birth" as a substring with word boundaries on each end.

Example:

```
RESTRICTED_WORDS=name,last name,first name,mrn,dob,d.o.b,date of birth,patient id
```

## Other Scripts

### `audit_datasets.py`

Audits all datasets in a Pennsieve organization (or specific datasets by ID). Counts `.lay` files per dataset and collects/merges `.csv` files. Report-only — no files are modified.

```bash
# Audit all datasets in the org
python audit_datasets.py

# Audit specific datasets
python audit_datasets.py N:dataset:abc123 N:dataset:def456
```

Output goes to `output/audit/`.

### `clean_local.py`

Clean a local `.lay` file without any API calls. Useful for testing.

```bash
# Dry-run (works on a copy, original untouched)
python clean_local.py path/to/file.lay

# Custom restricted words
python clean_local.py path/to/file.lay --words MRN,DOB,SSN

# Modify in place
python clean_local.py path/to/file.lay --in-place
```

## Output

Each run of `process.py` produces two CSV files in `output/`:

- **`run_log_<timestamp>.csv`** — full run log with status for every package processed (matched, skipped, cleaned, error)
- **`restricted_word_report_<dataset>_<timestamp>.csv`** — only files that had restricted word matches, with the matched words and count
