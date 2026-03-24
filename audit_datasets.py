"""
Audit all datasets in a Pennsieve organization.

- Merges all .csv files per dataset into per-dataset CSVs + a combined CSV.
- Counts .lay files per dataset (one .lay = one EEG).
- Writes results incrementally per dataset so you don't have to wait for the full run.

Usage:
    python audit_datasets.py

Requires PENNSIEVE_API_KEY and PENNSIEVE_API_SECRET env vars (or a .env file).

Output (in output/audit/):
    <dataset_name>.csv          - merged CSV for that dataset
    <dataset_name>.txt          - EEG count + summary for that dataset
    merged_all.csv              - all dataset CSVs combined
    summary.txt                 - full EEG count summary across all datasets
"""

import argparse
import csv
import logging
import os
import re
import sys
import tempfile

import requests
from dotenv import load_dotenv

load_dotenv()

from clients.authentication_client import KeySecretAuthProvider
from clients.base_client import SessionManager
from clients.pennsieve_client import PennsieveClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger()

EXPECTED_HEADERS = ["new_name", "age_in_days_at_time_of_eeg", "eeg_start_time", "eeg_duration", "date_of_csv_creation"]
FIELDNAMES = EXPECTED_HEADERS + ["_source_dataset", "_source_file"]
OUT_DIR = os.path.join("output", "audit")


def sanitize_filename(name):
    """Turn a dataset name into a safe filename."""
    return re.sub(r'[^\w\-. ]+', '_', name).strip()


def list_org_datasets(api_host, auth_headers):
    """List all datasets the authenticated user can see."""
    resp = requests.get(f"{api_host}/datasets", headers=auth_headers, params={"pageSize": 1000})
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, list):
        return data
    return data.get("datasets", data.get("results", []))


def get_source_files(pkg):
    """Extract source file names from a package's API response."""
    sources = []
    for obj in pkg.get("objects", {}).get("source", []):
        name = obj.get("content", {}).get("name", "")
        if name:
            sources.append(name)
    return sources


def write_dataset_csv(ds_name, rows):
    """Write per-dataset CSV. Returns the path."""
    safe_name = sanitize_filename(ds_name)
    path = os.path.join(OUT_DIR, f"{safe_name}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return path


def write_dataset_summary(ds_name, lay_count, csv_count, csv_rows, skipped):
    """Write per-dataset text summary. Returns the path."""
    safe_name = sanitize_filename(ds_name)
    path = os.path.join(OUT_DIR, f"{safe_name}.txt")
    with open(path, "w") as f:
        f.write(f"Dataset: {ds_name}\n")
        f.write(f"EEGs (.lay files): {lay_count}\n")
        f.write(f"CSV files collected: {csv_count}\n")
        f.write(f"CSV rows collected: {len(csv_rows)}\n")
        if skipped:
            f.write(f"\nSkipped CSVs:\n")
            for fname, reason in skipped:
                f.write(f"  {fname}: {reason}\n")
    return path


def append_to_merged_csv(rows):
    """Append rows to the combined CSV, creating it with headers if needed."""
    path = os.path.join(OUT_DIR, "merged_all.csv")
    file_exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description="Audit Pennsieve datasets for .lay and .csv files.")
    parser.add_argument(
        "dataset_ids",
        nargs="*",
        help="Optional dataset IDs to audit (e.g. N:dataset:abc123). If omitted, audits all datasets.",
    )
    args = parser.parse_args()

    api_host = os.getenv("PENNSIEVE_API_HOST", "https://api.pennsieve.net")
    api_key = os.getenv("PENNSIEVE_API_KEY")
    api_secret = os.getenv("PENNSIEVE_API_SECRET")

    if not api_key or not api_secret:
        print("Error: PENNSIEVE_API_KEY and PENNSIEVE_API_SECRET must be set.", file=sys.stderr)
        sys.exit(1)

    # Authenticate
    auth_provider = KeySecretAuthProvider(api_host, api_key, api_secret)
    session_manager = SessionManager(auth_provider)
    client = PennsieveClient(session_manager, api_host, api_host.replace("api.", "api2."))

    auth_headers = {"Authorization": f"Bearer {session_manager.session_token}"}

    if args.dataset_ids:
        # Build minimal dataset entries from the provided IDs
        datasets = [{"content": {"id": ds_id, "name": ds_id}} for ds_id in args.dataset_ids]
        log.info(f"Auditing {len(datasets)} specified dataset(s)")
    else:
        log.info("Listing all datasets in organization...")
        datasets = list_org_datasets(api_host, auth_headers)
        log.info(f"Found {len(datasets)} datasets")

    os.makedirs(OUT_DIR, exist_ok=True)

    # Remove stale merged file from previous runs
    merged_path = os.path.join(OUT_DIR, "merged_all.csv")
    if os.path.exists(merged_path):
        os.remove(merged_path)

    eeg_counts = []  # (dataset_name, lay_count)

    for i, ds in enumerate(datasets, 1):
        content = ds.get("content", ds)
        ds_id = content.get("id", content.get("nodeId"))
        ds_name = content.get("name", "unknown")

        log.info(f"[{i}/{len(datasets)}] Processing dataset: {ds_name} ({ds_id})")

        try:
            packages = client.list_dataset_packages(ds_id, include_source_files=False)
        except requests.exceptions.HTTPError as e:
            log.warning(f"  Could not list packages for {ds_name}: {e}")
            continue

        # Diagnostic: count states to understand what the API returned
        state_counts = {}
        for pkg in packages:
            state = pkg.get("content", {}).get("state", "UNKNOWN").upper()
            state_counts[state] = state_counts.get(state, 0) + 1
        log.info(f"  {len(packages)} total packages, states: {state_counts}")

        seen_lay_names = set()
        seen_csv_names = set()
        csv_packages = []

        for pkg in packages:
            pkg_content = pkg.get("content", {})
            node_id = pkg_content.get("nodeId")
            pkg_name = pkg_content.get("name", "")
            pkg_type = pkg_content.get("packageType", "").lower()
            state = pkg_content.get("state", "").upper()

            # Skip folders and deleted packages
            if pkg_type == "collection":
                continue
            if state in ("DELETED", "DELETING"):
                continue
            if "__DELETED__" in pkg_name:
                continue

            # Without includeSourceFiles, determine type from package name or packageType
            _, ext = os.path.splitext(pkg_name)
            ext_lower = ext.lower()

            if ext_lower == ".lay" or pkg_type == "timeseries":
                seen_lay_names.add(pkg_name)
            elif ext_lower == ".csv" or pkg_type in ("csv", "tabular"):
                if pkg_name not in seen_csv_names:
                    seen_csv_names.add(pkg_name)
                    csv_packages.append((node_id, pkg_name))

        lay_count = len(seen_lay_names)
        log.info(f"  After filtering: {lay_count} unique .lay files, {len(csv_packages)} unique .csv files (from {len(packages)} total packages)")
        eeg_counts.append((ds_name, lay_count))

        # Download and collect CSV contents for this dataset
        ds_csv_rows = []
        ds_skipped = []
        csv_files_collected = 0

        for node_id, csv_name in csv_packages:
            try:
                pkg_files = client.get_package_files(node_id)
                source_file = None
                for pf in pkg_files:
                    pf_name = pf.get("content", {}).get("name", "") if isinstance(pf, dict) else ""
                    if pf_name == csv_name:
                        source_file = pf
                        break
                if not source_file and pkg_files:
                    source_file = pkg_files[0] if isinstance(pkg_files, list) else None

                if not source_file:
                    log.warning(f"  No source file found for {csv_name}, skipping")
                    continue

                file_id = source_file.get("content", {}).get("id")
                download_url = client.get_file_download_url(node_id, file_id)

                with tempfile.NamedTemporaryFile(mode="wb", suffix=".csv", delete=False) as tmp:
                    tmp_path = tmp.name
                    client.download_file(download_url, tmp_path)

                with open(tmp_path, "r", newline="", encoding="utf-8-sig") as f:
                    reader = csv.DictReader(f)
                    file_headers = set(reader.fieldnames or [])
                    if not set(EXPECTED_HEADERS).issubset(file_headers):
                        missing = set(EXPECTED_HEADERS) - file_headers
                        log.warning(f"  Skipping {csv_name}: missing columns {missing}")
                        ds_skipped.append((csv_name, f"missing columns: {missing}"))
                        os.unlink(tmp_path)
                        continue
                    rows = list(reader)

                os.unlink(tmp_path)

                if rows:
                    for row in rows:
                        row["_source_dataset"] = ds_name
                        row["_source_file"] = csv_name
                    ds_csv_rows.extend(rows)
                    csv_files_collected += 1

                log.info(f"  Collected {len(rows)} rows from {csv_name}")

            except Exception:
                log.exception(f"  Error downloading {csv_name}")

        # --- Write results for this dataset immediately ---

        if ds_csv_rows:
            csv_path = write_dataset_csv(ds_name, ds_csv_rows)
            append_to_merged_csv(ds_csv_rows)
            log.info(f"  Wrote {len(ds_csv_rows)} rows to {csv_path}")

        summary_path = write_dataset_summary(ds_name, lay_count, csv_files_collected, ds_csv_rows, ds_skipped)

        print(f"  {ds_name}: {lay_count} EEGs, {len(ds_csv_rows)} CSV rows -> {summary_path}")

    # --- Final summary ---
    summary_path = os.path.join(OUT_DIR, "summary.txt")
    total_eegs = 0
    with open(summary_path, "w") as f:
        f.write("EEG counts (.lay files per dataset)\n")
        f.write("=" * 50 + "\n")
        for ds_name, count in sorted(eeg_counts, key=lambda x: x[1], reverse=True):
            if count > 0:
                f.write(f"  {ds_name}: {count} EEGs\n")
                total_eegs += count
        zero_count = sum(1 for _, c in eeg_counts if c == 0)
        if zero_count:
            f.write(f"\n  ({zero_count} datasets with 0 .lay files)\n")
        f.write(f"\n  Total: {total_eegs} EEGs across {len(eeg_counts)} datasets\n")

    # Also print to stdout
    with open(summary_path) as f:
        print("\n" + f.read())

    log.info(f"All results written to {OUT_DIR}/")


if __name__ == "__main__":
    main()
