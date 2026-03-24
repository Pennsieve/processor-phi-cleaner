"""
Pennsieve Processor: processor-phi-clean

Scans a dataset for files matching FILE_EXTENSIONS, downloads each,
and reports restricted words found only in [Comments] sections.
"""

import csv
import logging
import os
import re
import tempfile
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

from cleaners.lay_cleaner import LayCleaner
from clients.authentication_client import KeySecretAuthProvider, TokenAuthProvider
from clients.base_client import SessionManager
from clients.pennsieve_client import PennsieveClient
from clients.workflow_client import WorkflowClient
from config import Config

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger()

# Cleaner registry — add entries here for future file types
CLEANERS = {
    ".lay": LayCleaner(),
}


def get_source_files(pkg):
    """Extract source file names from a package's API response."""
    sources = []
    for obj in pkg.get("objects", {}).get("source", []):
        name = obj.get("content", {}).get("name", "")
        if name:
            sources.append(name)
    return sources


def main():
    config = Config()
    verbose = config.VERBOSE
    process_mode = config.PROCESS_MODE

    # Initialize auth
    if config.SESSION_TOKEN:
        auth_provider = TokenAuthProvider(config.API_HOST, config.SESSION_TOKEN, config.REFRESH_TOKEN)
    elif config.API_KEY and config.API_SECRET:
        auth_provider = KeySecretAuthProvider(config.API_HOST, config.API_KEY, config.API_SECRET)
    else:
        raise RuntimeError("no authentication credentials provided: set SESSION_TOKEN or API_KEY/API_SECRET")

    session_manager = SessionManager(auth_provider)
    client = PennsieveClient(session_manager, config.API_HOST, config.API_HOST2)

    # Resolve dataset ID: direct config (local dev) or workflow service (prod)
    if config.DATASET_ID:
        dataset_id = config.DATASET_ID
        if verbose:
            log.info(f"Using DATASET_ID from config: {dataset_id}")
    elif config.WORKFLOW_INSTANCE_ID:
        if verbose:
            log.info(f"Resolving dataset ID from workflow instance {config.WORKFLOW_INSTANCE_ID}")
        workflow_client = WorkflowClient(config.API_HOST2, session_manager)
        workflow = workflow_client.get_workflow_instance(config.WORKFLOW_INSTANCE_ID)
        dataset_id = workflow.dataset_id
        if verbose:
            log.info(f"Resolved dataset ID: {dataset_id}")
    else:
        raise RuntimeError("DATASET_ID or INTEGRATION_ID is required")

    dataset_name = config.DATASET_NAME or dataset_id
    try:
        dataset = client.get_dataset(dataset_id)
        content = dataset.get("content", {}) if isinstance(dataset, dict) else {}
        resolved_name = content.get("name") or (dataset.get("name") if isinstance(dataset, dict) else None)
        if isinstance(resolved_name, str) and resolved_name.strip():
            dataset_name = resolved_name
    except Exception:
        if verbose:
            log.info("Could not resolve dataset name from API; using DATASET_NAME or dataset ID")

    log.info(f"Starting restricted-word scan for dataset {dataset_id} ({dataset_name})")
    log.info(f"Process mode: {process_mode}")

    safe_dataset_name = re.sub(r"[^A-Za-z0-9._-]+", "_", dataset_name).strip("_")
    if not safe_dataset_name:
        safe_dataset_name = dataset_id
    if verbose:
        log.info(f"File extensions: {config.FILE_EXTENSIONS}")
        log.info(f"Restricted words: {config.RESTRICTED_WORDS}")

    # List all packages (without source files to avoid pagination cursor bugs)
    packages = client.list_dataset_packages(dataset_id, include_source_files=False)
    if verbose:
        log.info(f"Found {len(packages)} total packages in dataset")

    folder_names = {}
    if process_mode == "clean":
        for pkg in packages:
            c = pkg.get("content", {})
            if c.get("packageType", "").lower() == "collection":
                numeric_id = c.get("id")
                if numeric_id:
                    folder_names[numeric_id] = c.get("name", "")

    stats = {"found": 0, "matched": 0, "cleaned": 0, "skipped": 0, "errors": 0}

    # Run log CSV
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    run_log_path = os.path.join(config.OUTPUT_DIR, f"run_log_{run_timestamp}.csv")
    run_log_fields = ["dataset_id", "file_name", "node_id", "status", "detail", "matched_words", "match_count"]
    run_log_file = open(run_log_path, "w", newline="", encoding="utf-8")
    run_log_writer = csv.DictWriter(run_log_file, fieldnames=run_log_fields)
    run_log_writer.writeheader()

    def log_result(file_name, node_id, status, detail="", matched_words="", match_count=0):
        run_log_writer.writerow({
            "dataset_id": dataset_id,
            "file_name": file_name,
            "node_id": node_id,
            "status": status,
            "detail": detail,
            "matched_words": matched_words,
            "match_count": match_count,
        })
        run_log_file.flush()

    report_path = os.path.join(
        config.OUTPUT_DIR,
        f"restricted_word_report_{safe_dataset_name}_{run_timestamp}.csv",
    )
    report_fields = ["dataset_name", "dataset_id", "file_name", "node_id", "matched_words", "match_count"]
    report_file = open(report_path, "w", newline="", encoding="utf-8")
    report_writer = csv.DictWriter(report_file, fieldnames=report_fields)
    report_writer.writeheader()

    def log_match_report(file_name, node_id, matched_words, match_count):
        report_writer.writerow({
            "dataset_name": dataset_name,
            "dataset_id": dataset_id,
            "file_name": file_name,
            "node_id": node_id,
            "matched_words": matched_words,
            "match_count": match_count,
        })
        report_file.flush()

    for pkg in packages:
        content = pkg.get("content", {})
        node_id = content.get("nodeId")
        parent_id = content.get("parentId")
        state = content.get("state", "").upper()
        package_type = content.get("packageType", content.get("type", "unknown"))
        pkg_name = content.get("name", "unknown")

        if state in ("DELETED", "DELETING"):
            if verbose:
                log.info(f"    Skipping {pkg_name}: state is {state}")
            continue

        if "__DELETED__" in pkg_name:
            if verbose:
                log.info(f"    Skipping {pkg_name}: __DELETED__ in name")
            continue

        # Fast-path skip: if package name has an extension and it's not one we want,
        # skip before fetching package files.
        _, pkg_ext = os.path.splitext(pkg_name)
        if pkg_ext and pkg_ext.lower() not in config.FILE_EXTENSIONS:
            if verbose:
                log.info(f"    Skipping {pkg_name}: package extension {pkg_ext} is not targeted")
            continue

        if verbose:
            log.info(f"  Package: {pkg_name} (packageType={package_type}, state={state}, nodeId={node_id}, parentId={parent_id})")

        matching_name = pkg_name

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                # Get source file ID, then download via presigned URL
                pkg_files = client.get_package_files(node_id)
                source_file = None
                matching_ext = None
                for pf in pkg_files:
                    pf_name = pf.get("content", {}).get("name", "") if isinstance(pf, dict) else ""
                    _, ext = os.path.splitext(pf_name)
                    file_ext = ext.lower() if ext else None
                    if file_ext in config.FILE_EXTENSIONS:
                        source_file = pf
                        matching_ext = file_ext
                        matching_name = pf_name  # use actual source file name
                        break

                if not source_file:
                    if verbose:
                        log.info(f"    No matching extension in package files, skipping")
                    stats["skipped"] += 1
                    continue

                stats["found"] += 1
                cleaner = CLEANERS.get(matching_ext)
                if not cleaner:
                    log.warning(f"No cleaner registered for {matching_ext}, skipping {matching_name}")
                    log_result(matching_name, node_id, "skipped", f"no cleaner for {matching_ext}")
                    stats["skipped"] += 1
                    continue

                file_id = source_file.get("content", {}).get("id")
                download_url = client.get_file_download_url(node_id, file_id)
                dest_path = os.path.join(tmpdir, matching_name)
                if verbose:
                    log.info(f"Downloading {matching_name}...")
                client.download_file(download_url, dest_path)

                # Scan only in [Comments] section (no file modifications)
                matches = cleaner.find_restricted_words_in_comments(dest_path, config.RESTRICTED_WORDS)

                if not matches:
                    log.info(f"No restricted words found in [Comments] for {matching_name}")
                    log_result(matching_name, node_id, "skipped", "no restricted words in [Comments]")
                    stats["skipped"] += 1
                    continue

                unique_words = sorted({w for m in matches for w in m["words"]})

                log.info(
                    f"Found {len(matches)} restricted comment line(s) in {matching_name} "
                    f"(words: {', '.join(unique_words)})"
                )
                log_result(
                    matching_name,
                    node_id,
                    "matched",
                    "restricted words found in [Comments]",
                    matched_words=",".join(unique_words),
                    match_count=len(matches),
                )
                log_match_report(matching_name, node_id, ",".join(unique_words), len(matches))
                stats["matched"] += 1

                if process_mode == "clean":
                    modified = cleaner.clean(dest_path, config.RESTRICTED_WORDS)
                    if not modified:
                        log.warning(f"Expected matches but file was not modified: {matching_name}")
                        log_result(matching_name, node_id, "error", "matches found but clean reported no changes")
                        stats["errors"] += 1
                        continue

                    folder_path = folder_names.get(parent_id) if parent_id and parent_id != dataset_id else None
                    if verbose:
                        log.info(f"Uploading cleaned {matching_name}...")
                    client.upload_file(dataset_id, dest_path, folder_id=folder_path, verbose=verbose)

                    if verbose:
                        log.info(f"Upload confirmed. Deleting old package {node_id}...")
                    client.delete_packages([node_id])

                    log.info(f"Cleaned and replaced {matching_name}")
                    log_result(matching_name, node_id, "cleaned", "restricted words removed and replaced")
                    stats["cleaned"] += 1

        except Exception as e:
            log.exception(f"Error processing {matching_name}")
            log_result(matching_name, node_id, "error", str(e))
            stats["errors"] += 1

    run_log_file.close()
    report_file.close()

    log.info(
        f"Restricted-word scan complete: "
        f"{stats['found']} found, "
        f"{stats['matched']} matched, "
        f"{stats['cleaned']} cleaned, "
        f"{stats['skipped']} skipped, "
        f"{stats['errors']} errors"
    )
    log.info(f"Run log written to {run_log_path}")
    log.info(f"Match report written to {report_path}")


if __name__ == "__main__":
    main()
