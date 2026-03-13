"""
Pennsieve Processor: processor-phi-clean

Scans a dataset for files matching FILE_EXTENSIONS, downloads each,
runs the appropriate cleaner to remove PHI, then replaces the original
on Pennsieve with the cleaned version.
"""

import logging
import os
import tempfile

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

    log.info(f"Starting PHI cleaner for dataset {dataset_id}")
    if verbose:
        log.info(f"File extensions: {config.FILE_EXTENSIONS}")
        log.info(f"Restricted words: {config.RESTRICTED_WORDS}")

    # List all packages
    packages = client.list_dataset_packages(dataset_id)
    if verbose:
        log.info(f"Found {len(packages)} total packages in dataset")

    stats = {"found": 0, "cleaned": 0, "skipped": 0, "errors": 0}

    for pkg in packages:
        content = pkg.get("content", {})
        node_id = content.get("nodeId")
        parent_id = content.get("parentId")
        state = content.get("state", "").upper()
        package_type = content.get("packageType", content.get("type", "unknown"))
        pkg_name = content.get("name", "unknown")

        if verbose:
            log.info(f"  Package: {pkg_name} (packageType={package_type}, state={state}, nodeId={node_id}, parentId={parent_id})")

        if state in ("DELETED", "DELETING"):
            if verbose:
                log.info(f"    Skipping {pkg_name}: state is {state}")
            continue

        # Find a source file matching our extensions
        source_names = get_source_files(pkg)
        if verbose:
            log.info(f"    Source files: {source_names}")

        matching_name = None
        matching_ext = None
        for name in source_names:
            _, ext = os.path.splitext(name)
            if ext.lower() in config.FILE_EXTENSIONS:
                matching_name = name
                matching_ext = ext.lower()
                break

        if not matching_name:
            if verbose:
                log.info(f"    No matching extension, skipping")
            continue

        stats["found"] += 1
        cleaner = CLEANERS.get(matching_ext)
        if not cleaner:
            log.warning(f"No cleaner registered for {matching_ext}, skipping {matching_name}")
            stats["skipped"] += 1
            continue

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                dest_path = os.path.join(tmpdir, matching_name)

                # Get source file ID, then download via presigned URL
                pkg_files = client.get_package_files(node_id)
                source_file = None
                for pf in pkg_files:
                    pf_name = pf.get("content", {}).get("name", "") if isinstance(pf, dict) else ""
                    if pf_name == matching_name:
                        source_file = pf
                        break
                if not source_file and pkg_files:
                    source_file = pkg_files[0] if isinstance(pkg_files, list) else None

                if not source_file:
                    log.warning(f"No source file found for {matching_name}, skipping")
                    stats["skipped"] += 1
                    continue

                file_id = source_file.get("content", {}).get("id")
                download_url = client.get_file_download_url(node_id, file_id)
                if verbose:
                    log.info(f"Downloading {matching_name}...")
                client.download_file(download_url, dest_path)

                # Clean
                modified = cleaner.clean(dest_path, config.RESTRICTED_WORDS)

                if not modified:
                    log.info(f"No PHI found in {matching_name}")
                    stats["skipped"] += 1
                    continue

                log.info(f"Found PHI in {matching_name}")

                # Upload cleaned file first, THEN delete old package.
                # This ensures we never lose data — the original stays
                # until the replacement is confirmed uploaded.
                folder_id = parent_id if parent_id != dataset_id else None

                if verbose:
                    log.info(f"Uploading cleaned {matching_name}...")
                client.upload_file(dataset_id, dest_path, folder_id=folder_id, verbose=verbose)

                if verbose:
                    log.info(f"Upload confirmed. Deleting old package {node_id}...")
                client.delete_packages([node_id])

                log.info(f"Cleaned and replaced {matching_name}")
                stats["cleaned"] += 1

        except Exception:
            log.exception(f"Error processing {matching_name}")
            stats["errors"] += 1

    log.info(
        f"PHI cleaner complete: "
        f"{stats['found']} found, "
        f"{stats['cleaned']} cleaned, "
        f"{stats['skipped']} skipped, "
        f"{stats['errors']} errors"
    )


if __name__ == "__main__":
    main()
