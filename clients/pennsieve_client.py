import logging
import os
import time

import requests
from pennsieve2 import Pennsieve as Pennsieve2

from clients.base_client import BaseClient

log = logging.getLogger()


class PennsieveClient(BaseClient):
    """Client for Pennsieve API endpoints needed by the PHI cleaner."""

    def __init__(self, session_manager, api_host, api_host2):
        super().__init__(session_manager)
        self.api_host = api_host
        self.api_host2 = api_host2

    def _auth_headers(self):
        return {"Authorization": f"Bearer {self.session_manager.session_token}"}

    @BaseClient.retry_with_refresh
    def list_dataset_packages(self, dataset_id):
        """List all packages in a dataset with source file info (cursor-paginated)."""
        packages = []
        cursor = None
        page = 0

        while True:
            params = {"pageSize": 100}
            if cursor:
                params["cursor"] = cursor

            resp = requests.get(
                f"{self.api_host}/datasets/{dataset_id}/packages",
                headers=self._auth_headers(),
                params=params,
            )
            resp.raise_for_status()
            data = resp.json()

            page_packages = data.get("packages", [])
            packages.extend(page_packages)
            page += 1
            log.info(f"    Page {page}: {len(page_packages)} packages (total so far: {len(packages)})")

            new_cursor = data.get("cursor")
            if not new_cursor:
                break
            if new_cursor == cursor:
                log.warning(f"    API returned same cursor on page {page}, stopping (cursor loop)")
                break
            cursor = new_cursor

        return packages

    @BaseClient.retry_with_refresh
    def get_package_files(self, package_node_id):
        """List files in a package."""
        resp = requests.get(
            f"{self.api_host}/packages/{package_node_id}/files",
            headers=self._auth_headers(),
        )
        resp.raise_for_status()
        return resp.json()

    @BaseClient.retry_with_refresh
    def get_file_download_url(self, package_node_id, file_id):
        """Get presigned download URL for a specific file in a package."""
        resp = requests.get(
            f"{self.api_host}/packages/{package_node_id}/files/{file_id}",
            headers=self._auth_headers(),
        )
        resp.raise_for_status()
        data = resp.json()

        if "url" in data:
            return data["url"]

        raise ValueError(f"No download URL returned for {package_node_id}/{file_id}")

    def download_file(self, url, dest_path):
        """Download a file from a presigned URL (no auth needed)."""
        resp = requests.get(url, stream=True)
        resp.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

    @BaseClient.retry_with_refresh
    def delete_packages(self, node_ids):
        """Delete packages by node IDs."""
        resp = requests.post(
            f"{self.api_host}/data/delete",
            headers=self._auth_headers(),
            json={"things": node_ids},
        )
        resp.raise_for_status()
        return resp.json()

    def upload_file(self, dataset_id, file_path, folder_id=None, verbose=False):
        """Upload a file to a dataset via the Pennsieve Agent.

        Uses the pennsieve2 client which communicates with the agent over gRPC.
        The agent handles manifest creation, S3 upload, and package registration.
        """
        if verbose:
            log.info(f"Connecting to Pennsieve Agent at localhost:9000...")
        ps = Pennsieve2(target="localhost:9000")
        if verbose:
            log.info(f"Connected. Selecting dataset {dataset_id}...")
        ps.use_dataset(dataset_id)

        # target_base_path places the file inside the folder on Pennsieve
        target_path = ""
        if folder_id:
            target_path = folder_id

        if verbose:
            log.info(f"Creating manifest for {file_path} (target_path={target_path!r})...")
        ps.manifest.create(
            os.path.abspath(file_path),
            target_base_path=target_path,
        )
        if verbose:
            log.info("Manifest created. Starting upload...")
        ps.manifest.upload()

        # Wait for upload to complete by subscribing to status updates
        manifest_id = ps.manifest.manifest.id
        if verbose:
            log.info(f"Upload started (manifest {manifest_id}), waiting for completion...")

        max_wait = 300  # 5 minutes
        poll_interval = 5
        elapsed = 0
        while elapsed < max_wait:
            time.sleep(poll_interval)
            elapsed += poll_interval
            files_resp = ps.manifest.list_files()
            files = list(files_resp.file)
            if not files:
                if verbose:
                    log.info(f"  [{elapsed}s] No files in manifest yet...")
                continue
            # Proto status enum: FINALIZED=3, VERIFIED=4, FAILED=5, UPLOADED=9
            if verbose:
                statuses = [(f.source_path, f.status) for f in files]
                log.info(f"  [{elapsed}s] File statuses: {statuses}")
            all_done = all(f.status in (3, 4, 9) for f in files)  # Finalized, Verified, Uploaded
            if all_done:
                return
            any_failed = any(f.status == 5 for f in files)  # Failed=5
            if any_failed:
                raise RuntimeError(f"Upload failed for manifest {manifest_id}")

        raise RuntimeError(f"Upload timed out after {max_wait}s for manifest {manifest_id}")

    @BaseClient.retry_with_refresh
    def get_package(self, package_id):
        """Get a package by node ID. Used to confirm a package exists."""
        resp = requests.get(
            f"{self.api_host}/packages/{package_id}",
            headers=self._auth_headers(),
        )
        resp.raise_for_status()
        return resp.json()

    @BaseClient.retry_with_refresh
    def rename_package(self, package_id, new_name):
        """Rename a package."""
        resp = requests.put(
            f"{self.api_host}/packages/{package_id}",
            headers=self._auth_headers(),
            json={"name": new_name},
        )
        resp.raise_for_status()
        return resp.json()
