import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import responses

from clients.base_client import SessionManager
from clients.pennsieve_client import PennsieveClient

API_HOST = "https://api.test.pennsieve.io"
API_HOST2 = "https://api2.test.pennsieve.io"


def _make_client():
    auth_provider = MagicMock()
    auth_provider.get_session_token.return_value = "test-token"
    sm = SessionManager(auth_provider)
    return PennsieveClient(sm, API_HOST, API_HOST2)


def _make_package(node_id, name, source_name, parent_id=None):
    return {
        "content": {
            "nodeId": node_id,
            "name": name,
            "parentId": parent_id,
        },
        "objects": {
            "source": [
                {"content": {"name": source_name, "size": 1234}}
            ]
        },
    }


class TestListDatasetPackages:
    @responses.activate
    def test_single_page(self):
        pkg = _make_package("N:package:1", "test", "test.lay")
        responses.add(
            responses.GET,
            f"{API_HOST}/datasets/D1/packages",
            json={"packages": [pkg], "cursor": None},
            status=200,
        )

        client = _make_client()
        result = client.list_dataset_packages("D1")

        assert len(result) == 1
        assert result[0]["content"]["nodeId"] == "N:package:1"
        assert responses.calls[0].request.headers["Authorization"] == "Bearer test-token"

    @responses.activate
    def test_paginated(self):
        pkg1 = _make_package("N:package:1", "file1", "file1.lay")
        pkg2 = _make_package("N:package:2", "file2", "file2.lay")

        responses.add(
            responses.GET,
            f"{API_HOST}/datasets/D1/packages",
            json={"packages": [pkg1], "cursor": "page2"},
            status=200,
        )
        responses.add(
            responses.GET,
            f"{API_HOST}/datasets/D1/packages",
            json={"packages": [pkg2], "cursor": None},
            status=200,
        )

        client = _make_client()
        result = client.list_dataset_packages("D1")

        assert len(result) == 2
        assert result[0]["content"]["nodeId"] == "N:package:1"
        assert result[1]["content"]["nodeId"] == "N:package:2"
        assert len(responses.calls) == 2


class TestGetPackageFiles:
    @responses.activate
    def test_returns_file_list(self):
        responses.add(
            responses.GET,
            f"{API_HOST}/packages/N:package:1/files",
            json=[{"content": {"id": "file-1", "name": "test.lay"}}],
            status=200,
        )

        client = _make_client()
        files = client.get_package_files("N:package:1")

        assert len(files) == 1
        assert files[0]["content"]["id"] == "file-1"


class TestGetFileDownloadUrl:
    @responses.activate
    def test_returns_presigned_url(self):
        responses.add(
            responses.GET,
            f"{API_HOST}/packages/N:package:1/files/file-1",
            json={"url": "https://s3.example.com/file"},
            status=200,
        )

        client = _make_client()
        url = client.get_file_download_url("N:package:1", "file-1")

        assert url == "https://s3.example.com/file"
        assert responses.calls[0].request.headers["Authorization"] == "Bearer test-token"


class TestDeletePackages:
    @responses.activate
    def test_sends_correct_payload_and_auth(self):
        responses.add(
            responses.POST,
            f"{API_HOST}/data/delete",
            json={"success": True},
            status=200,
        )

        client = _make_client()
        client.delete_packages(["N:package:1", "N:package:2"])

        req = responses.calls[0].request
        assert req.headers["Authorization"] == "Bearer test-token"
        body = json.loads(req.body)
        assert body == {"things": ["N:package:1", "N:package:2"]}


class TestUploadFile:
    @patch("clients.pennsieve_client.Pennsieve2")
    def test_upload_creates_manifest_and_uploads(self, mock_ps2_cls):
        mock_ps2 = MagicMock()
        mock_ps2_cls.return_value = mock_ps2

        # Mock manifest.list_files to return completed file
        mock_file = MagicMock()
        mock_file.status = 4  # Finalized
        mock_files_resp = MagicMock()
        mock_files_resp.files = [mock_file]
        mock_ps2.manifest.list_files.return_value = mock_files_resp

        with tempfile.NamedTemporaryFile(suffix=".lay", delete=False) as f:
            f.write(b"test content")
            tmp_path = f.name

        try:
            client = _make_client()
            client.upload_file("D1", tmp_path, folder_id="N:collection:folder1")

            mock_ps2.use_dataset.assert_called_once_with("D1")
            mock_ps2.manifest.create.assert_called_once()
            create_args = mock_ps2.manifest.create.call_args
            assert create_args[1]["target_base_path"] == "N:collection:folder1"
            mock_ps2.manifest.upload.assert_called_once()
        finally:
            os.unlink(tmp_path)

    @patch("clients.pennsieve_client.Pennsieve2")
    def test_upload_without_folder_id(self, mock_ps2_cls):
        mock_ps2 = MagicMock()
        mock_ps2_cls.return_value = mock_ps2

        mock_file = MagicMock()
        mock_file.status = 4  # Finalized
        mock_files_resp = MagicMock()
        mock_files_resp.files = [mock_file]
        mock_ps2.manifest.list_files.return_value = mock_files_resp

        with tempfile.NamedTemporaryFile(suffix=".lay", delete=False) as f:
            f.write(b"test content")
            tmp_path = f.name

        try:
            client = _make_client()
            client.upload_file("D1", tmp_path)

            create_args = mock_ps2.manifest.create.call_args
            assert create_args[1]["target_base_path"] == ""
        finally:
            os.unlink(tmp_path)


class TestGetPackage:
    @responses.activate
    def test_returns_package(self):
        responses.add(
            responses.GET,
            f"{API_HOST}/packages/N:package:1",
            json={"content": {"nodeId": "N:package:1", "name": "test"}},
            status=200,
        )

        client = _make_client()
        result = client.get_package("N:package:1")

        assert result["content"]["nodeId"] == "N:package:1"
        assert responses.calls[0].request.headers["Authorization"] == "Bearer test-token"


class TestRenamePackage:
    @responses.activate
    def test_sends_new_name(self):
        responses.add(
            responses.PUT,
            f"{API_HOST}/packages/N:package:1",
            json={"content": {"nodeId": "N:package:1", "name": "original-name"}},
            status=200,
        )

        client = _make_client()
        result = client.rename_package("N:package:1", "original-name")

        req = responses.calls[0].request
        assert req.headers["Authorization"] == "Bearer test-token"
        body = json.loads(req.body)
        assert body == {"name": "original-name"}
        assert result["content"]["name"] == "original-name"
