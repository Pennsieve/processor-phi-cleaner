import os
from unittest.mock import MagicMock, patch

from process import get_source_files, main


def _make_package(node_id, source_name, parent_id=None):
    return {
        "content": {
            "nodeId": node_id,
            "name": source_name.rsplit(".", 1)[0],
            "parentId": parent_id,
        },
        "objects": {
            "source": [
                {"content": {"name": source_name, "size": 100}}
            ]
        },
    }


def _setup_client_for_clean(mock_client, lay_content):
    """Configure mock client for a successful clean+replace flow."""
    mock_client.get_package_files.return_value = [
        {"content": {"id": "file-1", "name": "test.lay"}},
    ]
    mock_client.get_file_download_url.return_value = "https://s3.example.com/file"

    def fake_download(url, dest_path):
        with open(dest_path, "w") as f:
            f.write(lay_content)

    mock_client.download_file.side_effect = fake_download
    mock_client.delete_packages.return_value = {"success": True}
    mock_client.upload_file.return_value = None


def _env(**overrides):
    base = {
        "DATASET_ID": "D1",
        "DATASET_NAME": "Dataset One",
        "INTEGRATION_ID": "",
        "FILE_EXTENSIONS": ".lay",
        "RESTRICTED_WORDS": "MRN,DOB",
        "PROCESS_MODE": "report",
        "PENNSIEVE_API_KEY": "key",
        "PENNSIEVE_API_SECRET": "secret",
        "PENNSIEVE_API_HOST": "https://api.test",
        "PENNSIEVE_API_HOST2": "https://api2.test",
    }
    base.update(overrides)
    return base


class TestGetSourceFiles:
    def test_extracts_source_names(self):
        pkg = _make_package("N:pkg:1", "file.lay")
        assert get_source_files(pkg) == ["file.lay"]

    def test_empty_objects(self):
        pkg = {"content": {"nodeId": "N:pkg:1"}, "objects": {}}
        assert get_source_files(pkg) == []

    def test_missing_objects(self):
        pkg = {"content": {"nodeId": "N:pkg:1"}}
        assert get_source_files(pkg) == []


class TestMain:
    @patch("process.PennsieveClient")
    @patch("process.KeySecretAuthProvider")
    def test_reports_matching_package_without_modifying_remote(self, mock_auth_cls, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.list_dataset_packages.return_value = [
            _make_package("N:pkg:1", "test.lay", parent_id="N:collection:folder1"),
        ]

        example_lay = os.path.join(os.path.dirname(__file__), "..", "example_lay", "ex2.lay")
        with open(example_lay, "r") as f:
            lay_content = f.read()

        _setup_client_for_clean(mock_client, lay_content)

        with patch.dict(os.environ, _env(), clear=False):
            os.environ.pop("SESSION_TOKEN", None)
            main()

        mock_client.delete_packages.assert_not_called()
        mock_client.upload_file.assert_not_called()

    @patch("process.PennsieveClient")
    @patch("process.KeySecretAuthProvider")
    def test_clean_mode_replaces_matching_package(self, mock_auth_cls, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.list_dataset_packages.return_value = [
            _make_package("N:pkg:1", "test.lay", parent_id="N:collection:folder1"),
        ]

        example_lay = os.path.join(os.path.dirname(__file__), "..", "example_lay", "ex2.lay")
        with open(example_lay, "r") as f:
            lay_content = f.read()

        _setup_client_for_clean(mock_client, lay_content)

        with patch.dict(os.environ, _env(PROCESS_MODE="clean"), clear=False):
            os.environ.pop("SESSION_TOKEN", None)
            main()

        mock_client.upload_file.assert_called_once()
        mock_client.delete_packages.assert_called_once_with(["N:pkg:1"])

    @patch("process.PennsieveClient")
    @patch("process.KeySecretAuthProvider")
    def test_skips_non_matching_extensions(self, mock_auth_cls, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.list_dataset_packages.return_value = [
            _make_package("N:pkg:1", "data.csv"),
        ]

        with patch.dict(os.environ, _env(), clear=False):
            os.environ.pop("SESSION_TOKEN", None)
            main()

        mock_client.get_file_download_url.assert_not_called()
        mock_client.download_file.assert_not_called()

    @patch("process.PennsieveClient")
    @patch("process.KeySecretAuthProvider")
    def test_skips_clean_file(self, mock_auth_cls, mock_client_cls):
        """File with no PHI should not be deleted or re-uploaded."""
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.list_dataset_packages.return_value = [
            _make_package("N:pkg:1", "clean.lay"),
        ]
        mock_client.get_package_files.return_value = [
            {"content": {"id": "file-1", "name": "clean.lay"}},
        ]
        mock_client.get_file_download_url.return_value = "https://s3.example.com/file"

        def fake_download(url, dest_path):
            with open(dest_path, "w") as f:
                f.write("[Comments]\n0.000,0.000,0,131072,Normal EEG\n")

        mock_client.download_file.side_effect = fake_download

        with patch.dict(os.environ, _env(), clear=False):
            os.environ.pop("SESSION_TOKEN", None)
            main()

        mock_client.upload_file.assert_not_called()
        mock_client.delete_packages.assert_not_called()

    @patch("process.PennsieveClient")
    @patch("process.KeySecretAuthProvider")
    def test_error_in_one_package_does_not_abort(self, mock_auth_cls, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.list_dataset_packages.return_value = [
            _make_package("N:pkg:1", "bad.lay"),
            _make_package("N:pkg:2", "good.lay"),
        ]
        mock_client.get_package_files.side_effect = [
            RuntimeError("download failed"),
            [{"content": {"id": "file-2", "name": "good.lay"}}],
        ]
        mock_client.get_file_download_url.return_value = "https://s3.example.com/file2"

        def fake_download(url, dest_path):
            with open(dest_path, "w") as f:
                f.write("[Comments]\n0.000,0.000,0,131072,Has MRN 123\n")

        mock_client.download_file.side_effect = fake_download
        with patch.dict(os.environ, _env(RESTRICTED_WORDS="MRN"), clear=False):
            os.environ.pop("SESSION_TOKEN", None)
            main()

        # Second package should still be processed despite first failing
        assert mock_client.get_file_download_url.call_count == 1

    @patch("process.PennsieveClient")
    @patch("process.KeySecretAuthProvider")
    def test_root_package_still_reports_without_remote_modifications(self, mock_auth_cls, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.list_dataset_packages.return_value = [
            _make_package("N:pkg:1", "root.lay", parent_id="D1"),
        ]

        _setup_client_for_clean(
            mock_client,
            "[Comments]\n0.000,0.000,0,131072,Patient MRN 555\n",
        )

        with patch.dict(os.environ, _env(RESTRICTED_WORDS="MRN"), clear=False):
            os.environ.pop("SESSION_TOKEN", None)
            main()

        mock_client.upload_file.assert_not_called()
        mock_client.delete_packages.assert_not_called()

    @patch("process.PennsieveClient")
    @patch("process.KeySecretAuthProvider")
    def test_no_delete_or_upload_when_matches_found(self, mock_auth_cls, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.list_dataset_packages.return_value = [
            _make_package("N:pkg:1", "test.lay", parent_id="N:collection:f1"),
        ]

        _setup_client_for_clean(
            mock_client,
            "[Comments]\n0.000,0.000,0,131072,MRN 999\n",
        )

        with patch.dict(os.environ, _env(RESTRICTED_WORDS="MRN"), clear=False):
            os.environ.pop("SESSION_TOKEN", None)
            main()

        mock_client.delete_packages.assert_not_called()
        mock_client.upload_file.assert_not_called()

    @patch("process.WorkflowClient")
    @patch("process.PennsieveClient")
    @patch("process.KeySecretAuthProvider")
    def test_resolves_dataset_id_from_workflow_service(self, mock_auth_cls, mock_client_cls, mock_wf_cls):
        """When DATASET_ID is not set, resolve it from the workflow service via INTEGRATION_ID."""
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.list_dataset_packages.return_value = []

        mock_wf_client = MagicMock()
        mock_wf_cls.return_value = mock_wf_client
        mock_wf_instance = MagicMock()
        mock_wf_instance.dataset_id = "N:dataset:resolved"
        mock_wf_client.get_workflow_instance.return_value = mock_wf_instance

        env = _env(DATASET_ID="", INTEGRATION_ID="wf-run-123")
        with patch.dict(os.environ, env, clear=False):
            os.environ.pop("SESSION_TOKEN", None)
            main()

        mock_wf_client.get_workflow_instance.assert_called_once_with("wf-run-123")
        mock_client.list_dataset_packages.assert_called_once_with("N:dataset:resolved", include_source_files=False)

    @patch("process.PennsieveClient")
    @patch("process.KeySecretAuthProvider")
    def test_raises_when_no_dataset_id_or_integration_id(self, mock_auth_cls, mock_client_cls):
        env = _env(DATASET_ID="")
        with patch.dict(os.environ, env, clear=False):
            os.environ.pop("SESSION_TOKEN", None)
            os.environ.pop("INTEGRATION_ID", None)
            raised = False
            try:
                main()
            except RuntimeError as e:
                raised = True
                assert "DATASET_ID or INTEGRATION_ID" in str(e)
            assert raised
