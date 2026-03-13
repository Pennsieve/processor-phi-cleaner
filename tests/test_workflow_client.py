import json

import responses
from unittest.mock import MagicMock

from clients.base_client import SessionManager
from clients.workflow_client import WorkflowClient

API_HOST2 = "https://api2.test.pennsieve.io"


def _make_client():
    auth_provider = MagicMock()
    auth_provider.get_session_token.return_value = "test-token"
    sm = SessionManager(auth_provider)
    return WorkflowClient(API_HOST2, sm)


class TestGetWorkflowInstance:
    @responses.activate
    def test_returns_dataset_id(self):
        responses.add(
            responses.GET,
            f"{API_HOST2}/compute/workflows/runs/wf-123",
            json={
                "uuid": "wf-123",
                "datasetId": "N:dataset:abc",
                "dataSources": {
                    "source1": {"packageIds": ["N:package:1", "N:package:2"]}
                },
            },
            status=200,
        )

        client = _make_client()
        instance = client.get_workflow_instance("wf-123")

        assert instance.id == "wf-123"
        assert instance.dataset_id == "N:dataset:abc"
        assert responses.calls[0].request.headers["Authorization"] == "Bearer test-token"

    @responses.activate
    def test_raises_on_http_error(self):
        responses.add(
            responses.GET,
            f"{API_HOST2}/compute/workflows/runs/bad-id",
            json={"error": "not found"},
            status=404,
        )

        client = _make_client()
        raised = False
        try:
            client.get_workflow_instance("bad-id")
        except Exception:
            raised = True

        assert raised
