import json
import logging

import requests

from clients.base_client import BaseClient

log = logging.getLogger()


class WorkflowInstance:
    def __init__(self, id, dataset_id):
        self.id = id
        self.dataset_id = dataset_id


class WorkflowClient(BaseClient):
    """Resolves workflow context from the Pennsieve compute API."""

    def __init__(self, api_host, session_manager):
        super().__init__(session_manager)
        self.api_host = api_host

    @BaseClient.retry_with_refresh
    def get_workflow_instance(self, workflow_instance_id):
        """Fetch workflow instance to get dataset_id and other context."""
        url = f"{self.api_host}/compute/workflows/runs/{workflow_instance_id}"
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.session_manager.session_token}",
        }

        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        return WorkflowInstance(
            id=data["uuid"],
            dataset_id=data["datasetId"],
        )
