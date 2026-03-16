"""
Lambda handler that bridges the Lambda event payload to environment variables
expected by the processor logic in process.py.
"""

import os
import subprocess


def _set_from_event(event, event_key, env_key, default=None):
    """Set env var from event payload, but don't overwrite existing env vars with empty strings."""
    value = event.get(event_key)
    if value:
        os.environ[env_key] = value
    elif default is not None and not os.environ.get(env_key):
        os.environ[env_key] = default


def handler(event, context):
    _set_from_event(event, "inputDir", "INPUT_DIR")
    _set_from_event(event, "outputDir", "OUTPUT_DIR")
    _set_from_event(event, "workflowInstanceId", "INTEGRATION_ID")
    _set_from_event(event, "sessionToken", "SESSION_TOKEN")
    _set_from_event(event, "refreshToken", "REFRESH_TOKEN")
    _set_from_event(event, "apiKey", "PENNSIEVE_API_KEY")
    _set_from_event(event, "apiSecret", "PENNSIEVE_API_SECRET")
    _set_from_event(event, "datasetId", "DATASET_ID")
    _set_from_event(event, "fileExtensions", "FILE_EXTENSIONS", default=".lay")
    _set_from_event(event, "restrictedWords", "RESTRICTED_WORDS", default="MRN,DOB")

    subprocess.run(["python", "process.py"], check=True)
    return {"status": "success"}
