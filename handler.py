"""
Lambda handler that bridges the Lambda event payload to environment variables
expected by the processor logic in process.py.
"""

import os
import subprocess


def handler(event, context):
    os.environ["INPUT_DIR"] = event.get("inputDir", "")
    os.environ["OUTPUT_DIR"] = event.get("outputDir", "")
    os.environ["INTEGRATION_ID"] = event.get("workflowInstanceId", "")
    os.environ["SESSION_TOKEN"] = event.get("sessionToken", "")
    os.environ["REFRESH_TOKEN"] = event.get("refreshToken", "")
    os.environ["PENNSIEVE_API_KEY"] = event.get("apiKey", "")
    os.environ["PENNSIEVE_API_SECRET"] = event.get("apiSecret", "")
    os.environ["DATASET_ID"] = event.get("datasetId", "")
    os.environ["FILE_EXTENSIONS"] = event.get("fileExtensions", ".lay")
    os.environ["RESTRICTED_WORDS"] = event.get("restrictedWords", "MRN,DOB")

    subprocess.run(["python", "process.py"], check=True)
    return {"status": "success"}
