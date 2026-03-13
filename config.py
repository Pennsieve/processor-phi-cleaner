import logging
import os
import uuid

log = logging.getLogger()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class Config:
    def __init__(self):
        self.ENVIRONMENT = os.getenv("ENVIRONMENT", "local")

        self.INPUT_DIR = os.getenv("INPUT_DIR", os.path.join(BASE_DIR, "input"))
        self.OUTPUT_DIR = os.getenv("OUTPUT_DIR", os.path.join(BASE_DIR, "output"))

        self.WORKFLOW_INSTANCE_ID = os.getenv("INTEGRATION_ID")

        self.SESSION_TOKEN = os.getenv("SESSION_TOKEN")
        self.REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")
        self.API_KEY = os.getenv("PENNSIEVE_API_KEY")
        self.API_SECRET = os.getenv("PENNSIEVE_API_SECRET")
        self.API_HOST = os.getenv("PENNSIEVE_API_HOST", "https://api.pennsieve.net")
        self.API_HOST2 = os.getenv("PENNSIEVE_API_HOST2", "https://api2.pennsieve.net")

        self.DATASET_ID = os.getenv("DATASET_ID")
        self.FILE_EXTENSIONS = [e.strip().lower() for e in os.getenv("FILE_EXTENSIONS", ".lay").split(",")]
        self.RESTRICTED_WORDS = [w.strip() for w in os.getenv("RESTRICTED_WORDS", "MRN,DOB").split(",") if w.strip()]
        self.VERBOSE = os.getenv("VERBOSE", "").lower() in ("1", "true", "yes")
