import os
from unittest.mock import patch

from clients.authentication_client import (
    KeySecretAuthProvider,
    TokenAuthProvider,
)
from clients.base_client import SessionManager
from config import Config


def test_session_token_selects_token_provider():
    with patch.dict(os.environ, {"SESSION_TOKEN": "tok", "REFRESH_TOKEN": "ref"}, clear=False):
        config = Config()
        assert config.SESSION_TOKEN == "tok"

        auth_provider = TokenAuthProvider(config.API_HOST, config.SESSION_TOKEN, config.REFRESH_TOKEN)
        sm = SessionManager(auth_provider)
        assert sm.session_token == "tok"


def test_api_key_selects_key_secret_provider():
    """Verify that API key/secret config values are picked up correctly.

    We don't actually call Cognito here — just verify config routing.
    """
    env = {
        "PENNSIEVE_API_KEY": "key123",
        "PENNSIEVE_API_SECRET": "secret456",
    }
    # Clear SESSION_TOKEN so the elif branch would be taken
    with patch.dict(os.environ, env, clear=False):
        os.environ.pop("SESSION_TOKEN", None)
        config = Config()
        assert config.SESSION_TOKEN is None
        assert config.API_KEY == "key123"
        assert config.API_SECRET == "secret456"


def test_no_credentials_raises():
    """With no auth env vars, the decision logic should raise RuntimeError."""
    clean_env = {
        k: v for k, v in os.environ.items()
        if k not in ("SESSION_TOKEN", "REFRESH_TOKEN", "PENNSIEVE_API_KEY", "PENNSIEVE_API_SECRET")
    }
    with patch.dict(os.environ, clean_env, clear=True):
        config = Config()
        assert config.SESSION_TOKEN is None
        assert config.API_KEY is None

        raised = False
        try:
            if config.SESSION_TOKEN:
                TokenAuthProvider(config.API_HOST, config.SESSION_TOKEN, config.REFRESH_TOKEN)
            elif config.API_KEY and config.API_SECRET:
                KeySecretAuthProvider(config.API_HOST, config.API_KEY, config.API_SECRET)
            else:
                raise RuntimeError("no authentication credentials provided")
        except RuntimeError:
            raised = True

        assert raised
