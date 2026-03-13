import base64
import json
import logging
from abc import ABC, abstractmethod

import boto3
import requests

log = logging.getLogger()


class AuthProvider(ABC):
    """Interface for authentication strategies.

    All auth methods ultimately produce a session token and the ability to
    refresh it. Implementations differ only in how they bootstrap.
    """

    @abstractmethod
    def get_session_token(self) -> str:
        """Return the current session token."""
        ...

    @abstractmethod
    def refresh(self) -> str:
        """Refresh and return a new session token."""
        ...


class CognitoClient:
    """Shared Cognito interaction logic used by all auth providers."""

    def __init__(self, api_host):
        self.api_host = api_host
        self._cognito_config = None

    def _get_cognito_config(self):
        if self._cognito_config is not None:
            return self._cognito_config

        url = f"{self.api_host}/authentication/cognito-config"
        response = requests.get(url)
        response.raise_for_status()
        data = json.loads(response.content)

        self._cognito_config = {
            "user_pool_client_id": data["userPool"]["appClientId"],
            "token_pool_client_id": data["tokenPool"]["appClientId"],
            "region": data["region"],
        }
        return self._cognito_config

    def _get_idp_client(self):
        config = self._get_cognito_config()
        return boto3.client(
            "cognito-idp",
            region_name=config["region"],
            aws_access_key_id="",
            aws_secret_access_key="",
        )

    def authenticate(self, api_key, api_secret):
        """Exchange API key/secret for session + refresh tokens via Cognito USER_PASSWORD_AUTH."""
        config = self._get_cognito_config()
        idp_client = self._get_idp_client()

        login_response = idp_client.initiate_auth(
            AuthFlow="USER_PASSWORD_AUTH",
            AuthParameters={"USERNAME": api_key, "PASSWORD": api_secret},
            ClientId=config["token_pool_client_id"],
        )

        auth_result = login_response["AuthenticationResult"]
        return auth_result["AccessToken"], auth_result["RefreshToken"]

    @staticmethod
    def _decode_token(token):
        """Decode a JWT payload without verification (for extracting claims like device_key)."""
        payload = token.split(".")[1]
        # JWT base64url encoding may lack padding
        padding = 4 - len(payload) % 4
        if padding != 4:
            payload += "=" * padding
        return json.loads(base64.urlsafe_b64decode(payload))

    def refresh_token(self, refresh_token, session_token=None):
        """Use a refresh token to obtain a new access token via Cognito REFRESH_TOKEN_AUTH."""
        config = self._get_cognito_config()
        idp_client = self._get_idp_client()

        auth_parameters = {"REFRESH_TOKEN": refresh_token}

        device_key = None
        if session_token:
            try:
                decoded = self._decode_token(session_token)
                device_key = decoded.get("device_key")
                if device_key:
                    log.info(f"extracted device_key from session token: {device_key}")
            except Exception as e:
                log.warning(f"failed to extract device_key from session token: {e}")

        if device_key:
            auth_parameters["DEVICE_KEY"] = device_key

        response = idp_client.initiate_auth(
            AuthFlow="REFRESH_TOKEN_AUTH",
            AuthParameters=auth_parameters,
            ClientId=config["token_pool_client_id"],
        )

        return response["AuthenticationResult"]["AccessToken"]


class TokenAuthProvider(AuthProvider):
    """Auth provider for pre-supplied session + refresh tokens (production path)."""

    def __init__(self, api_host, session_token, refresh_token):
        self._session_token = session_token
        self._refresh_token = refresh_token
        self._cognito = CognitoClient(api_host)

    def get_session_token(self) -> str:
        return self._session_token

    def refresh(self) -> str:
        if not self._refresh_token:
            raise RuntimeError("cannot refresh session: no refresh token available")
        log.info("refreshing session token using refresh token")
        self._session_token = self._cognito.refresh_token(self._refresh_token, self._session_token)
        return self._session_token


class KeySecretAuthProvider(AuthProvider):
    """Auth provider that authenticates with API key/secret (local development path).

    Authenticates eagerly on construction to obtain session + refresh tokens,
    then refreshes using the same Cognito refresh flow as TokenAuthProvider.
    """

    def __init__(self, api_host, api_key, api_secret):
        self._api_key = api_key
        self._api_secret = api_secret
        self._cognito = CognitoClient(api_host)

        log.info("authenticating with API key/secret")
        self._session_token, self._refresh_token = self._cognito.authenticate(api_key, api_secret)

    def get_session_token(self) -> str:
        return self._session_token

    def refresh(self) -> str:
        if self._refresh_token:
            log.info("refreshing session token using refresh token")
            self._session_token = self._cognito.refresh_token(self._refresh_token, self._session_token)
        else:
            log.info("no refresh token, re-authenticating with API key/secret")
            self._session_token, self._refresh_token = self._cognito.authenticate(
                self._api_key, self._api_secret
            )
        return self._session_token
