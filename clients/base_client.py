import logging

import requests

log = logging.getLogger()


class SessionManager:
    """Encapsulates a shared API session and token refresh."""

    def __init__(self, auth_provider):
        self._auth_provider = auth_provider

    @property
    def session_token(self):
        return self._auth_provider.get_session_token()

    def refresh_session(self):
        self._auth_provider.refresh()


class BaseClient:
    def __init__(self, session_manager):
        self.session_manager = session_manager

    def retry_with_refresh(func):
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code in (401, 403):
                    log.warning("refreshing session")
                    self.session_manager.refresh_session()
                    return func(self, *args, **kwargs)
                raise

        return wrapper
