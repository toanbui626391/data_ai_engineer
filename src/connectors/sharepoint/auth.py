"""
Thread-safe Microsoft Entra ID (Azure AD) OAuth 2.0 token manager.
"""

from datetime import datetime, timezone
import threading
import time
from typing import Dict

import requests

from .exceptions import AuthenticationError
from .telemetry import log_json


class TokenManager:
    """Manages proactive thread-safe OAuth 2.0 client credential tokens for Microsoft Graph."""

    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self._token_lock = threading.Lock()
        self.access_token: str = ""
        self.token_expires_at: float = 0.0

    def get_auth_headers(self) -> Dict[str, str]:
        """Returns valid Authorization headers, proactively refreshing before expiration."""
        token = self.get_token()
        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        }

    def get_token(self) -> str:
        """Retrieves active access token, refreshing if expired or within 5-minute buffer."""
        now = time.time()
        # Fast path check without holding lock
        if self.access_token and (self.token_expires_at - now) > 300:
            return self.access_token

        with self._token_lock:
            # Re-check under lock (double-checked locking)
            if self.access_token and (self.token_expires_at - time.time()) > 300:
                return self.access_token

            self._refresh_token()
            return self.access_token

    def invalidate_token(self):
        """Forces next call to obtain a fresh token (e.g. after HTTP 401)."""
        with self._token_lock:
            self.access_token = ""
            self.token_expires_at = 0.0

    def _refresh_token(self):
        """Acquires fresh access token using OAuth 2.0 client credentials grant."""
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "https://graph.microsoft.com/.default",
            "grant_type": "client_credentials"
        }

        try:
            resp = requests.post(url, data=data, timeout=15)
            if resp.status_code != 200:
                error_body = resp.text
                log_json(
                    "fatal",
                    "OAuth 2.0 token acquisition failed",
                    action="auth_failure",
                    status_code=resp.status_code,
                    response=error_body
                )
                raise AuthenticationError(f"OAuth 2.0 token request failed with status {resp.status_code}: {error_body}")

            payload = resp.json()
            self.access_token = payload["access_token"]
            expires_in = int(payload.get("expires_in", 3600))
            self.token_expires_at = time.time() + expires_in

            expiry_dt = datetime.fromtimestamp(self.token_expires_at, tz=timezone.utc).isoformat()
            log_json(
                "info",
                "Acquired fresh Microsoft Entra ID access token",
                expires_in=expires_in,
                expires_at_utc=expiry_dt
            )
        except requests.RequestException as e:
            log_json("fatal", "Network error during OAuth 2.0 token acquisition", error=str(e))
            raise AuthenticationError(f"Network error acquiring OAuth 2.0 token: {e}") from e
