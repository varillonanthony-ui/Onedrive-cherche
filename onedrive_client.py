"""
OneDrive API client using Microsoft Graph API + MSAL device flow auth.
"""
import os
import json
import time
import requests
import msal


GRAPH_API = "https://graph.microsoft.com/v1.0"
SCOPES = ["Files.Read.All", "User.Read", "offline_access"]

TOKEN_CACHE_FILE = ".token_cache.json"


class OneDriveClient:
    def __init__(self, client_id: str, client_secret: str, tenant_id: str = "common"):
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.token = None
        self._cache = msal.SerializableTokenCache()

        if os.path.exists(TOKEN_CACHE_FILE):
            with open(TOKEN_CACHE_FILE, "r") as f:
                self._cache.deserialize(f.read())

        self.app = msal.PublicClientApplication(
            client_id=self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
            token_cache=self._cache,
        )

    def _save_cache(self):
        if self._cache.has_state_changed:
            with open(TOKEN_CACHE_FILE, "w") as f:
                f.write(self._cache.serialize())

    def authenticate_device_flow(self) -> dict | None:
        """Try silent first, then device flow."""
        accounts = self.app.get_accounts()
        if accounts:
            result = self.app.acquire_token_silent(SCOPES, account=accounts[0])
            if result and "access_token" in result:
                self.token = result
                self._save_cache()
                return result

        flow = self.app.initiate_device_flow(scopes=SCOPES)
        if "user_code" not in flow:
            raise ValueError(f"Impossible d'initier le device flow: {flow.get('error_description')}")

        # Print message for user to open browser
        print("\n" + "="*60)
        print(flow["message"])
        print("="*60 + "\n")

        # In Streamlit we show it via returned message
        self._pending_flow = flow
        result = self.app.acquire_token_by_device_flow(flow)
        if "access_token" in result:
            self.token = result
            self._save_cache()
            return result
        raise ValueError(f"Auth échouée : {result.get('error_description', result)}")

    def get_device_flow_message(self) -> str | None:
        """Return the device flow message for display in UI."""
        if hasattr(self, "_pending_flow"):
            return self._pending_flow.get("message", "")
        return None

    def _headers(self) -> dict:
        if not self.token:
            raise RuntimeError("Non authentifié. Appelez authenticate_device_flow() d'abord.")
        return {
            "Authorization": f"Bearer {self.token['access_token']}",
            "Content-Type": "application/json",
        }

    def _get(self, url: str, params: dict = None) -> dict:
        resp = requests.get(url, headers=self._headers(), params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def get_user_info(self) -> dict:
        return self._get(f"{GRAPH_API}/me")

    def list_items(self, folder_path: str = "/", delta_link: str = None) -> tuple[list, str | None]:
        """
        List all items under a folder path.
        Returns (items, next_delta_link).
        """
        items = []
        if delta_link:
            url = delta_link
        elif folder_path == "/":
            url = f"{GRAPH_API}/me/drive/root/delta"
        else:
            # Encode path
            encoded = folder_path.rstrip("/")
            url = f"{GRAPH_API}/me/drive/root:{encoded}:/delta"

        while url:
            data = self._get(url)
            items.extend(data.get("value", []))
            url = data.get("@odata.nextLink")
            if not url:
                break

        delta_link = data.get("@odata.deltaLink") if items or True else None
        return items, delta_link

    def get_item_metadata(self, item_id: str) -> dict:
        return self._get(f"{GRAPH_API}/me/drive/items/{item_id}")

    def download_file(self, item_id: str, max_bytes: int = 20_000_000) -> bytes | None:
        """Download file content (up to max_bytes). Returns None if too large."""
        meta = self.get_item_metadata(item_id)
        size = meta.get("size", 0)
        if size > max_bytes:
            return None

        url = f"{GRAPH_API}/me/drive/items/{item_id}/content"
        resp = requests.get(url, headers=self._headers(), stream=True, timeout=60)
        resp.raise_for_status()
        return resp.content

    def get_item_path(self, item: dict) -> str:
        """Build full path string from item's parentReference + name."""
        parent_path = item.get("parentReference", {}).get("path", "")
        # Strip '/drive/root:' prefix
        if parent_path.startswith("/drive/root:"):
            parent_path = parent_path[len("/drive/root:"):]
        name = item.get("name", "")
        if parent_path:
            return f"{parent_path}/{name}"
        return f"/{name}"
