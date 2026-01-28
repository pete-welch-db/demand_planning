from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import requests

from config import AppConfig


@dataclass(frozen=True)
class GenieAnswer:
    ok: bool
    text: str
    raw: Optional[dict] = None


class GenieClient:
    """
    Minimal “Genie-style assistant” wrapper.

    Databricks Genie APIs and auth patterns can vary by workspace.
    For this demo we:
    - keep the integration optional
    - gracefully fall back if not configured
    """

    def __init__(self, cfg: AppConfig):
        self.cfg = cfg

    def is_configured(self) -> bool:
        return bool(self.cfg.genie_space_id and self.cfg.databricks_host and self.cfg.databricks_token)

    def ask(self, question: str) -> GenieAnswer:
        if not self.is_configured():
            return GenieAnswer(
                ok=False,
                text="Genie is not configured (need GENIE_SPACE_ID, DATABRICKS_HOST, DATABRICKS_TOKEN). Falling back to local insights.",
                raw=None,
            )

        # NOTE: Replace this endpoint with the official Genie endpoint for your workspace.
        # Keeping it as a stub so the app runs anywhere.
        url = f"{self.cfg.databricks_host.rstrip('/')}/api/2.0/genie/spaces/{self.cfg.genie_space_id}/ask"
        headers = {"Authorization": f"Bearer {self.cfg.databricks_token}"}
        payload = {"question": question}

        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=30)
            if resp.status_code >= 300:
                return GenieAnswer(ok=False, text=f"Genie call failed ({resp.status_code}). Falling back.", raw={"body": resp.text})
            data = resp.json()
            text = data.get("answer") or data.get("text") or str(data)
            return GenieAnswer(ok=True, text=text, raw=data)
        except Exception as e:
            return GenieAnswer(ok=False, text=f"Genie call errored ({type(e).__name__}). Falling back.", raw=None)

