"""Odin AI agent client (port of ST_GetAIResponse_ParallelExecution's REST calls)."""
from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any

from .config import OdinConfig

# Keys in json_response.normalized_csv_file_links, in the order the bot checked them.
CATEGORY_KEYS = (
    "portfolio",
    "landlord",
    "transport",
    "other_fuels",
    "metrics",
    "energy_efficiency_narrative",
    "invoice",
    "half_hourly_data",
)

# Readable name written to UploadedFile.CategoryRecognized.
CATEGORY_DISPLAY_NAMES = {
    "portfolio": "Portfolio",
    "landlord": "Landlord",
    "transport": "Transport",
    "other_fuels": "Other Fuels",
    "metrics": "Metrics",
    "energy_efficiency_narrative": "Energy Efficiency Narrative",
    "invoice": "Invoice",
    "half_hourly_data": "Half Hourly Data",
}


class AIError(Exception):
    pass


class OdinClient:
    def __init__(self, cfg: OdinConfig):
        if not (cfg.api_key and cfg.api_secret):
            raise ValueError("SECR_ODIN_API_KEY / SECR_ODIN_API_SECRET are not set")
        if not (cfg.project_id and cfg.agent_id):
            raise ValueError("odin.project_id / odin.agent_id are not configured")
        self.cfg = cfg
        self._local = threading.local()  # one requests.Session per thread

    @property
    def _http(self):
        import requests

        session = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers.update({"X-API-KEY": self.cfg.api_key, "X-API-SECRET": self.cfg.api_secret})
            self._local.session = session
        return session

    def create_chat(self, name: str) -> str:
        resp = self._http.post(
            f"{self.cfg.base_uri}/chat/create",
            json={"project_id": self.cfg.project_id, "name": name},
            timeout=self.cfg.request_timeout_seconds,
        )
        resp.raise_for_status()
        chat_id = (resp.json() or {}).get("chat_id")
        if not chat_id:
            raise AIError(f"Chat creation returned no chat id. Response: {resp.text[:500]}")
        return str(chat_id)

    def send_file(self, chat_id: str, file_path: Path) -> bool:
        """Upload the file to the chat. Returns False when the request timed out.

        A timeout is treated as accepted (the bot did the same, "fire and forget"):
        the response is polled afterwards anyway.
        """
        import requests

        data = {
            "project_id": self.cfg.project_id,
            "agent_id": self.cfg.agent_id,
            "chat_id": chat_id,
            "message": self.cfg.message,
        }
        try:
            with file_path.open("rb") as fh:
                resp = self._http.post(
                    f"{self.cfg.base_uri}/v4/chat/message",
                    data=data,
                    files={"quick_upload_file": (file_path.name, fh)},
                    timeout=self.cfg.submit_timeout_seconds,
                )
            resp.raise_for_status()
            return True
        except requests.Timeout:
            return False

    def get_csv_links(self, chat_id: str) -> dict[str, str] | None:
        """Return {category_key: link} once the agent answered, or None while it is still working."""
        resp = self._http.get(
            f"{self.cfg.base_uri}/project/{self.cfg.project_id}/chat/{chat_id}",
            timeout=self.cfg.request_timeout_seconds,
        )
        resp.raise_for_status()
        return parse_agent_response(resp.json())


def parse_agent_response(body: dict[str, Any]) -> dict[str, str] | None:
    messages = (body or {}).get("messages") or []
    if not messages:
        return None
    json_response = messages[0].get("json_response")
    if not json_response:
        return None
    if isinstance(json_response, str):
        try:
            json_response = json.loads(json_response)
        except ValueError:
            raise AIError(f"Agent json_response is not valid JSON: {json_response[:300]}")
    links = (json_response or {}).get("normalized_csv_file_links") or {}
    return {key: str(links[key]) for key in CATEGORY_KEYS if links.get(key)}
