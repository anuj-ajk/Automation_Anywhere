"""Odin AI REST calls used by the bot: create chat, send the file, read the chat."""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any

from .config import AIConfig
from .logging_setup import get_logger

LOG = get_logger(__name__)

# Keys of json_response.normalized_csv_file_links, in the order the bot checked them (lCategoryNodeList).
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

# Name written to UploadedFile.CategoryRecognized (the CASE in the bot's UPDATE).
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
    def __init__(self, cfg: AIConfig):
        self.cfg = cfg
        self._local = threading.local()  # requests.Session is not thread-safe: one per worker thread
        LOG.info("Odin client: %s, project %s, agent %s", cfg.base_uri, cfg.project_id, cfg.agent_id)

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
        started = time.monotonic()
        resp = self._http.post(
            f"{self.cfg.base_uri}/chat/create",
            json={"project_id": self.cfg.project_id, "name": name},
            timeout=self.cfg.request_timeout_seconds,
        )
        _trace("POST /chat/create", resp, started)
        resp.raise_for_status()
        chat_id = (resp.json() or {}).get("chat_id")
        if not chat_id:
            raise AIError(f"Chat creation returned no chat id. Response: {resp.text[:500]}")
        LOG.info("Chat %s created", chat_id)
        return str(chat_id)

    def send_file(self, chat_id: str, file_path: Path) -> bool:
        """Upload the file to the chat. Returns False when the upload timed out (treated as accepted)."""
        import requests

        data = {"project_id": self.cfg.project_id, "agent_id": self.cfg.agent_id,
                "chat_id": chat_id, "message": self.cfg.message}
        LOG.info("Uploading %s (%d bytes) to chat %s", file_path.name, file_path.stat().st_size, chat_id)
        started = time.monotonic()
        try:
            with file_path.open("rb") as fh:
                resp = self._http.post(
                    f"{self.cfg.base_uri}/v4/chat/message",
                    data=data,
                    files={"quick_upload_file": (file_path.name, fh)},
                    timeout=self.cfg.submit_timeout_seconds,
                )
            _trace("POST /v4/chat/message", resp, started)
            resp.raise_for_status()
            LOG.info("Upload accepted in %.1fs", time.monotonic() - started)
            return True
        except requests.Timeout:
            LOG.warning("Upload timed out after %.1fs; assuming it was accepted (fire and forget)",
                        time.monotonic() - started)
            return False

    def get_csv_links(self, chat_id: str) -> dict[str, str] | None:
        """{category: link} once the agent has answered, None while it is still working."""
        started = time.monotonic()
        resp = self._http.get(
            f"{self.cfg.base_uri}/project/{self.cfg.project_id}/chat/{chat_id}",
            timeout=self.cfg.request_timeout_seconds,
        )
        _trace(f"GET /chat/{chat_id}", resp, started)
        resp.raise_for_status()
        return parse_agent_response(resp.json())


def _trace(call: str, resp, started: float) -> None:
    elapsed = time.monotonic() - started
    if resp.status_code >= 400:
        LOG.error("Odin %s -> HTTP %s in %.1fs: %s", call, resp.status_code, elapsed, resp.text[:500])
    else:
        LOG.debug("Odin %s -> HTTP %s in %.1fs", call, resp.status_code, elapsed)


def parse_agent_response(body: dict[str, Any]) -> dict[str, str] | None:
    """Read messages[0].json_response.normalized_csv_file_links (same JSON paths as the bot)."""
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
    unknown = [k for k, v in links.items() if v and k not in CATEGORY_KEYS]
    if unknown:
        LOG.warning("Agent returned link(s) for unknown categories, ignored: %s", unknown)
    if not links:
        LOG.warning("Agent answered without normalized_csv_file_links. json_response keys: %s",
                    list((json_response or {}).keys()))
    return {key: str(links[key]) for key in CATEGORY_KEYS if links.get(key)}
