"""Odin AI agent client (port of ST_GetAIResponse_ParallelExecution's REST calls)."""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any

from .config import OdinConfig
from .logging_setup import module_logger

LOG = module_logger(__name__)

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
        LOG.info("Odin AI client ready: %s, project %s, agent %s", cfg.base_uri, cfg.project_id, cfg.agent_id)

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
        LOG.info("Chat %s created for %s", chat_id, name)
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
            LOG.info("Upload to chat %s accepted in %.1fs", chat_id, time.monotonic() - started)
            return True
        except requests.Timeout:
            LOG.warning("Upload to chat %s timed out after %.1fs (treated as accepted)", chat_id,
                        time.monotonic() - started)
            return False

    def get_csv_links(self, chat_id: str) -> dict[str, str] | None:
        """Return {category_key: link} once the agent answered, or None while it is still working."""
        started = time.monotonic()
        resp = self._http.get(
            f"{self.cfg.base_uri}/project/{self.cfg.project_id}/chat/{chat_id}",
            timeout=self.cfg.request_timeout_seconds,
        )
        _trace(f"GET /chat/{chat_id}", resp, started)
        resp.raise_for_status()
        links = parse_agent_response(resp.json())
        if links is not None:
            LOG.info("Chat %s answered with %d CSV link(s): %s", chat_id, len(links), ", ".join(links) or "none")
        return links


def _trace(call: str, resp, started: float) -> None:
    elapsed = time.monotonic() - started
    if resp.status_code >= 400:
        LOG.error("Odin %s -> HTTP %s in %.1fs: %s", call, resp.status_code, elapsed, resp.text[:500])
    else:
        LOG.debug("Odin %s -> HTTP %s in %.1fs", call, resp.status_code, elapsed)


_NOT_A_LINK = {"", "na", "n/a", "n.a.", "none", "null", "nan", "-"}


def is_real_link(value) -> bool:
    """True only for an http(s) URL. The agent writes 'NA' (or similar) for categories it did not find."""
    if value is None or str(value).strip().lower() in _NOT_A_LINK:
        return False
    from urllib.parse import urlparse

    parsed = urlparse(str(value).strip())
    return parsed.scheme in ("http", "https") and bool(parsed.netloc)


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
    raw_links = (json_response or {}).get("normalized_csv_file_links") or {}
    # Categories the sheet does not contain come back as "NA": they are not links, so they are left out
    # (no download, no load, not counted as a failure, not added to CategoryRecognized).
    not_available = [k for k, v in raw_links.items() if not is_real_link(v)]
    if not_available:
        LOG.debug("Categories without a CSV link (NA), left out: %s", ", ".join(not_available))
    links = {k: str(v).strip() for k, v in raw_links.items() if is_real_link(v)}
    unknown = [k for k in links if k not in CATEGORY_KEYS]
    if unknown:
        LOG.warning("Agent returned link(s) for unknown categories, ignored: %s", unknown)
    if not links:
        LOG.warning("Agent answered without any CSV link (all NA or missing). Keys in json_response: %s",
                    list((json_response or {}).keys()))
    return {key: links[key] for key in CATEGORY_KEYS if key in links}
