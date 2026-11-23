"""Settings for one run. They come from the AA dictionary (or CLI) plus environment variables.

Secrets are read ONLY from environment variables (or the optional ``secrets`` dictionary that
aa_entry.py copies into the environment), never from files:

    SECR_ODIN_API_KEY, SECR_ODIN_API_SECRET   - ESG Locker / AA ESG EKB Credential
    SECR_DB_USER, SECR_DB_PASSWORD            - ESG Locker / ESGDB MySQL (only when update_db is on)
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

DEFAULT_AI_MESSAGE = (
    "extract and make sure not to skip any records then normalize and return the csv file link"
)

# Key names used by the AA bot's dProcessConfig, accepted as aliases so the bot can pass them as-is.
_ALIASES = {
    "base_uri": ("base_uri", "ENV_Base_URI"),
    "project_id": ("project_id", "Agent_Project_ID"),
    "agent_id": ("agent_id", "Agent_ID"),
    "db_host": ("db_host", "ESGDBServerName"),
    "db_name": ("db_name", "DatabaseName"),
    "db_port": ("db_port", "MySQL_Port"),
}


@dataclass
class AIConfig:
    base_uri: str = "https://api.getodin.ai"
    project_id: str = ""
    agent_id: str = ""
    api_key: str = ""
    api_secret: str = ""
    message: str = DEFAULT_AI_MESSAGE
    submit_timeout_seconds: int = 300      # a timeout while uploading is treated as "accepted" (as the bot did)
    request_timeout_seconds: int = 120
    poll_interval_seconds: int = 40        # bot: Delay 40 s
    max_poll_attempts: int = 15            # bot: gives up when the loop counter reaches 15
    max_parallel: int = 10                 # sheets submitted / polled at the same time

    update_db: bool = True                 # write UploadedFile status + CategoryRecognized like the bot
    db_host: str = ""
    db_name: str = "esgdb"
    db_port: int = 3306
    db_user: str = ""
    db_password: str = ""

    log_dir: Path = Path(__file__).resolve().parents[1] / "logs"
    debug: bool = False

    def validate(self) -> None:
        problems = []
        if not (self.api_key and self.api_secret):
            problems.append("SECR_ODIN_API_KEY / SECR_ODIN_API_SECRET are not set")
        if not (self.project_id and self.agent_id):
            problems.append("project_id (Agent_Project_ID) and agent_id (Agent_ID) are required")
        if self.update_db and not self.db_host:
            problems.append("db_host (ESGDBServerName) is required when update_db is true")
        if self.update_db and not (self.db_user and self.db_password):
            problems.append("SECR_DB_USER / SECR_DB_PASSWORD are not set (or pass update_db=false)")
        if self.max_parallel < 1 or self.max_poll_attempts < 1 or self.poll_interval_seconds < 0:
            problems.append("max_parallel and max_poll_attempts must be >= 1, poll_interval_seconds >= 0")
        if problems:
            raise ValueError("Invalid settings: " + "; ".join(problems))


def as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "y")


def _pick(args: Mapping[str, Any], name: str):
    for key in _ALIASES.get(name, (name,)):
        value = args.get(key)
        if value not in (None, ""):
            return value
    return None


def from_args(args: Mapping[str, Any]) -> AIConfig:
    """Build the settings from a dictionary (AA) - unknown keys are ignored, every value may be text."""
    cfg = AIConfig()
    for name in ("base_uri", "project_id", "agent_id", "message", "db_host", "db_name"):
        value = _pick(args, name)
        if value is not None:
            setattr(cfg, name, str(value).strip().rstrip("/") if name == "base_uri" else str(value).strip())
    for name in ("submit_timeout_seconds", "request_timeout_seconds", "poll_interval_seconds",
                 "max_poll_attempts", "max_parallel", "db_port"):
        value = _pick(args, name)
        if value is not None:
            setattr(cfg, name, int(float(value)))
    for name in ("update_db", "debug"):
        value = _pick(args, name)
        if value is not None:
            setattr(cfg, name, as_bool(value))
    if _pick(args, "log_dir"):
        cfg.log_dir = Path(str(_pick(args, "log_dir")))

    cfg.api_key = os.environ.get("SECR_ODIN_API_KEY", "")
    cfg.api_secret = os.environ.get("SECR_ODIN_API_SECRET", "")
    cfg.db_user = os.environ.get("SECR_DB_USER", "")
    cfg.db_password = os.environ.get("SECR_DB_PASSWORD", "")
    return cfg
