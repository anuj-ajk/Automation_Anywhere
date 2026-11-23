"""One log file per run; every line shows the thread, module and the [file][sheet] it is about."""
from __future__ import annotations

import contextlib
import logging
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Iterator

LOGGER_NAME = "ai_response"
_ctx = threading.local()


class _ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        parts = []
        if getattr(_ctx, "file_id", None) is not None:
            parts.append(f"[file {_ctx.file_id}]")
        if getattr(_ctx, "sheet", None) is not None:
            parts.append(f"[{_ctx.sheet}]")
        record.ctx = "".join(parts) + " " if parts else ""
        return True


@contextlib.contextmanager
def log_context(**values) -> Iterator[None]:
    """Tag every log line written by this thread inside the block (file_id=..., sheet=...)."""
    old = {k: getattr(_ctx, k, None) for k in values}
    for k, v in values.items():
        setattr(_ctx, k, v)
    try:
        yield
    finally:
        for k, v in old.items():
            setattr(_ctx, k, v)


def setup_logging(log_dir: Path, debug: bool = False) -> tuple[str, str]:
    """Configure the logger; safe to call again in a long-lived AA Python session. Returns (run id, log file)."""
    run_id = uuid.uuid4().hex[:12]
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"AIResponse_{datetime.now():%Y%m%d_%H%M%S}_{run_id}.log"
    fmt = logging.Formatter(
        f"%(asctime)s %(levelname)-7s [run {run_id}] [%(threadName)s] [%(name)s] %(ctx)s%(message)s"
    )
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    for old in list(logger.handlers):
        logger.removeHandler(old)
        old.close()
    logger.propagate = False
    for handler in (logging.StreamHandler(), logging.FileHandler(log_file, encoding="utf-8")):
        handler.setFormatter(fmt)
        handler.addFilter(_ContextFilter())
        logger.addHandler(handler)
    for noisy in ("urllib3", "requests", "pymysql"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    logger.info("Log file: %s", log_file)
    return run_id, str(log_file)


def get_logger(name: str) -> logging.Logger:
    """``get_logger(__name__)`` -> child of the 'ai_response' logger."""
    return logging.getLogger(name if name.startswith(LOGGER_NAME) else f"{LOGGER_NAME}.{name}")
