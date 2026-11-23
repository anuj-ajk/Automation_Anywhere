"""Logging: console + one log file per run, with batch / file context on every line."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime
from pathlib import Path

LOGGER_NAME = "secr"


def setup_logging(log_dir: Path, level: int = logging.INFO) -> tuple[logging.Logger, str, str]:
    """Configure the 'secr' logger. Returns the logger, the run's correlation id and the log file path.

    Safe to call again in the same Python process (AA keeps the session open): old handlers are closed.
    """
    correlation_id = uuid.uuid4().hex[:12]
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"SECR_{datetime.now():%Y%m%d_%H%M%S}_{correlation_id}.log"

    fmt = logging.Formatter(
        f"%(asctime)s %(levelname)-7s [{correlation_id}] [%(threadName)s] %(message)s"
    )
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level)
    for old in list(logger.handlers):
        logger.removeHandler(old)
        old.close()
    logger.propagate = False

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    logger.info("Log file: %s", log_file)
    return logger, correlation_id, str(log_file)


class ContextLogger(logging.LoggerAdapter):
    """Prefixes messages with e.g. ``[batch 40][file 77]`` (replaces the AA 'reference' field)."""

    def process(self, msg, kwargs):
        parts = []
        if self.extra.get("batch_id") is not None:
            parts.append(f"[batch {self.extra['batch_id']}]")
        if self.extra.get("file_id") is not None:
            parts.append(f"[file {self.extra['file_id']}]")
        prefix = "".join(parts)
        return (f"{prefix} {msg}" if prefix else msg), kwargs

    def child(self, **extra) -> "ContextLogger":
        merged = dict(self.extra)
        merged.update(extra)
        return ContextLogger(self.logger, merged)


def get_logger(**extra) -> ContextLogger:
    return ContextLogger(logging.getLogger(LOGGER_NAME), extra)
