"""Logging: console + one log file per run, with batch / file context on every line.

Every line looks like::

    2026-11-23 10:15:02,123 INFO    [run 3f2a9c1b7d40] [b40-file_0] [secr.normalize] [batch 40][file 77] Sheet 'Data' submitted

* ``run``      - correlation id of this run (one per CLI / AA call)
* thread name  - ``batch_N`` = batch worker, ``b<batch>-file_N`` = file worker of that batch
* logger name  - which module wrote the line (secr.db, secr.storage, secr.ai_client, ...)
* ``[batch][file]`` - set per worker thread with :func:`log_context`, so even low-level modules
  (DB, S3, Odin, CSV) that do not know the batch show it.

Use ``--debug`` (CLI) or ``debug: true`` (AA) to also see every SQL statement, HTTP call and poll.
"""
from __future__ import annotations

import contextlib
import logging
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Iterator

LOGGER_NAME = "secr"

_ctx = threading.local()


def _current_prefix() -> str:
    parts = []
    batch_id = getattr(_ctx, "batch_id", None)
    file_id = getattr(_ctx, "file_id", None)
    if batch_id is not None:
        parts.append(f"[batch {batch_id}]")
    if file_id is not None:
        parts.append(f"[file {file_id}]")
    return "".join(parts) + " " if parts else ""


class _ContextFilter(logging.Filter):
    """Adds ``%(ctx)s`` (the batch/file prefix of the current thread) to every record."""

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "ctx"):
            record.ctx = _current_prefix()
        return True


@contextlib.contextmanager
def log_context(**values) -> Iterator[None]:
    """Set batch_id / file_id for every log line written by this thread inside the block."""
    old = {k: getattr(_ctx, k, None) for k in values}
    for k, v in values.items():
        setattr(_ctx, k, v)
    try:
        yield
    finally:
        for k, v in old.items():
            setattr(_ctx, k, v)


def current_context() -> dict:
    """The batch/file tags of this thread, to pass on to worker threads with log_context(**...)."""
    return {"batch_id": getattr(_ctx, "batch_id", None), "file_id": getattr(_ctx, "file_id", None)}


def setup_logging(log_dir: Path, level: int = logging.INFO) -> tuple[logging.Logger, str, str]:
    """Configure the 'secr' logger. Returns the logger, the run's correlation id and the log file path.

    Safe to call again in the same Python process (AA keeps the session open): old handlers are closed.
    """
    correlation_id = uuid.uuid4().hex[:12]
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"SECR_{datetime.now():%Y%m%d_%H%M%S}_{correlation_id}.log"

    fmt = logging.Formatter(
        f"%(asctime)s %(levelname)-7s [run {correlation_id}] [%(threadName)s] [%(name)s] %(ctx)s%(message)s"
    )
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level)
    for old in list(logger.handlers):
        logger.removeHandler(old)
        old.close()
    logger.propagate = False

    console = logging.StreamHandler()
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    for handler in (console, file_handler):
        handler.setFormatter(fmt)
        handler.addFilter(_ContextFilter())
        logger.addHandler(handler)

    # Third-party libraries stay quiet unless something goes wrong.
    for noisy in ("botocore", "boto3", "s3transfer", "urllib3", "requests", "pymysql"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    logger.info("Log file: %s (level %s)", log_file, logging.getLevelName(level))
    return logger, correlation_id, str(log_file)


def module_logger(name: str) -> logging.Logger:
    """Logger for a module, e.g. ``module_logger(__name__)`` -> 'secr.db'. Inherits the run's handlers."""
    return logging.getLogger(name if name.startswith(LOGGER_NAME) else f"{LOGGER_NAME}.{name}")


@contextlib.contextmanager
def timed_step(log, step: str, level: int = logging.INFO) -> Iterator[None]:
    """Log '<step> - started' / '<step> - done in 1.2s' / '<step> - FAILED after 1.2s: error'."""
    log.log(level, "%s - started", step)
    started = time.monotonic()
    try:
        yield
    except Exception as exc:
        log.error("%s - FAILED after %.1fs: %s: %s", step, time.monotonic() - started, type(exc).__name__, exc)
        raise
    log.log(level, "%s - done in %.1fs", step, time.monotonic() - started)


class ContextLogger(logging.LoggerAdapter):
    """Logger bound to a batch / file (kept for the flows; the prefix comes from ``extra``)."""

    def process(self, msg, kwargs):
        parts = []
        if self.extra.get("batch_id") is not None:
            parts.append(f"[batch {self.extra['batch_id']}]")
        if self.extra.get("file_id") is not None:
            parts.append(f"[file {self.extra['file_id']}]")
        extra = dict(kwargs.get("extra") or {})
        extra.setdefault("ctx", "".join(parts) + " " if parts else _current_prefix())
        kwargs["extra"] = extra
        return msg, kwargs

    def child(self, **extra) -> "ContextLogger":
        merged = dict(self.extra)
        merged.update(extra)
        return ContextLogger(self.logger, merged)


def get_logger(name: str = LOGGER_NAME, **extra) -> ContextLogger:
    return ContextLogger(module_logger(name), extra)
