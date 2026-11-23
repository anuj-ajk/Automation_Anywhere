"""Wiring shared by the command line (``python -m secr``) and the Automation Anywhere entry (``aa_entry.py``)."""
from __future__ import annotations

import logging
import os
import platform
import sys
from typing import Iterable

from .batch import BatchProcessor
from .config import AppConfig
from .daisy import DaisyFlow
from .db import EsgDb
from .loader import StagingLoader
from .logging_setup import get_logger, setup_logging
from .models import BatchResult
from .normalize import NormalizeFlow
from .notifier import Notifier
from .orchestrator import exit_code, run_batches, summarize


def apply_overrides(cfg: AppConfig, max_batches: int | None = None, max_files: int | None = None,
                    force: bool = False) -> None:
    if force:  # process every file of the batch again, also those already completed
        cfg.run.skip_completed_files = False
    if max_batches:
        cfg.run.max_parallel_batches = int(max_batches)
    if max_files:
        cfg.run.max_parallel_files_per_batch = int(max_files)
    cfg.run.max_db_connections = max(
        cfg.run.max_db_connections,
        cfg.run.max_parallel_batches * (cfg.run.max_parallel_files_per_batch + 1),
    )


def build(cfg: AppConfig, debug: bool = False) -> tuple[BatchProcessor, EsgDb, Notifier, str]:
    """Set up logging and all services. Returns (processor, db, notifier, log_file)."""
    _, _, log_file = setup_logging(cfg.run.log_dir, logging.DEBUG if debug else logging.INFO)
    log = get_logger(__name__)
    _log_settings(cfg, log)
    notifier = Notifier(cfg.notify, log)
    db = EsgDb(cfg.db, cfg.run.max_db_connections)
    loader = StagingLoader(cfg.run.created_by, cfg.run.serialize_staging_loads,
                           cfg.run.staging_lock_timeout_seconds, cfg.run.insert_chunk_size, log,
                           clear_staging=cfg.run.clear_staging_before_load)

    # Created separately so a Daisy-only setup does not need S3/Odin settings and vice versa.
    normalize = daisy = None
    try:
        from .ai_client import OdinClient
        from .storage import S3Storage

        normalize = NormalizeFlow(db, S3Storage(cfg.s3), OdinClient(cfg.odin), loader,
                                  cfg.odin.poll_interval_seconds, cfg.odin.max_poll_attempts)
    except Exception as exc:
        log.warning("Normalize flow disabled: %s", exc)
    try:
        from .source_systems import SourceSystems

        daisy = DaisyFlow(db, SourceSystems(cfg.sources), loader)
    except Exception as exc:
        log.warning("Daisy flow disabled: %s", exc)

    log.info("Flows enabled: Normalize=%s, Daisy=%s", normalize is not None, daisy is not None)
    processor = BatchProcessor(db, normalize, daisy, cfg.run.work_dir,
                               cfg.run.max_parallel_files_per_batch, cfg.run.keep_work_files, notifier,
                               skip_statuses=cfg.run.completed_statuses if cfg.run.skip_completed_files else (),
                               status_on_file_errors=cfg.run.batch_status_on_file_errors)
    return processor, db, notifier, log_file


_SECRET_VARS = ("SECR_DB_USER", "SECR_DB_PASSWORD", "SECR_S3_BUCKET", "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY", "SECR_ODIN_API_KEY", "SECR_ODIN_API_SECRET",
                "SECR_EM_CONNECTION_STRING", "SECR_NAV_CONNECTION_STRING")


def _log_settings(cfg: AppConfig, log) -> None:
    """Write the effective settings at the start of the log. Secret values are never logged, only set/not set."""
    r = cfg.run
    log.info("Python %s on %s (pid %d)", sys.version.split()[0], platform.node(), os.getpid())
    log.info("Parallelism: %d batch(es) x %d file(s) per batch, max %d DB connection(s)",
             r.max_parallel_batches, r.max_parallel_files_per_batch, r.max_db_connections)
    log.info("Work folder %s (keep files: %s), serialize staging loads: %s, created by: %s",
             r.work_dir, r.keep_work_files, r.serialize_staging_loads, r.created_by)
    if str(r.work_dir).startswith("\\\\"):
        log.warning("run.work_dir %s is on a network share: a short local folder (e.g. %%TEMP%%\\SECR_work) "
                    "is faster and keeps paths under the Windows 260-character limit", r.work_dir)
    log.info("Rerun: skip files already in %s: %s; clear earlier staging rows before load: %s",
             "/".join(r.completed_statuses), r.skip_completed_files, r.clear_staging_before_load)
    log.info("Odin polling: every %ss, up to %d attempts; source environment: %s",
             cfg.odin.poll_interval_seconds, cfg.odin.max_poll_attempts, cfg.sources.environment)
    log.info("Secrets: %s", ", ".join(f"{v}={'set' if os.environ.get(v) else 'NOT SET'}" for v in _SECRET_VARS))


def execute(processor: BatchProcessor, notifier: Notifier, batch_ids: Iterable, max_parallel_batches: int
            ) -> tuple[list[BatchResult], int, str]:
    """Run the batches, send the start/end e-mails. Returns (results, exit code, summary text)."""
    log = get_logger(__name__)
    ids = [str(b) for b in batch_ids]
    log.info("Run started for %d batch id(s): %s", len(ids), ", ".join(ids) or "none")
    notifier.send("SECR normalizer started", f"Batches: {', '.join(ids) or 'none'}")
    results = run_batches(ids, processor.process, max_parallel_batches)
    summary = summarize(results)
    log.info("Summary\n%s", summary)
    code = exit_code(results)
    log.info("Run finished with exit code %d (0 = all OK, 2 = some files failed, 1 = a batch failed)", code)
    notifier.send(f"SECR normalizer {'completed' if code == 0 else 'finished with errors'}", summary)
    return results, code, summary
