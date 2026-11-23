"""Wiring shared by the command line (``python -m secr``) and the Automation Anywhere entry (``aa_entry.py``)."""
from __future__ import annotations

import logging
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


def apply_overrides(cfg: AppConfig, max_batches: int | None = None, max_files: int | None = None) -> None:
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
    log = get_logger()
    notifier = Notifier(cfg.notify, log)
    db = EsgDb(cfg.db, cfg.run.max_db_connections)
    loader = StagingLoader(cfg.run.created_by, cfg.run.serialize_staging_loads,
                           cfg.run.staging_lock_timeout_seconds, cfg.run.insert_chunk_size, log)

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

    processor = BatchProcessor(db, normalize, daisy, cfg.run.work_dir,
                               cfg.run.max_parallel_files_per_batch, cfg.run.keep_work_files, notifier)
    return processor, db, notifier, log_file


def execute(processor: BatchProcessor, notifier: Notifier, batch_ids: Iterable, max_parallel_batches: int
            ) -> tuple[list[BatchResult], int, str]:
    """Run the batches, send the start/end e-mails. Returns (results, exit code, summary text)."""
    log = get_logger()
    ids = [str(b) for b in batch_ids]
    notifier.send("SECR normalizer started", f"Batches: {', '.join(ids) or 'none'}")
    results = run_batches(ids, processor.process, max_parallel_batches)
    summary = summarize(results)
    log.info("Summary\n%s", summary)
    code = exit_code(results)
    notifier.send(f"SECR normalizer {'completed' if code == 0 else 'finished with errors'}", summary)
    return results, code, summary
