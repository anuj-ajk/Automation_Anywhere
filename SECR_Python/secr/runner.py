"""Wiring shared by the command line (``python -m secr``) and the Automation Anywhere entry (``aa_entry.py``)."""
from __future__ import annotations

import logging
import os
import socket
import uuid
from pathlib import Path
import platform
import sys
from typing import Iterable

from .batch import BatchProcessor
from .config import AppConfig, min_db_connections
from .daisy import DaisyFlow
from .db import EsgDb
from .loader import StagingLoader
from .logging_setup import get_logger, setup_logging
from .models import BatchResult
from .normalize import NormalizeFlow
from .notifier import Notifier
from .orchestrator import exit_code, run_batches, run_pending, summarize


def resolve_dir(value, base: Path | None = None) -> Path:
    """Expand %TEMP% / ~ and make a relative folder relative to ``base`` (the config file's folder)."""
    p = Path(os.path.expanduser(os.path.expandvars(str(value).strip())))
    if not p.is_absolute() and base is not None:
        p = base / p
    return p.resolve()


def apply_overrides(cfg: AppConfig, max_batches: int | None = None, max_files: int | None = None,
                    force: bool = False, work_dir=None, log_dir=None, base: Path | None = None) -> None:
    """Values passed by the caller (AA dictionary / command line) win over config.yaml."""
    if work_dir:
        cfg.run.work_dir = resolve_dir(work_dir, base)
    if log_dir:
        cfg.run.log_dir = resolve_dir(log_dir, base)
    if force:  # process every file of the batch again, also those already completed
        cfg.run.skip_completed_files = False
        cfg.run.take_in_progress_batches = True  # and take a batch even if it is IN_PROGRESS
    if max_batches:
        cfg.run.max_parallel_batches = int(max_batches)
    if max_files:
        cfg.run.max_parallel_files_per_batch = int(max_files)
    # Keep the configured cap; only raise it to the deadlock-free minimum if more batches were requested.
    needed = min_db_connections(cfg)
    if cfg.run.max_db_connections < needed:
        get_logger(__name__).warning(
            "max_db_connections %d is too low for %d parallel batch(es); raised to %d",
            cfg.run.max_db_connections, cfg.run.max_parallel_batches, needed)
        cfg.run.max_db_connections = needed


def build(cfg: AppConfig, debug: bool = False) -> tuple[BatchProcessor, EsgDb, Notifier, str]:
    """Set up logging and all services. Returns (processor, db, notifier, log_file)."""
    _, _, log_file = setup_logging(cfg.run.log_dir, logging.DEBUG if debug else logging.INFO)
    log = get_logger(__name__)
    _log_settings(cfg, log)
    notifier = Notifier(cfg.notify, log)
    db = EsgDb(cfg.db, cfg.run.max_db_connections)
    loader = StagingLoader(cfg.run.created_by, cfg.run.serialize_staging_loads,
                           cfg.run.staging_lock_timeout_seconds, cfg.run.insert_chunk_size, log,
                           clear_staging=cfg.run.clear_staging_before_load, db=db,
                           insert_connections=cfg.run.insert_connections,
                           parallel_insert_min_rows=cfg.run.parallel_insert_min_rows)

    # Created separately so a Daisy-only setup does not need S3/Odin settings and vice versa.
    normalize = daisy = None
    try:
        from .ai_client import OdinClient
        from .storage import S3Storage

        normalize = NormalizeFlow(db, S3Storage(cfg.s3), OdinClient(cfg.odin), loader,
                                  cfg.odin.poll_interval_seconds, cfg.odin.max_poll_attempts,
                                  cfg.odin.max_parallel_sheets, cfg.odin.max_parallel_requests)
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
                               status_on_file_errors=cfg.run.batch_status_on_file_errors,
                               status_in_progress=cfg.run.batch_status_in_progress)
    processor.claim_batch_procedure = cfg.run.claim_batch_procedure
    processor.claimable_statuses = tuple(cfg.run.claimable_statuses)
    processor.take_in_progress = cfg.run.take_in_progress_batches
    apply_claim_tracking(processor, cfg, db, log)  # ClaimedAt / ClaimedBy, if sql/AddClaimColumns.sql was run
    if processor.track_claims:  # a batch left IN_PROGRESS by a process that died must not block this run
        try:
            released = release_stale(processor, cfg, db)
            if released:
                log.warning("Released %d stale claim(s): %s", len(released),
                            ", ".join(str(x["BatchId"]) for x in released))
        except Exception as exc:
            log.warning("Could not release stale claims: %s", exc)
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
    log.info("Bulk insert: chunks of %d, files with >= %d rows split over up to %d connection(s)",
             r.insert_chunk_size, r.parallel_insert_min_rows, r.insert_connections)
    log.info("Rerun: skip files already in %s: %s; clear earlier staging rows before load: %s",
             "/".join(r.completed_statuses), r.skip_completed_files, r.clear_staging_before_load)
    log.info("Odin polling: every %ss, up to %d attempts; up to %d sheet(s) per file in parallel, max %d "
             "Odin call(s) in flight; source environment: %s", cfg.odin.poll_interval_seconds,
             cfg.odin.max_poll_attempts, cfg.odin.max_parallel_sheets, cfg.odin.max_parallel_requests,
             cfg.sources.environment)
    log.info("Secrets: %s", ", ".join(f"{v}={'set' if os.environ.get(v) else 'NOT SET'}" for v in _SECRET_VARS))


def apply_claim_tracking(processor: BatchProcessor, cfg: AppConfig, db: EsgDb, log) -> bool:
    """Switch on ClaimedAt / ClaimedBy handling when sql/AddClaimColumns.sql has been run."""
    r = cfg.run
    if not r.batch_status_in_progress:
        return False
    try:
        with db.session() as s:
            available = s.has_claim_columns()
    except Exception as exc:
        log.warning("Could not check for the claim columns: %s", exc)
        available = False
    if not available:
        log.warning("FileUploadBatch has no ClaimedAt/ClaimedBy/ClaimPreviousStatusId columns: stale IN_PROGRESS "
                    "batches cannot be detected (run sql/AddClaimColumns.sql)")
        return False
    processor.track_claims = True
    processor.claim_owner = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"[:100]
    processor.heartbeat_seconds = max(5, r.claim_heartbeat_seconds)
    if r.stale_claim_minutes and r.stale_claim_minutes * 60 < 3 * processor.heartbeat_seconds:
        log.warning("stale_claim_minutes (%d) is short compared with claim_heartbeat_seconds (%d): running "
                    "batches could be released", r.stale_claim_minutes, processor.heartbeat_seconds)
    log.info("Claim tracking on: owner %s, heartbeat every %ds, stale after %s", processor.claim_owner,
             processor.heartbeat_seconds, f"{r.stale_claim_minutes} min" if r.stale_claim_minutes else "never")
    return True


def release_stale(processor: BatchProcessor, cfg: AppConfig, db: EsgDb) -> list:
    """Put IN_PROGRESS batches whose claim went stale back to their previous status."""
    r = cfg.run
    if not (processor.track_claims and r.stale_claim_minutes):
        return []
    fallback = r.claimable_statuses[0] if r.claimable_statuses else "PROCESSING"
    with db.session() as s:
        if r.release_stale_procedure:
            return s.release_stale_via_proc(r.release_stale_procedure, r.batch_status_in_progress,
                                            r.stale_claim_minutes, fallback)
        return s.release_stale_claims(r.batch_status_in_progress, r.stale_claim_minutes, fallback)


def pending_batch_ids(cfg: AppConfig, db: EsgDb, limit: int | None = None) -> list[str]:
    """Batch ids returned by run.pending_batches_query (first column, or the BatchId column)."""
    log = get_logger(__name__)
    if not cfg.run.pending_batches_query:
        raise ValueError("run.pending_batches_query is not set in config.yaml")
    with db.session() as s:
        rows = s.query(cfg.run.pending_batches_query)
    ids = [str(r.get("BatchId") or next(iter(r.values()))) for r in rows]
    if limit and len(ids) > limit:
        log.info("%d pending batch(es) found; taking the first %d", len(ids), limit)
        ids = ids[:limit]
    log.info("Pending batches: %s", ", ".join(ids) or "none")
    return ids


def make_claimer(cfg: AppConfig, db: EsgDb, processor: BatchProcessor):
    """fetch_new(n, exclude) for the pending loop: claims up to n pending batches atomically."""
    log = get_logger(__name__)
    r = cfg.run
    state = {"warned": False}

    owner = processor.claim_owner if processor.track_claims else None

    def fetch_new(n: int, exclude: set) -> list[str]:
        if n <= 0:
            return []
        try:  # batches left IN_PROGRESS by a process that died become pending again
            released = release_stale(processor, cfg, db)
            if released:
                log.warning("Released %d stale claim(s): %s", len(released),
                            ", ".join(str(x["BatchId"]) for x in released))
        except Exception as exc:
            log.warning("Could not release stale claims: %s", exc)
        if r.claim_procedure:  # one database call claims them all
            with db.session() as s:
                claimed = s.claim_pending_via_proc(r.claim_procedure, n, r.claimable_statuses,
                                                   r.batch_status_in_progress, exclude, owner or "")
            for b, previous in claimed:
                processor.note_claimed(b, previous)
            return [b for b, _ in claimed]

        candidates = [b for b in pending_batch_ids(cfg, db) if b not in exclude]
        if not r.batch_status_in_progress:  # nothing to claim with: rely on the per-batch DB lock only
            return candidates[:n]
        claimed = []
        with db.session() as s:
            if not s.batch_status_exists(r.batch_status_in_progress):
                if not state["warned"]:
                    log.warning("%s is not in RefBatchStatus: batches cannot be claimed atomically; only the "
                                "per-batch DB lock protects them", r.batch_status_in_progress)
                    state["warned"] = True
                return candidates[:n]
            for b in candidates:
                previous = s.claim_batch(b, r.claimable_statuses, r.batch_status_in_progress, owner,
                                        procedure=r.claim_batch_procedure)
                if previous is None:
                    log.info("Batch %s was taken by another system (or is no longer pending); skipped", b)
                    continue
                processor.note_claimed(b, previous)
                claimed.append(b)
                if len(claimed) >= n:
                    break
        if claimed:
            log.info("Claimed batch(es) %s", ", ".join(claimed))
        return claimed

    return fetch_new


def execute_pending(processor: BatchProcessor, notifier: Notifier, cfg: AppConfig, db: EsgDb,
                    initial_ids: Iterable = (), max_batches: int = 0) -> tuple[list[BatchResult], int, str]:
    """Pending mode: claim pending batches and keep every free slot busy until nothing is pending."""
    log = get_logger(__name__)
    r = cfg.run
    log.info("Run started in pending mode%s", f" (plus batch id(s) {', '.join(map(str, initial_ids))})"
             if initial_ids else "")
    results = run_pending(processor.process, make_claimer(cfg, db, processor), r.max_parallel_batches,
                          poll_seconds=r.pending_poll_seconds,
                          max_runtime_seconds=r.pending_max_runtime_minutes * 60,
                          max_batches=max_batches, initial_ids=initial_ids)
    summary = summarize(results)
    log.info("Summary\n%s", summary)
    code = exit_code(results)
    log.info("Run finished with exit code %d (0 = all OK, 2 = some files failed, 1 = a batch failed)", code)
    if results:
        notifier.send(f"SECR normalizer {'completed' if code == 0 else 'finished with errors'}", summary)
    return results, code, summary


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
