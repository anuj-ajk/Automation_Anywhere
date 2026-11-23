"""Process one batch (the part of MT Normalizer Uploaded Data between the framework steps).

All files of the batch - however many there are, uploaded and Daisy mixed - go into one
per-batch worker pool of ``max_parallel_files_per_batch`` threads. The Normalize and Daisy
files therefore run side by side instead of one main task after the other.
"""
from __future__ import annotations

import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from . import status as st
from .daisy import DaisyFlow
from .db import EsgDb
from .logging_setup import get_logger, log_context, timed_step
from .models import BatchContext, BatchResult, FileOutcome
from .normalize import NormalizeFlow


class BatchProcessor:
    def __init__(self, db: EsgDb, normalize: NormalizeFlow | None, daisy: DaisyFlow | None,
                 work_root, files_per_batch: int, keep_work_files: bool, notifier=None):
        self.db = db
        self.normalize = normalize
        self.daisy = daisy
        self.work_root = work_root
        self.files_per_batch = files_per_batch
        self.keep_work_files = keep_work_files
        self.notifier = notifier

    def process(self, batch_id: str) -> BatchResult:
        with log_context(batch_id=batch_id):
            return self._process(batch_id)

    def _process(self, batch_id: str) -> BatchResult:
        log = get_logger(__name__, batch_id=batch_id)
        started = time.monotonic()
        log.info("Batch started")
        work_dir = self.work_root / f"batch_{batch_id}_{datetime.now():%Y%m%d_%H%M%S_%f}"
        lock_name = f"secr_batch_{batch_id}"

        # This connection is held for the whole batch: it owns a MySQL named lock so the same
        # batch is never processed twice at once (other thread, other process or other machine).
        with self.db.session() as lock_session:
            log.debug("Trying DB lock %s", lock_name)
            if not lock_session.try_lock(lock_name, 0):
                log.warning("Batch is already being processed elsewhere; skipped")
                return BatchResult(batch_id, "SKIPPED", error="already running")
            try:
                log.debug("Got DB lock %s; work folder %s", lock_name, work_dir)
                result = self._process_locked(batch_id, work_dir, log)
            finally:
                lock_session.release_lock(lock_name)
                log.debug("Released DB lock %s", lock_name)
                if self.keep_work_files:
                    log.info("Work files kept in %s", work_dir)
                else:
                    shutil.rmtree(work_dir, ignore_errors=True)
                    log.debug("Removed work folder %s", work_dir)
        result.seconds = round(time.monotonic() - started, 1)
        log.info("Batch finished: %s in %.1fs (%d file(s))", result.state, result.seconds, len(result.files))
        return result

    def _process_locked(self, batch_id: str, work_dir, log) -> BatchResult:
        try:
            with timed_step(log, "Read batch files from ESG DB"), self.db.session() as s:
                rows = s.fetch_batch_files(batch_id)
            log.info("%d valid SECR file row(s) found", len(rows))
            for r in rows:
                log.debug("  FileId=%s Category=%s FileName=%s StoragePath=%s", r.get("FileId"),
                          r.get("CategorySelected"), r.get("FileName"), r.get("StoragePath"))
            if not rows:
                # The bot read row [0] without checking and failed here; an empty batch is not an error.
                log.warning("No valid SECR files found for this batch")
                return BatchResult(batch_id, "NO_FILES")

            first = rows[0]
            ctx = BatchContext(
                batch_id=batch_id,
                report_id=str(first.get("ReportId") or ""),
                client_name=str(first.get("BusinessPartnerName") or ""),
                report_start=first.get("ReportStart"),
                report_end=first.get("ReportEnd"),
                work_dir=work_dir,
            )
            def is_daisy(r) -> bool:
                return (r.get("FileName") or "").strip() == st.DAISY_FILE_NAME

            daisy_rows = [r for r in rows if is_daisy(r)]
            upload_rows = [r for r in rows if not is_daisy(r)]
            log.info("Client %s, report %s (%s to %s): %d uploaded file(s), %d Daisy file(s)",
                     ctx.client_name, ctx.report_id, ctx.report_start, ctx.report_end,
                     len(upload_rows), len(daisy_rows))

            jobs = []
            if upload_rows and self.normalize is None:
                raise RuntimeError("Batch has uploaded files but the Normalize flow is not configured")
            if daisy_rows and self.daisy is None:
                raise RuntimeError("Batch has Daisy files but the Daisy flow is not configured")
            jobs += [(self.normalize.run_file, r) for r in upload_rows]
            jobs += [(self.daisy.run_file, r) for r in daisy_rows]

            workers = max(1, min(self.files_per_batch, len(jobs)))
            log.info("Processing %d file(s), up to %d in parallel", len(jobs), workers)
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix=f"b{batch_id}-file") as pool:
                futures = [pool.submit(fn, ctx, row, log) for fn, row in jobs]
                outcomes: list[FileOutcome] = [f.result() for f in futures]

            log.info("All files done: %d OK, %d with errors", sum(o.ok for o in outcomes),
                     sum(not o.ok for o in outcomes))
            with self.db.session() as s:
                s.set_batch_status(batch_id, st.BATCH_COMPLETE)
            log.info("Batch status set to %s", st.BATCH_COMPLETE)

            state = "SUCCESS" if all(o.ok for o in outcomes) else "COMPLETED_WITH_FILE_ERRORS"
            for o in outcomes:
                (log.info if o.ok else log.warning)("File %s [%s] %s -> %s %s", o.file_id, o.flow,
                                                   o.file_name, o.status, o.detail)
            return BatchResult(batch_id, state, outcomes)
        except Exception as exc:
            log.exception("Batch failed")
            self._mark_in_progress_failed(batch_id, f"Batch {batch_id} failed: {exc}", log)
            if self.notifier:
                self.notifier.send(f"SECR batch {batch_id} failed", f"Batch {batch_id} failed:\n{exc}")
            return BatchResult(batch_id, "FAILED", error=str(exc))

    def _mark_in_progress_failed(self, batch_id: str, description: str, log) -> None:
        try:
            with self.db.session() as s:
                for src, dst in ((st.PARSING, st.FAILED), (st.PARSED, st.FAILED),
                                 (st.NORMALIZING, st.FAILED_WHILE_NORMALIZATION)):
                    moved = s.move_batch_files(batch_id, src, dst, description)
                    log.warning("Moved %s in-progress file(s) from %s to %s", moved, src, dst)
        except Exception as exc:
            log.error("Unable to update the status of in-progress files: %s", exc)
