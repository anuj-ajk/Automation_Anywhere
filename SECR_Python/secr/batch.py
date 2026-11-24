"""Process one batch (the part of MT Normalizer Uploaded Data between the framework steps).

All files of the batch - however many there are, uploaded and Daisy mixed - go into one
per-batch worker pool of ``max_parallel_files_per_batch`` threads. The Normalize and Daisy
files therefore run side by side instead of one main task after the other.
"""
from __future__ import annotations

import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from . import status as st
from .daisy import DaisyFlow
from .db import EsgDb, is_lock_wait_timeout
from .logging_setup import get_logger, log_context, timed_step
from .models import BatchContext, BatchResult, FileOutcome
from .normalize import NormalizeFlow


# _mark_batch_in_progress result: another system owns the batch, do not process it
CLAIMED_ELSEWHERE = object()
# The FileUploadBatch row is locked by another session (MySQL 1205, e.g. an uncommitted transaction):
# the batch is skipped completely, nothing is read, sent to the AI or loaded.
ROW_LOCKED = object()


class BatchProcessor:
    def __init__(self, db: EsgDb, normalize: NormalizeFlow | None, daisy: DaisyFlow | None,
                 work_root, files_per_batch: int, keep_work_files: bool, notifier=None,
                 skip_statuses=st.DEFAULT_COMPLETED_STATUSES,
                 status_on_file_errors: str = st.BATCH_PARTIAL_FAILURE,
                 status_in_progress: str = st.BATCH_IN_PROGRESS):
        self.db = db
        self.normalize = normalize
        self.daisy = daisy
        self.work_root = work_root
        self.files_per_batch = files_per_batch
        self.keep_work_files = keep_work_files
        self.notifier = notifier
        # Rerun of a batch: files already in one of these statuses are not processed again.
        self.skip_statuses = {str(s).upper() for s in (skip_statuses or ())}
        # Batch status when some files failed; falls back to COMPLETE if the code is not in RefBatchStatus.
        self.status_on_file_errors = (status_on_file_errors or st.BATCH_COMPLETE).strip().upper()
        # Set as soon as the batch is picked up, so it no longer looks pending ("" = leave the status alone).
        self.status_in_progress = (status_in_progress or "").strip().upper()
        # Batches already claimed (set IN_PROGRESS atomically) by the pending loop: batch id -> previous status.
        self._claimed: dict[str, str] = {}
        self._claimed_lock = threading.Lock()
        # Claim tracking (sql/AddClaimColumns.sql), switched on by the runner when the columns exist:
        # ClaimedAt/ClaimedBy are written on claim, refreshed every heartbeat_seconds, emptied at the end.
        self.track_claims = False
        self.claim_owner = ""
        self.heartbeat_seconds = 60
        # Batches run by explicit id are claimed through this stored procedure (e.g. "ClaimBatch") when set.
        # A batch already IN_PROGRESS (another system owns it) is then skipped, unless take_in_progress.
        self.claim_batch_procedure = ""
        self.claimable_statuses: tuple = ()
        self.take_in_progress = False

    def _claim_kw(self, finished: bool) -> dict:
        """Extra set_batch_status arguments: record the claim when it starts, clear it when the batch ends."""
        if not self.track_claims:
            return {}
        return {"clear_claim": True} if finished else {"claim_owner": self.claim_owner}

    def _start_heartbeat(self, batch_id: str, log):
        """Refresh ClaimedAt every heartbeat_seconds while the batch runs. Returns a function that stops it."""
        if not (self.track_claims and self.status_in_progress):
            return lambda: None
        stop = threading.Event()
        context = {"batch_id": batch_id}

        def beat():
            with log_context(**context):
                while not stop.wait(self.heartbeat_seconds):
                    try:
                        with self.db.try_session() as s:  # never wait for a connection just for a heartbeat
                            if s is None:
                                log.debug("Heartbeat skipped: no free DB connection")
                                continue
                            s.heartbeat(batch_id, self.claim_owner)
                        log.debug("Heartbeat: ClaimedAt refreshed")
                    except Exception as exc:
                        log.warning("Heartbeat failed (will retry): %s", exc)

        thread = threading.Thread(target=beat, name=f"b{batch_id}-heartbeat", daemon=True)
        thread.start()

        def stop_heartbeat():
            stop.set()
            thread.join(timeout=5)
        return stop_heartbeat

    def note_claimed(self, batch_id: str, previous_status: str) -> None:
        with self._claimed_lock:
            self._claimed[str(batch_id)] = previous_status

    def _mark_batch_in_progress(self, batch_id: str, log) -> str | None:
        """Set the in-progress status. Returns the previous status code (to put back if the batch does not
        run), or None when nothing was changed."""
        with self._claimed_lock:
            claimed_from = self._claimed.pop(str(batch_id), None)
        if claimed_from is not None:
            log.info("Batch was claimed from %s (already %s)", claimed_from or "<none>", self.status_in_progress)
            return claimed_from
        if not self.status_in_progress:
            return None
        try:
            with self.db.session() as s:
                if not s.batch_status_exists(self.status_in_progress):
                    log.warning("Batch status %s is not in RefBatchStatus (codes: %s); batch status left unchanged",
                                self.status_in_progress, ", ".join(s.batch_status_codes()) or "none")
                    return None
                if self.claim_batch_procedure:
                    claimed = self._claim_through_procedure(s, batch_id, log)
                    if claimed is not None:
                        return claimed
                previous = s.get_batch_status(batch_id)
                if self.claim_batch_procedure and previous.upper() == self.status_in_progress:
                    # Only reached with take_in_progress: put the batch back to a claimable status on failure
                    previous = self.claimable_statuses[0] if self.claimable_statuses else ""
                s.set_batch_status(batch_id, self.status_in_progress, **self._claim_kw(finished=False))
            log.info("Batch status %s -> %s", previous or "<none>", self.status_in_progress)
            return previous
        except Exception as exc:  # a status write must never stop the batch - unless its row is locked
            if is_lock_wait_timeout(exc):
                return ROW_LOCKED
            log.error("Could not set batch status %s: %s", self.status_in_progress, exc)
            return None

    def _claim_through_procedure(self, s, batch_id: str, log):
        """Claim the batch with the ClaimBatch procedure. Returns the previous status when claimed,
        CLAIMED_ELSEWHERE when another system owns it, or None to fall back to setting the status directly."""
        owner = self.claim_owner if self.track_claims else None
        try:
            previous = s.claim_batch(batch_id, self.claimable_statuses, self.status_in_progress, owner,
                                     procedure=self.claim_batch_procedure)
        except Exception as exc:
            if is_lock_wait_timeout(exc):  # setting the status directly would only wait for the same lock
                return ROW_LOCKED
            log.error("%s failed (%s); setting %s directly", self.claim_batch_procedure, exc,
                      self.status_in_progress)
            return None
        if previous is not None:
            log.info("Batch claimed through %s: %s -> %s", self.claim_batch_procedure, previous or "<none>",
                     self.status_in_progress)
            return previous
        current = s.get_batch_status(batch_id)
        if current.upper() == self.status_in_progress and not self.take_in_progress:
            log.warning("Batch is already %s (claimed by another system or run); skipped. Run with force to "
                        "take it anyway", current)
            return CLAIMED_ELSEWHERE
        log.info("Batch status %s is not claimable (%s); setting %s directly for this explicit run",
                 current or "<none>", ", ".join(self.claimable_statuses) or "none", self.status_in_progress)
        return None

    def _restore_batch_status(self, batch_id: str, previous: str | None, log) -> None:
        """Put the status back when the batch did not run, so it is picked up again (e.g. by --pending)."""
        if not previous:
            return
        try:
            with self.db.session() as s:
                s.set_batch_status(batch_id, previous, **self._claim_kw(finished=True))
            log.info("Batch status set back to %s", previous)
        except Exception as exc:
            log.error("Could not set batch status back to %s: %s", previous, exc)

    def _finish_batch_status(self, batch_id: str, state: str, log) -> None:
        code = st.BATCH_COMPLETE
        if state != "SUCCESS" and self.status_on_file_errors != st.BATCH_COMPLETE:
            with self.db.session() as s:
                if s.batch_status_exists(self.status_on_file_errors):
                    code = self.status_on_file_errors
                else:
                    log.warning("Batch status %s is not in RefBatchStatus; using %s instead",
                                self.status_on_file_errors, st.BATCH_COMPLETE)
        with self.db.session() as s:
            s.set_batch_status(batch_id, code, **self._claim_kw(finished=True))
        log.info("Batch status set to %s", code)

    def process(self, batch_id: str) -> BatchResult:
        with log_context(batch_id=batch_id):
            return self._process(batch_id)

    def _process(self, batch_id: str) -> BatchResult:
        log = get_logger(__name__, batch_id=batch_id)
        started = time.monotonic()
        log.info("Batch started")
        # Short folder names: the whole local path must stay under the Windows 260-character limit.
        work_dir = self.work_root / f"b{batch_id}_{datetime.now():%H%M%S%f}"
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
                stop_heartbeat = self._start_heartbeat(batch_id, log)
                try:
                    result = self._process_locked(batch_id, work_dir, log)
                finally:
                    stop_heartbeat()
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
        # First thing once the batch is ours: mark it in progress.
        previous_status = self._mark_batch_in_progress(batch_id, log)
        if previous_status is CLAIMED_ELSEWHERE:
            return BatchResult(batch_id, "SKIPPED", error="already IN_PROGRESS (claimed elsewhere)")
        if previous_status is ROW_LOCKED:
            log.error("Batch skipped: its FileUploadBatch row is locked by another database session (most "
                      "likely a transaction that was never committed, e.g. a Workbench tab). Nothing was "
                      "processed; COMMIT or ROLLBACK that session and run the batch again")
            return BatchResult(batch_id, "LOCKED", error="batch row locked by another DB session (uncommitted "
                                                         "transaction?); skipped, nothing processed")
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
                self._restore_batch_status(batch_id, previous_status, log)
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

            skipped: list[FileOutcome] = []
            if self.skip_statuses:
                todo = []
                for r in rows:
                    code = str(r.get("StatusCode") or "").upper()
                    if code in self.skip_statuses:
                        name = str(r.get("FileName") or "")
                        flow = "daisy" if is_daisy(r) else "normalize"
                        if flow == "daisy":
                            name = f"{st.DAISY_FILE_NAME} ({r.get('CategorySelected')})"
                        skipped.append(FileOutcome(str(r["FileId"]), name, flow, code,
                                                   f"skipped: already {code}"))
                        log.info("File %s (%s) is already %s; skipped", r["FileId"], name, code)
                    else:
                        todo.append(r)
                if skipped:
                    log.info("Rerun: %d file(s) already completed are skipped, %d file(s) to process",
                             len(skipped), len(todo))
                rows = todo
            if not rows:
                log.info("Every file of this batch is already completed; nothing to do")
                with self.db.session() as s:
                    s.set_batch_status(batch_id, st.BATCH_COMPLETE, **self._claim_kw(finished=True))
                return BatchResult(batch_id, "ALREADY_COMPLETED", skipped)

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
            outcomes = skipped + outcomes

            log.info("All files done: %d OK, %d with errors", sum(o.ok for o in outcomes),
                     sum(not o.ok for o in outcomes))
            state = "SUCCESS" if all(o.ok for o in outcomes) else "COMPLETED_WITH_FILE_ERRORS"
            self._finish_batch_status(batch_id, state, log)
            for o in outcomes:
                (log.info if o.ok else log.warning)("File %s [%s] %s -> %s %s", o.file_id, o.flow,
                                                   o.file_name, o.status, o.detail)
            return BatchResult(batch_id, state, outcomes)
        except Exception as exc:
            log.exception("Batch failed")
            self._mark_in_progress_failed(batch_id, f"Batch {batch_id} failed: {exc}", log)
            self._restore_batch_status(batch_id, previous_status, log)
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
