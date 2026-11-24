"""Normalize flow for one uploaded file (port of MainTask_Normalize_RawData + its sub tasks).

The AA bot ran each stage for ALL files of a batch before moving to the next stage
(download all -> split all -> submit all -> poll one by one -> load all). Here every file
runs its own pipeline in a worker thread, so a slow AI response for one file no longer
holds up the others.

Inside one file (same approach as SECR_AIResponse):
  * all sheets are submitted to the AI in parallel (up to ``max_parallel_sheets``);
  * each poll round checks every waiting chat in parallel, then waits ``poll_interval``;
  * the normalized CSVs are downloaded in parallel, then loaded one after the other
    (staging tables are locked per table and earlier rows are cleared first, so loads stay ordered).
A semaphore shared by ALL files and batches caps the Odin calls in flight (``max_parallel_requests``),
so 10 batches x 4 files x N sheets never become hundreds of simultaneous API calls.

Status flow per file (unchanged):
    PARSING -> PARSED -> NORMALIZING -> NORMALIZATION_COMPLETED / PARTIAL_FAILURE /
    FAILED_WHILE_NORMALIZATION -> COMPLETED / PARTIAL_FAILURE / FAILED
"""
from __future__ import annotations

import contextlib
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import progress as pg
from . import sheet_results as sr
from . import status as st
from .ai_client import CATEGORY_DISPLAY_NAMES, OdinClient, is_real_link
from .db import EsgDb
from .downloader import download_link, filename_from_url
from .excel_split import split_workbook
from .loader import NORMALIZED_SPECS, StagingLoader
from .logging_setup import ContextLogger, get_logger, log_context, timed_step
from .models import BatchContext, FileOutcome
from .paths import short_name
from .retry import is_transient_http, retry_call
from .storage import S3Storage


@dataclass
class _Sheet:
    name: str
    path: Path
    chat_id: str = ""
    links: dict[str, str] = field(default_factory=dict)
    error: str = ""
    answered: bool = False


class NormalizeFlow:
    def __init__(self, db: EsgDb, storage: S3Storage, ai: OdinClient, loader: StagingLoader,
                 poll_interval: int, max_poll_attempts: int, max_parallel_sheets: int = 5,
                 max_parallel_requests: int = 20, sleep=time.sleep, submit_attempts: int = 3,
                 submit_retry_backoff: float = 10):
        self.db = db
        self.storage = storage
        self.ai = ai
        self.loader = loader
        self.poll_interval = poll_interval
        self.max_poll_attempts = max_poll_attempts
        self.max_parallel_sheets = max(1, int(max_parallel_sheets))
        # Shared by every file and batch of the run: Odin calls / CSV downloads in flight at once.
        self._api_slots = threading.BoundedSemaphore(max(1, int(max_parallel_requests)))
        self._sleep = sleep
        self.submit_attempts = max(1, int(submit_attempts))
        self.submit_retry_backoff = float(submit_retry_backoff)
        # Rerun: skip the sheets UploadedFile.SheetResults lists as COMPLETED (False = force: redo all).
        self.resume_sheets = True
        # None until the first read: False when UploadedFile.SheetResults does not exist (column not added).
        self._sheet_results_available: bool | None = None
        # UploadedFile.ProgressPercent (shared with the Daisy flow by runner.build)
        self.progress = pg.ProgressReporter(db)

    # ------------------------------------------------------------------ concurrency helpers
    @contextlib.contextmanager
    def _api_call(self):
        self._api_slots.acquire()
        try:
            yield
        finally:
            self._api_slots.release()

    def _parallel(self, items, fn, ctx: BatchContext, file_id: str, label: str) -> list:
        """Run ``fn(item)`` for every item on up to ``max_parallel_sheets`` threads, keeping the
        batch/file log context in the worker threads. Results come back in input order."""
        items = list(items)
        if not items:
            return []

        def run(item):
            with log_context(batch_id=ctx.batch_id, file_id=file_id):
                return fn(item)

        workers = min(self.max_parallel_sheets, len(items))
        if workers == 1:
            return [run(i) for i in items]
        with ThreadPoolExecutor(max_workers=workers,
                                thread_name_prefix=f"b{ctx.batch_id}-f{file_id}-{label}") as pool:
            return list(pool.map(run, items))

    # ------------------------------------------------------------------ public
    def run_file(self, ctx: BatchContext, row: dict[str, Any], log: ContextLogger) -> FileOutcome:
        file_id = str(row["FileId"])
        file_name = str(row.get("FileName") or f"file_{file_id}")
        log = get_logger(__name__, batch_id=ctx.batch_id, file_id=file_id)
        work = ctx.work_dir / f"f{file_id}"
        started = time.monotonic()
        with log_context(batch_id=ctx.batch_id, file_id=file_id):
            log.info("Normalize started for '%s' (category selected: %s)", file_name, row.get("CategorySelected"))
            self.progress.start(file_id)
            try:
                outcome = self._run(ctx, row, file_id, file_name, work, log)
            except Exception as exc:  # anything unexpected: fail this file only, never the batch
                log.exception("Unexpected error")
                self._status(file_id, st.FAILED, f"Unexpected error: {exc}", log=log)
                outcome = FileOutcome(file_id, file_name, "normalize", st.FAILED, str(exc))
            self.progress.set(file_id, pg.DONE, log)  # finished, whatever the outcome
            log.info("Normalize finished for '%s': %s in %.1fs", file_name, outcome.status,
                     time.monotonic() - started)
            return outcome

    # ------------------------------------------------------------------ steps
    def _run(self, ctx, row, file_id, file_name, work: Path, log) -> FileOutcome:
        # 1. Download from S3
        self._status(file_id, st.PARSING, log=log)
        self.progress.set(file_id, pg.S3_DOWNLOADING, log)
        try:
            with timed_step(log, "Step 1/4 download from S3"):
                local = self.storage.download(str(row.get("StoragePath") or ""),
                                              work / "src" / short_name(Path(file_name).name))
            log.info("Downloaded %s (%d bytes)", row.get("StoragePath"), local.stat().st_size)
            self.progress.set(file_id, pg.S3_DOWNLOADED, log)
        except Exception as exc:
            msg = f"Unable to download the file {row.get('StoragePath')} from S3: {exc}"
            log.error(msg)
            self._status(file_id, st.FAILED, msg, log=log)
            return FileOutcome(file_id, file_name, "normalize", st.FAILED, msg)

        # Sheets an earlier run of this file version completed are not split, normalized or loaded again.
        results = self._previous_results(file_id, row, log)
        done = results.completed()
        if done:
            log.info("Rerun: %d sheet(s) already completed in an earlier run are skipped: %s", len(done),
                     ", ".join(done))
        # Staging tables an earlier run loaded for this file: their rows stay (no clear before the retry).
        keep_tables = {NORMALIZED_SPECS[c.category].staging_table for r in results.sheets.values()
                       for c in r.categories.values() if c.ok and c.category in NORMALIZED_SPECS}

        # 2. Split into one workbook per sheet (non-Excel files pass through)
        try:
            with timed_step(log, "Step 2/4 split workbook into sheets"):
                split = split_workbook(local, work / "sheets", skip=done)
        except Exception as exc:
            msg = f"Unable to split the file: {exc}"
            log.error(msg)
            self._status(file_id, st.FAILED, msg, log=log)
            return FileOutcome(file_id, file_name, "normalize", st.FAILED, msg)
        split_errors = ""
        for bad in (s for s in split if not s.ok):
            log.warning("Sheet '%s' could not be split: %s", bad.sheet_name, bad.error)
            results.record(bad.sheet_name, "").fail(sr.STAGE_SPLIT, bad.error)
            split_errors += f" {bad.sheet_name}: could not be split ({bad.error})."
        split_failed = sum(1 for s in split if not s.ok)
        sheets = [_Sheet(s.sheet_name, s.file_path) for s in split if s.ok]
        records = {s.name: results.record(s.name, s.path.name) for s in sheets}
        self._status(file_id, st.PARSED, log=log)
        self.progress.set(file_id, pg.SPLIT_DONE, log)
        log.info("%d sheet(s) to normalize: %s", len(sheets), ", ".join(s.name for s in sheets) or "none")
        if not sheets:  # the data sheets were completed by earlier runs (or only failed to split)
            self._save_results(file_id, results, log)
            final = st.roll_up(len(done), split_failed, all_ok=st.COMPLETED, none_ok=st.FAILED)
            self._status(file_id, final, split_errors or None, append=True, log=log)
            return FileOutcome(file_id, file_name, "normalize", final,
                               split_errors.strip() or f"all {len(done)} sheet(s) already completed")

        # 3. AI normalization: submit every sheet, then poll all chats together
        with timed_step(log, "Step 3/4 AI normalization (submit + poll)"):
            self._submit(ctx, file_id, sheets, log)
            self.progress.set(file_id, pg.SUBMITTED, log)
            self._poll(ctx, file_id, sheets, log)
        self.progress.set(file_id, pg.AI_DONE, log)
        ai_ok = [s for s in sheets if s.links]
        ai_failed = [s for s in sheets if not s.links]
        ai_errors = "".join(f" {s.name}: {s.error or 'no CSV link returned'}." for s in ai_failed)
        for bad in ai_failed:
            records[bad.name].fail(sr.STAGE_AI if bad.chat_id else sr.STAGE_SUBMIT,
                                   bad.error or "no CSV link returned")
        ai_status = st.roll_up(len(ai_ok) + len(done), len(ai_failed), all_ok=st.NORMALIZATION_COMPLETED,
                               none_ok=st.FAILED_WHILE_NORMALIZATION)
        self._status(file_id, ai_status, ai_errors or None, append=True, log=log)
        log.info("AI result: %d sheet(s) OK, %d failed -> %s", len(ai_ok), len(ai_failed), ai_status)
        for bad in ai_failed:
            log.warning("AI failed for sheet '%s': %s", bad.name, bad.error or "no CSV link returned")
        for key in sorted({k for s in ai_ok for k in s.links}):
            self._db(lambda s, k=key: s.add_recognized_category(file_id, CATEGORY_DISPLAY_NAMES[k]), log)
        if not ai_ok:
            log.warning("No sheet was normalized; nothing to load")
            self._save_results(file_id, results, log)
            return FileOutcome(file_id, file_name, "normalize", ai_status, ai_errors.strip())

        # A category of a retried sheet that was loaded in an earlier run is not loaded twice.
        carried = 0
        for sheet in ai_ok:
            rec = records[sheet.name]  # the new answer decides: earlier failures it no longer returns are dropped
            rec.categories = {k: c for k, c in rec.categories.items() if c.ok or k in sheet.links}
            for key in [k for k in sheet.links if k in records[sheet.name].categories
                        and records[sheet.name].categories[k].ok]:
                log.info("Sheet %s: %s was already loaded in an earlier run; not loaded again", sheet.name, key)
                del sheet.links[key]
                carried += 1

        log.info("Step 4/4 download and load %d normalized CSV(s) - started",
                 sum(len(s.links) for s in ai_ok))
        step_started = time.monotonic()

        # 4a. Download every normalized CSV in parallel
        tasks = [(sheet, key, link, work / "csv" / f"{idx:02d}_{key}.csv")
                 for idx, sheet in enumerate(ai_ok, 1) for key, link in sheet.links.items()]

        def download(task):
            sheet, key, link, csv_path = task
            try:
                log.info("Downloading %s CSV for sheet '%s': %s", key, sheet.name, filename_from_url(link))
                with self._api_call():
                    download_link(link, csv_path)
                log.debug("Saved %s (%d bytes)", csv_path, csv_path.stat().st_size)
                return None
            except Exception as exc:
                log.error("Download failed for %s (%s): %s", sheet.name, key, exc)
                return exc

        download_errors = self._parallel(tasks, download, ctx, file_id, "dl")
        self.progress.set(file_id, pg.CSV_DOWNLOADED, log)

        # 4b. Load them one after the other (staging tables are shared and locked per table)
        loaded, load_failed, load_errors = 0, 0, []
        for n, ((sheet, key, _link, csv_path), error) in enumerate(zip(tasks, download_errors)):
            self.progress.set(file_id, pg.between(pg.CSV_DOWNLOADED, pg.DONE - 1, n, len(tasks)), log)
            record = records[sheet.name]
            stage = sr.STAGE_DOWNLOAD if error is not None else sr.STAGE_LOAD
            try:
                if error is not None:
                    raise error
                table = NORMALIZED_SPECS[key].staging_table
                with self.db.session() as s:
                    result = self.loader.load(s, NORMALIZED_SPECS[key], csv_path, work / "cleaned",
                                              file_id, ctx.report_id, clear=table not in keep_tables)
                loaded += 1
                record.category_done(key, result.rows_inserted)
                log.info("Loaded %s (%s): %d row(s)", sheet.name, key, result.rows_inserted)
            except Exception as exc:
                load_failed += 1
                record.category_failed(key, stage, exc)
                load_errors.append(f" Error: {sr.redact(exc)} while processing {sheet.name} ({key}).")
                log.error("Load failed for %s (%s): %s", sheet.name, key, sr.redact(exc))
        for sheet in ai_ok:
            records[sheet.name].finish()

        log.info("Step 4/4 download and load - done in %.1fs (%d loaded, %d failed)",
                 time.monotonic() - step_started, loaded, load_failed)
        self._save_results(file_id, results, log)

        # Unlike the bot, a sheet that already failed in the AI step keeps the file out of COMPLETED.
        # Sheets / categories completed by earlier runs count as loaded.
        ok_total = loaded + carried + sum(max(1, len(r.categories)) for r in done.values())
        # A sheet that could not be split also keeps the file out of COMPLETED, so a rerun retries it.
        failed_total = load_failed + len(ai_failed) + split_failed
        final = st.roll_up(ok_total, failed_total, all_ok=st.COMPLETED, none_ok=st.FAILED)
        description = (split_errors + "".join(load_errors)) or None
        self._status(file_id, final, description, append=True, log=log)
        log.info("Final status %s (loaded %d, failed %d%s)", final, loaded, failed_total,
                 f", {ok_total - loaded} completed earlier" if ok_total > loaded else "")
        return FileOutcome(file_id, file_name, "normalize", final,
                           (split_errors + ai_errors + "".join(load_errors)).strip())

    def _submit(self, ctx: BatchContext, file_id: str, sheets: list[_Sheet], log) -> None:
        """Create a chat and upload the sheet for every sheet, in parallel."""
        def submit(sheet: _Sheet) -> None:
            try:
                log.info("Submitting sheet '%s' (%s) to the AI agent", sheet.name, sheet.path.name)

                def attempt() -> bool:
                    # A retry starts over with a new chat, so a half-sent upload never leaves two
                    # messages (and two answers) in the chat that is polled. The wait between
                    # attempts happens outside the API slots.
                    sheet.chat_id = ""
                    with self._api_call():
                        chat_id = self.ai.create_chat(sheet.path.name)
                    with self._api_call():
                        ok = self.ai.send_file(chat_id, sheet.path)
                    sheet.chat_id = chat_id
                    return ok

                accepted = retry_call(attempt, attempts=self.submit_attempts,
                                      backoff_seconds=self.submit_retry_backoff, is_retryable=is_transient_http,
                                      what=f"Odin submit of sheet '{sheet.name}'", log=log, sleep=self._sleep)
                if not accepted:
                    log.warning("Upload of %s timed out; assuming it was accepted and polling anyway", sheet.name)
                log.info("Sheet %s submitted, chat id %s", sheet.name, sheet.chat_id)
            except Exception as exc:
                sheet.chat_id = ""
                sheet.error = f"chat could not be created or message not sent ({exc})"
                log.error("Sheet %s: %s", sheet.name, sheet.error)

        if len(sheets) > 1:
            log.info("Submitting %d sheet(s), up to %d at a time", len(sheets),
                     min(self.max_parallel_sheets, len(sheets)))
        self._parallel(sheets, submit, ctx, file_id, "ai")
        if any(s.chat_id for s in sheets):  # NORMALIZING once, as soon as one sheet was accepted
            self._status(file_id, st.NORMALIZING, log=log)

    def _poll(self, ctx: BatchContext, file_id: str, sheets: list[_Sheet], log) -> None:
        """Every round polls all waiting chats in parallel, then waits ``poll_interval`` seconds."""
        pending = [s for s in sheets if s.chat_id]
        total = len(pending)
        if not pending:
            log.warning("No sheet was submitted; skipping polling")
            return
        log.info("Polling %d chat(s) every %ss, up to %d attempts", len(pending), self.poll_interval,
                 self.max_poll_attempts)

        def poll(sheet: _Sheet, attempt: int) -> None:
            try:
                with self._api_call():
                    links = self.ai.get_csv_links(sheet.chat_id)
            except Exception as exc:  # transient HTTP errors: try again next round
                log.warning("Poll %d for %s failed: %s", attempt, sheet.name, exc)
                return
            if links is None:
                log.debug("Sheet '%s' (chat %s): agent still working", sheet.name, sheet.chat_id)
                return
            links = {k: v for k, v in links.items() if is_real_link(v)}  # never keep "NA" links
            sheet.links = links
            sheet.answered = True
            if not links:
                sheet.error = "agent answered without any CSV link"
            log.info("Sheet %s normalized after poll %d: %s", sheet.name, attempt,
                     ", ".join(links) or "no categories")

        for attempt in range(1, self.max_poll_attempts + 1):
            log.debug("Poll attempt %d/%d, waiting for: %s", attempt, self.max_poll_attempts,
                      ", ".join(s.name for s in pending))
            self._parallel(pending, lambda sh, a=attempt: poll(sh, a), ctx, file_id, "poll")
            pending = [s for s in pending if not s.answered]
            self.progress.set(file_id, pg.between(pg.SUBMITTED, pg.AI_DONE, total - len(pending), total), log)
            if not pending:
                return
            if attempt % 10 == 0:
                log.info("Still waiting for %d sheet(s) after %d poll(s): %s", len(pending), attempt,
                         ", ".join(s.name for s in pending))
            if attempt < self.max_poll_attempts:
                self._sleep(self.poll_interval)
        for sheet in pending:
            sheet.error = "timed out waiting for the AI response"
            log.warning("Chat %s for %s timed out", sheet.chat_id, sheet.name)

    # ------------------------------------------------------------------ per-sheet results
    def _previous_results(self, file_id: str, row: dict[str, Any], log) -> sr.FileSheetResults:
        """UploadedFile.SheetResults of an earlier run of this file version (empty: process every sheet)."""
        version = str(row.get("FileVersion") or "")
        fresh = sr.FileSheetResults(file_version=version)
        if self._sheet_results_available is False:
            return fresh
        try:
            with self.db.session() as s:
                text = s.get_sheet_results(file_id)
            self._sheet_results_available = True
        except Exception as exc:
            if self._sheet_results_available is None:
                log.warning("UploadedFile.SheetResults cannot be read (%s): sheet results are not kept and a "
                            "rerun processes every sheet (run sql/AddSheetResultsColumn.sql)", exc)
            self._sheet_results_available = False
            return fresh
        if not self.resume_sheets:
            if text:
                log.info("Force: earlier sheet results are ignored, every sheet is processed again")
            return fresh
        try:
            previous = sr.FileSheetResults.parse(text)
        except Exception as exc:
            log.warning("UploadedFile.SheetResults is not valid JSON (%s); every sheet is processed", exc)
            return fresh
        if text and previous.file_version != version:
            log.info("Sheet results are for file version %s, this is version %s; every sheet is processed",
                     previous.file_version or "<none>", version or "<none>")
            return fresh
        previous.file_version = version
        return previous

    def _save_results(self, file_id: str, results: sr.FileSheetResults, log) -> None:
        if not self._sheet_results_available:
            return
        failed = [r.sheet for r in results.sheets.values() if not r.ok]
        log.info("Sheet results saved: %d completed, %d failed%s", len(results.sheets) - len(failed),
                 len(failed), f" ({', '.join(failed)})" if failed else "")
        self._db(lambda s: s.save_sheet_results(file_id, results.to_json()), log)

    # ------------------------------------------------------------------ db helpers
    def _status(self, file_id, code, description=None, *, append=False, log=None) -> None:
        if log:
            log.info("Status -> %s%s", code, f" ({description.strip()})" if description else "")
        self._db(lambda s: s.set_file_status(file_id, code, description, append=append), log)

    def _db(self, action, log) -> None:
        """Status writes must not kill the pipeline; log and carry on (as the bot's nested try blocks did)."""
        try:
            with self.db.session() as s:
                action(s)
        except Exception as exc:
            if log:
                log.error("Unable to update the database: %s", exc)
