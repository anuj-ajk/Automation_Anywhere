"""Normalize flow for one uploaded file (port of MainTask_Normalize_RawData + its sub tasks).

The AA bot ran each stage for ALL files of a batch before moving to the next stage
(download all -> split all -> submit all -> poll one by one -> load all). Here every file
runs its own pipeline in a worker thread, so a slow AI response for one file no longer
holds up the others, and the sheets of one file are polled together.

Status flow per file (unchanged):
    PARSING -> PARSED -> NORMALIZING -> NORMALIZATION_COMPLETED / PARTIAL_FAILURE /
    FAILED_WHILE_NORMALIZATION -> COMPLETED / PARTIAL_FAILURE / FAILED
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import status as st
from .ai_client import CATEGORY_DISPLAY_NAMES, OdinClient
from .db import EsgDb
from .downloader import download_link, filename_from_url
from .excel_split import split_workbook
from .loader import NORMALIZED_SPECS, StagingLoader
from .logging_setup import ContextLogger
from .models import BatchContext, FileOutcome
from .storage import S3Storage


@dataclass
class _Sheet:
    name: str
    path: Path
    chat_id: str = ""
    links: dict[str, str] = field(default_factory=dict)
    error: str = ""


class NormalizeFlow:
    def __init__(self, db: EsgDb, storage: S3Storage, ai: OdinClient, loader: StagingLoader,
                 poll_interval: int, max_poll_attempts: int):
        self.db = db
        self.storage = storage
        self.ai = ai
        self.loader = loader
        self.poll_interval = poll_interval
        self.max_poll_attempts = max_poll_attempts

    # ------------------------------------------------------------------ public
    def run_file(self, ctx: BatchContext, row: dict[str, Any], log: ContextLogger) -> FileOutcome:
        file_id = str(row["FileId"])
        file_name = str(row.get("FileName") or f"file_{file_id}")
        log = log.child(file_id=file_id)
        work = ctx.work_dir / f"file_{file_id}"
        try:
            return self._run(ctx, row, file_id, file_name, work, log)
        except Exception as exc:  # anything unexpected: fail this file only, never the batch
            log.exception("Unexpected error")
            self._status(file_id, st.FAILED, f"Unexpected error: {exc}", log=log)
            return FileOutcome(file_id, file_name, "normalize", st.FAILED, str(exc))

    # ------------------------------------------------------------------ steps
    def _run(self, ctx, row, file_id, file_name, work: Path, log) -> FileOutcome:
        # 1. Download from S3
        self._status(file_id, st.PARSING, log=log)
        try:
            local = self.storage.download(str(row.get("StoragePath") or ""),
                                          work / "source" / Path(file_name).name)
            log.info("Downloaded %s", row.get("StoragePath"))
        except Exception as exc:
            msg = f"Unable to download the file {row.get('StoragePath')} from S3: {exc}"
            log.error(msg)
            self._status(file_id, st.FAILED, msg, log=log)
            return FileOutcome(file_id, file_name, "normalize", st.FAILED, msg)

        # 2. Split into one workbook per sheet (non-Excel files pass through)
        try:
            split = split_workbook(local, work / "sheets")
        except Exception as exc:
            msg = f"Unable to split the file: {exc}"
            log.error(msg)
            self._status(file_id, st.FAILED, msg, log=log)
            return FileOutcome(file_id, file_name, "normalize", st.FAILED, msg)
        for bad in (s for s in split if not s.ok):
            log.warning("Sheet '%s' could not be split: %s", bad.sheet_name, bad.error)
        sheets = [_Sheet(s.sheet_name, s.file_path) for s in split if s.ok]
        self._status(file_id, st.PARSED, log=log)
        log.info("%d sheet(s) to normalize", len(sheets))

        # 3. AI normalization: submit every sheet, then poll all chats together
        self._submit(file_id, sheets, log)
        self._poll(sheets, log)
        ai_ok = [s for s in sheets if s.links]
        ai_failed = [s for s in sheets if not s.links]
        ai_errors = "".join(f" {s.name}: {s.error or 'no CSV link returned'}." for s in ai_failed)
        ai_status = st.roll_up(len(ai_ok), len(ai_failed), all_ok=st.NORMALIZATION_COMPLETED,
                               none_ok=st.FAILED_WHILE_NORMALIZATION)
        self._status(file_id, ai_status, ai_errors or None, append=True, log=log)
        for key in sorted({k for s in ai_ok for k in s.links}):
            self._db(lambda s, k=key: s.add_recognized_category(file_id, CATEGORY_DISPLAY_NAMES[k]), log)
        if not ai_ok:
            return FileOutcome(file_id, file_name, "normalize", ai_status, ai_errors.strip())

        # 4. Download each normalized CSV and load it
        loaded, load_failed, load_errors = 0, 0, []
        for idx, sheet in enumerate(ai_ok, 1):
            for key, link in sheet.links.items():
                try:
                    csv_path = work / "normalized" / f"{idx:02d}_{key}_{filename_from_url(link)}"
                    download_link(link, csv_path)
                    with self.db.session() as s:
                        result = self.loader.load(s, NORMALIZED_SPECS[key], csv_path, work / "cleaned",
                                                  file_id, ctx.report_id)
                    loaded += 1
                    log.info("Loaded %s (%s): %d row(s)", sheet.name, key, result.rows_inserted)
                except Exception as exc:
                    load_failed += 1
                    load_errors.append(f" Error: {exc} while processing {sheet.name} ({key}).")
                    log.error("Load failed for %s (%s): %s", sheet.name, key, exc)

        # Unlike the bot, a sheet that already failed in the AI step keeps the file out of COMPLETED.
        final = st.roll_up(loaded, load_failed + len(ai_failed), all_ok=st.COMPLETED, none_ok=st.FAILED)
        description = "".join(load_errors) or None
        self._status(file_id, final, description, append=True, log=log)
        log.info("Final status %s (loaded %d, failed %d)", final, loaded, load_failed + len(ai_failed))
        return FileOutcome(file_id, file_name, "normalize", final, (ai_errors + "".join(load_errors)).strip())

    def _submit(self, file_id: str, sheets: list[_Sheet], log) -> None:
        marked = False
        for sheet in sheets:
            try:
                sheet.chat_id = self.ai.create_chat(sheet.path.name)
                accepted = self.ai.send_file(sheet.chat_id, sheet.path)
                if not accepted:
                    log.warning("Upload of %s timed out; assuming it was accepted and polling anyway", sheet.name)
                if not marked:
                    self._status(file_id, st.NORMALIZING, log=log)
                    marked = True
                log.info("Sheet %s submitted, chat id %s", sheet.name, sheet.chat_id)
            except Exception as exc:
                sheet.chat_id = ""
                sheet.error = f"chat could not be created or message not sent ({exc})"
                log.error("Sheet %s: %s", sheet.name, sheet.error)

    def _poll(self, sheets: list[_Sheet], log) -> None:
        pending = [s for s in sheets if s.chat_id]
        for attempt in range(1, self.max_poll_attempts + 1):
            for sheet in list(pending):
                try:
                    links = self.ai.get_csv_links(sheet.chat_id)
                except Exception as exc:  # transient HTTP errors: try again next round
                    log.warning("Poll %d for %s failed: %s", attempt, sheet.name, exc)
                    continue
                if links is not None:
                    sheet.links = links
                    if not links:
                        sheet.error = "agent answered without any CSV link"
                    pending.remove(sheet)
                    log.info("Sheet %s normalized: %s", sheet.name, ", ".join(links) or "no categories")
            if not pending:
                return
            if attempt < self.max_poll_attempts:
                time.sleep(self.poll_interval)
        for sheet in pending:
            sheet.error = "timed out waiting for the AI response"
            log.warning("Chat %s for %s timed out", sheet.chat_id, sheet.name)

    # ------------------------------------------------------------------ db helpers
    def _status(self, file_id, code, description=None, *, append=False, log=None) -> None:
        self._db(lambda s: s.set_file_status(file_id, code, description, append=append), log)

    def _db(self, action, log) -> None:
        """Status writes must not kill the pipeline; log and carry on (as the bot's nested try blocks did)."""
        try:
            with self.db.session() as s:
                action(s)
        except Exception as exc:
            if log:
                log.error("Unable to update the database: %s", exc)
