"""The sub task itself: submit every split sheet to the Odin agent and collect the CSV links.

Bot (ST_GetAIResponse_ParallelExecution)            Python
-----------------------------------------------     -------------------------------------------------
create chat + send file, one sheet after another     up to ``max_parallel`` sheets submitted at once
poll ONE chat until it answers (15 x 40 s), then     every round polls ALL waiting chats in parallel,
the next chat -> worst case 10 min per sheet         then waits 40 s -> worst case ~10 min in total
roll-up status per FileID when the FileID changes    roll-up per FileID after all its sheets are done

Input  (ioFileToNormalizePathTable): rows of FileID + SplittedFilePath (one row per split sheet).
Output (tNormalizedCSVFileLinkTable): rows of FileID, NormalizedCSVLink, FileName (+ Category).
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

from .logging_setup import get_logger, log_context
from .odin_client import CATEGORY_DISPLAY_NAMES, OdinClient
from .status_db import NORMALIZING, roll_up

LOG = get_logger(__name__)


@dataclass
class SheetJob:
    file_id: str
    path: Path
    chat_id: str = ""
    links: dict[str, str] = field(default_factory=dict)
    error: str = ""
    answered: bool = False
    polls: int = 0

    @property
    def file_name(self) -> str:
        return self.path.name


@dataclass
class FileSummary:
    file_id: str
    status: str
    sheets: int
    success: int
    failed: int
    description: str


@dataclass
class RunResult:
    links: list[dict[str, str]]
    files: list[FileSummary]
    seconds: float


class AIResponseProcessor:
    def __init__(self, client: OdinClient, status_db, max_parallel: int, poll_interval: int,
                 max_poll_attempts: int, sleep=time.sleep):
        self.client = client
        self.db = status_db
        self.max_parallel = max_parallel
        self.poll_interval = poll_interval
        self.max_poll_attempts = max_poll_attempts
        self._sleep = sleep

    def run(self, jobs: list[SheetJob]) -> RunResult:
        started = time.monotonic()
        file_ids = list(dict.fromkeys(j.file_id for j in jobs))
        LOG.info("%d sheet(s) from %d file(s) to normalize, up to %d in parallel",
                 len(jobs), len(file_ids), self.max_parallel)
        for j in jobs:
            LOG.debug("  file %s: %s", j.file_id, j.path)

        self._submit_all(jobs)
        self._poll_all(jobs)
        links, files = self._finish(jobs, file_ids)

        seconds = round(time.monotonic() - started, 1)
        LOG.info("Done in %.1fs: %d CSV link(s); files: %s", seconds, len(links),
                 ", ".join(f"{f.file_id}={f.status}" for f in files) or "none")
        return RunResult(links, files, seconds)

    # ------------------------------------------------------------ step 1: submit
    def _submit_all(self, jobs: list[SheetJob]) -> None:
        LOG.info("Step 1/3 submit %d sheet(s) - started", len(jobs))
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=max(1, min(self.max_parallel, len(jobs) or 1)),
                                thread_name_prefix="submit") as pool:
            list(pool.map(self._submit_one, jobs))
        marked = set()
        for job in jobs:  # NORMALIZING once per FileID, as soon as one of its sheets was accepted
            if job.chat_id and job.file_id not in marked:
                with log_context(file_id=job.file_id):
                    self.db.set_status(job.file_id, NORMALIZING)
                marked.add(job.file_id)
        ok = sum(1 for j in jobs if j.chat_id)
        LOG.info("Step 1/3 submit - done in %.1fs: %d submitted, %d failed", time.monotonic() - started,
                 ok, len(jobs) - ok)

    def _submit_one(self, job: SheetJob) -> None:
        with log_context(file_id=job.file_id, sheet=job.file_name):
            try:
                if not job.path.is_file():
                    raise FileNotFoundError(f"File not found: {job.path}")
                job.chat_id = self.client.create_chat(job.file_name)
                self.client.send_file(job.chat_id, job.path)
                LOG.info("Submitted, chat id %s", job.chat_id)
            except Exception as exc:
                job.chat_id = ""
                job.error = f"Chat was not created or the message was not sent for the file {job.file_name} ({exc})."
                LOG.error("Submit failed: %s", exc)

    # ------------------------------------------------------------ step 2: poll
    def _poll_all(self, jobs: list[SheetJob]) -> None:
        pending = [j for j in jobs if j.chat_id]
        if not pending:
            LOG.warning("Step 2/3 poll - skipped, no sheet was submitted")
            return
        LOG.info("Step 2/3 poll %d chat(s) - started (every %ss, up to %d attempts)", len(pending),
                 self.poll_interval, self.max_poll_attempts)
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=max(1, min(self.max_parallel, len(pending))),
                                thread_name_prefix="poll") as pool:
            for attempt in range(1, self.max_poll_attempts + 1):
                LOG.debug("Poll round %d/%d: %d chat(s) waiting", attempt, self.max_poll_attempts, len(pending))
                list(pool.map(lambda j: self._poll_one(j, attempt), pending))
                pending = [j for j in pending if not j.answered]
                if not pending:
                    break
                LOG.info("Poll round %d/%d: %d chat(s) still waiting: %s", attempt, self.max_poll_attempts,
                         len(pending), ", ".join(j.file_name for j in pending))
                if attempt < self.max_poll_attempts:
                    self._sleep(self.poll_interval)
        for job in pending:
            job.error = f"Time out while processing the file {job.file_name}."
            with log_context(file_id=job.file_id, sheet=job.file_name):
                LOG.error("Chat %s passed the allowed time for retrieving the response (%d polls)",
                          job.chat_id, job.polls)
        LOG.info("Step 2/3 poll - done in %.1fs: %d answered, %d timed out", time.monotonic() - started,
                 sum(1 for j in jobs if j.answered), len(pending))

    def _poll_one(self, job: SheetJob, attempt: int) -> None:
        with log_context(file_id=job.file_id, sheet=job.file_name):
            job.polls = attempt
            try:
                links = self.client.get_csv_links(job.chat_id)
            except Exception as exc:  # a transient HTTP error: try again in the next round
                LOG.warning("Poll %d failed (will retry): %s", attempt, exc)
                return
            if links is None:
                LOG.debug("Poll %d: agent still working", attempt)
                return
            job.answered = True
            job.links = links
            if links:
                LOG.info("Answered after poll %d: %s", attempt, ", ".join(links))
            else:
                job.error = f"The agent answered without any CSV link for the file {job.file_name}."
                LOG.warning("Answered after poll %d but without any CSV link", attempt)

    # ------------------------------------------------------------ step 3: results + status
    def _finish(self, jobs: list[SheetJob], file_ids: list[str]) -> tuple[list[dict], list[FileSummary]]:
        LOG.info("Step 3/3 write results and statuses - started")
        links: list[dict[str, str]] = []
        files: list[FileSummary] = []
        for file_id in file_ids:
            with log_context(file_id=file_id):
                own = [j for j in jobs if j.file_id == file_id]
                success = failed = 0
                errors = []
                categories: list[str] = []
                for job in own:
                    for category, link in job.links.items():
                        links.append({"FileID": file_id, "NormalizedCSVLink": link,
                                      "FileName": job.file_name, "Category": category})
                        success += 1
                        if category not in categories:
                            categories.append(category)
                    if not job.links:
                        failed += 1
                        errors.append(job.error or f"No result for the file {job.file_name}.")
                for category in categories:
                    self.db.add_category(file_id, CATEGORY_DISPLAY_NAMES.get(category, category))
                status = roll_up(success, failed)
                description = "".join(f" {e}" for e in errors)
                self.db.set_status(file_id, status, description or None)
                LOG.info("Final status %s (%d sheet(s): %d CSV link(s), %d failed sheet(s))",
                         status, len(own), success, failed)
                files.append(FileSummary(file_id, status, len(own), success, failed, description.strip()))
        LOG.info("Step 3/3 write results and statuses - done")
        return links, files
