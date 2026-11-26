"""UploadedFile.ProgressPercent (sql/AddProgressColumn.sql): an indicative 0-100 progress per file.

Only goes up within a run and ends at 100 whatever the outcome. Writes never stop the pipeline: a missing
column switches the feature off for the rest of the process (one warning), other errors are logged and skipped.
"""
from __future__ import annotations

import threading

from .logging_setup import module_logger

LOG = module_logger(__name__)
_UNKNOWN_COLUMN = 1054

# Normalize flow milestones
PICKED_UP = 25
S3_DOWNLOADING = 30
S3_DOWNLOADED = 40
SPLIT_DONE = 50
SUBMITTED = 60
AI_DONE = 80
CSV_DOWNLOADED = 85
DONE = 100

# Daisy flow milestones
DAISY_QUERYING = 10
DAISY_EXPORTED = 60


def between(start: int, end: int, done: int, total: int) -> int:
    """``start`` .. ``end`` in proportion to ``done`` of ``total`` (never reaches ``end`` before all are done)."""
    if total <= 0:
        return end
    value = start + (end - start) * done // total
    return value if done >= total else min(value, end - 1)


class ProgressReporter:
    def __init__(self, db):
        self.db = db
        self._available: bool | None = None  # False once the column turned out to be missing
        self._last: dict[str, int] = {}
        self._lock = threading.Lock()

    def start(self, file_id) -> None:
        """A new run of this file: progress starts again from 0."""
        with self._lock:
            self._last.pop(str(file_id), None)
        self.set(file_id, PICKED_UP)

    def set(self, file_id, percent: int, log=None) -> None:
        file_id, percent = str(file_id), max(0, min(100, int(percent)))
        with self._lock:
            if self._available is False or self._last.get(file_id, -1) >= percent:
                return  # never goes down, and the same value is not written twice
            self._last[file_id] = percent
        try:
            with self.db.session() as s:
                s.set_file_progress(file_id, percent)
            self._available = True
            (log or LOG).debug("Progress %d%%", percent)
        except Exception as exc:
            code = exc.args[0] if getattr(exc, "args", None) else None
            if code == _UNKNOWN_COLUMN or isinstance(exc, AttributeError):
                if self._available is not False:
                    LOG.warning("UploadedFile.ProgressPercent is not available (%s): progress is not reported "
                                "(run sql/AddProgressColumn.sql)", exc)
                self._available = False
            else:
                (log or LOG).warning("Could not update progress to %d%%: %s", percent, exc)
