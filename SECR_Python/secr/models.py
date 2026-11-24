"""Shared data classes."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class BatchContext:
    batch_id: str
    report_id: str
    client_name: str
    report_start: Any
    report_end: Any
    work_dir: Path


@dataclass
class FileOutcome:
    file_id: str
    file_name: str
    flow: str           # "normalize" or "daisy"
    status: str         # final RefFileProcessingStatus code
    detail: str = ""

    @property
    def ok(self) -> bool:
        from .status import OK_FILE_STATUSES

        return self.status in OK_FILE_STATUSES


@dataclass
class BatchResult:
    batch_id: str
    state: str  # SUCCESS / COMPLETED_WITH_FILE_ERRORS / FAILED / NO_FILES / SKIPPED / LOCKED / ALREADY_COMPLETED
    files: list[FileOutcome] = field(default_factory=list)
    error: str = ""
    seconds: float = 0.0
