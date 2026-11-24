"""Per-sheet outcome of the Normalize flow, kept in UploadedFile.SheetResults (sql/AddSheetResultsColumn.sql).

One JSON document per uploaded file lists every sheet that came out of the workbook split: COMPLETED when
all its normalized CSVs were loaded, otherwise FAILED with the step (``stage``) and the ``reason``. A rerun of
the same file version skips the COMPLETED sheets, so only the failed ones go to the AI agent and are loaded
again (see ``NormalizeFlow``). A list (not a map) is stored so MySQL's JSON_TABLE can query it directly.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime

COMPLETED = "COMPLETED"
FAILED = "FAILED"

# Where a FAILED sheet / category stopped.
STAGE_SPLIT = "split"        # the sheet could not be copied out of the workbook
STAGE_SUBMIT = "submit"      # chat not created / sheet not uploaded to the AI agent
STAGE_AI = "ai"              # agent timed out or answered without a CSV link
STAGE_DOWNLOAD = "download"  # normalized CSV could not be downloaded
STAGE_LOAD = "load"          # staging insert or a stored procedure failed

MAX_REASON = 1000
_URL_QUERY = re.compile(r"(https?://[^\s?'\"]+)\?[^\s'\")]*")
_TOKEN_PARAM = re.compile(r"[?&](?:token|sig|signature|x-amz-[a-z-]+)=[^\s'\")]*", re.IGNORECASE)


def redact(text) -> str:
    """Drop URL query strings: Odin download links carry a signed token that must not be stored."""
    return _TOKEN_PARAM.sub("", _URL_QUERY.sub(r"\1", str(text or "")))


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _reason(text) -> str:
    return redact(text).strip()[:MAX_REASON]


@dataclass
class CategoryRecord:
    category: str
    status: str = FAILED
    rows: int = 0
    stage: str = ""
    reason: str = ""

    @property
    def ok(self) -> bool:
        return self.status == COMPLETED

    def to_dict(self) -> dict:
        out = {"category": self.category, "status": self.status, "rows": self.rows}
        if not self.ok:
            out.update(stage=self.stage, reason=self.reason)
        return out

    @classmethod
    def from_dict(cls, d: dict) -> "CategoryRecord":
        return cls(str(d.get("category") or ""), str(d.get("status") or FAILED), int(d.get("rows") or 0),
                   str(d.get("stage") or ""), str(d.get("reason") or ""))


@dataclass
class SheetRecord:
    sheet: str
    file: str = ""
    status: str = FAILED
    stage: str = ""
    reason: str = ""
    attempts: int = 0
    updated_at: str = ""
    categories: dict[str, CategoryRecord] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.status == COMPLETED

    def fail(self, stage: str, reason) -> None:
        self.status, self.stage, self.reason, self.updated_at = FAILED, stage, _reason(reason), _now()

    def category_done(self, key: str, rows: int) -> None:
        self.categories[key] = CategoryRecord(key, COMPLETED, int(rows))

    def category_failed(self, key: str, stage: str, reason) -> None:
        self.categories[key] = CategoryRecord(key, FAILED, 0, stage, _reason(reason))

    def finish(self) -> None:
        """COMPLETED when every category was loaded, else FAILED with the first failed category's reason."""
        failed = [c for c in self.categories.values() if not c.ok]
        if self.categories and not failed:
            self.status, self.stage, self.reason = COMPLETED, "", ""
        elif failed:
            first = failed[0]
            self.status, self.stage = FAILED, first.stage
            self.reason = _reason(f"{first.category}: {first.reason}")
        self.updated_at = _now()

    def to_dict(self) -> dict:
        return {"sheet": self.sheet, "file": self.file, "status": self.status, "stage": self.stage,
                "reason": self.reason, "attempts": self.attempts, "updated_at": self.updated_at,
                "categories": [c.to_dict() for c in self.categories.values()]}

    @classmethod
    def from_dict(cls, d: dict) -> "SheetRecord":
        cats = [CategoryRecord.from_dict(c) for c in (d.get("categories") or []) if isinstance(c, dict)]
        return cls(str(d.get("sheet") or ""), str(d.get("file") or ""), str(d.get("status") or FAILED),
                   str(d.get("stage") or ""), str(d.get("reason") or ""), int(d.get("attempts") or 0),
                   str(d.get("updated_at") or ""), {c.category: c for c in cats if c.category})


@dataclass
class FileSheetResults:
    file_version: str = ""
    sheets: dict[str, SheetRecord] = field(default_factory=dict)  # by sheet name, in split order

    @classmethod
    def parse(cls, text: str | None) -> "FileSheetResults":
        """Read the stored JSON; anything unreadable counts as 'no earlier results'."""
        if not text:
            return cls()
        data = json.loads(text) if isinstance(text, str) else text
        sheets = [SheetRecord.from_dict(s) for s in (data.get("sheets") or []) if isinstance(s, dict)]
        return cls(str(data.get("file_version") or ""), {s.sheet: s for s in sheets if s.sheet})

    def completed(self) -> dict[str, SheetRecord]:
        return {name: rec for name, rec in self.sheets.items() if rec.ok}

    def record(self, sheet: str, file: str) -> SheetRecord:
        """The record for ``sheet``, attempt counter raised: earlier category results are kept, so the
        categories that were already loaded are not loaded again."""
        rec = self.sheets.get(sheet) or SheetRecord(sheet)
        rec.file = file or rec.file
        rec.attempts += 1
        rec.updated_at = _now()
        self.sheets[sheet] = rec
        return rec

    def to_json(self) -> str:
        return json.dumps({"file_version": self.file_version, "updated_at": _now(),
                           "sheets": [s.to_dict() for s in self.sheets.values()]}, ensure_ascii=False)
