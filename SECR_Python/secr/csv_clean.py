"""CSV cleaning before the staging load (port of the bots' ``fill_csv_blanks`` script)."""
from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path

_NEWLINE_RE = re.compile(r"[^\S\r\n]*\r\n|[^\S\r\n]*\r|[^\S\r\n]*\n")
_NEWLINE_COUNT_RE = re.compile(r"\r\n|\r|\n")


@dataclass
class CleanResult:
    output_file: Path
    rows_processed: int  # includes the header row, as in the original script
    blanks_filled: int
    newlines_cleaned: int
    quotes_cleaned: int

    @property
    def data_rows(self) -> int:
        return max(self.rows_processed - 1, 0)


from .logging_setup import module_logger  # noqa: E402

LOG = module_logger(__name__)


def clean_csv(
    input_file: Path,
    output_file: Path,
    file_id: str,
    fill_value: str = "NA",
    newline_replacement: str = " ",
    remove_quotes: bool = True,
) -> CleanResult:
    """Fill blanks with ``fill_value``, flatten newlines, drop double quotes, add a ``FileId`` column."""
    if not input_file.exists():
        raise FileNotFoundError(f"File not found: {input_file}")

    blanks = newlines = quotes = 0
    with input_file.open("r", encoding="utf-8-sig", newline="") as fh:
        rows = list(csv.reader(fh, strict=True))

    for row in rows:
        for i, cell in enumerate(row):
            normalized = _NEWLINE_RE.sub(lambda _m: newline_replacement, cell)
            if normalized != cell:
                newlines += len(_NEWLINE_COUNT_RE.findall(cell))
                row[i] = normalized
            if remove_quotes and '"' in row[i]:
                quotes += row[i].count('"')
                row[i] = row[i].replace('"', "")
            if row[i].strip() == "":
                row[i] = fill_value
                blanks += 1

    if rows:
        rows[0].append("FileId")
        for row in rows[1:]:
            row.append(str(file_id))

    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8", newline="") as fh:
        csv.writer(fh).writerows(rows)

    LOG.debug("Cleaned %s -> %s: %d line(s) incl. header, %d blank(s) filled, %d newline(s), %d quote(s) removed",
              input_file.name, output_file.name, len(rows), blanks, newlines, quotes)
    if len(rows) <= 1:
        LOG.info("%s has a header only (no data rows)", input_file.name)
    return CleanResult(output_file, len(rows), blanks, newlines, quotes)
