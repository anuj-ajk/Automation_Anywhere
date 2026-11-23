"""Split a workbook into one workbook per sheet (port of ST_SplitSheetsAndCreateIndividualExcel)."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

SPLITTABLE_EXTENSIONS = {".xlsx", ".xlsm"}
# Sheets whose name contains one of these words are not data and are skipped (same list as the bot).
SKIP_SHEET_KEYWORDS = ("instructions", "start", "lookup", "comparison", "original", "qa action")


@dataclass
class SheetResult:
    sheet_name: str
    file_path: Path | None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None


class SplitError(Exception):
    pass


def _safe_name(name: str) -> str:
    return "".join(c for c in name if c.isalnum() or c in (" ", "_", "-")).rstrip() or "Sheet"


from .logging_setup import module_logger  # noqa: E402

LOG = module_logger(__name__)


def split_workbook(source_file: Path, output_dir: Path, prefix: str = "Sheet_") -> list[SheetResult]:
    """Return one result per data sheet.

    Non-Excel files (e.g. CSV) are passed through unchanged as a single result.
    Raises SplitError when the workbook cannot be opened or no sheet could be split.
    """
    if source_file.suffix.lower() not in SPLITTABLE_EXTENSIONS:
        LOG.info("%s is not an Excel workbook; sent to the AI as it is", source_file.name)
        return [SheetResult(sheet_name=source_file.name, file_path=source_file)]

    from openpyxl import Workbook, load_workbook

    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        source_wb = load_workbook(source_file, read_only=True, data_only=True, keep_links=False)
    except Exception as exc:
        raise SplitError(f"Cannot open workbook {source_file.name}: {exc}") from exc
    LOG.info("Workbook %s has %d sheet(s): %s", source_file.name, len(source_wb.sheetnames),
             ", ".join(source_wb.sheetnames))

    results: list[SheetResult] = []
    used_names: set[str] = set()
    try:
        for sheet_name in source_wb.sheetnames:
            if any(k in sheet_name.lower() for k in SKIP_SHEET_KEYWORDS):
                LOG.info("Sheet '%s' skipped (instruction / lookup sheet)", sheet_name)
                continue
            try:
                target_wb = Workbook(write_only=True)
                target_sheet = target_wb.create_sheet(title=sheet_name[:31])
                n_rows = 0
                for row in source_wb[sheet_name].iter_rows(values_only=True):
                    target_sheet.append(row)
                    n_rows += 1

                base = _safe_name(sheet_name)
                name, n = base, 1
                while name.lower() in used_names:  # two sheets can map to the same safe name
                    n += 1
                    name = f"{base}_{n}"
                used_names.add(name.lower())

                out = output_dir / f"{prefix}{name}.xlsx"
                target_wb.save(out)
                results.append(SheetResult(sheet_name=sheet_name, file_path=out))
                LOG.info("Sheet '%s' -> %s (%d row(s))", sheet_name, out.name, n_rows)
            except Exception as exc:  # one bad sheet does not fail the file
                LOG.warning("Sheet '%s' could not be split: %s", sheet_name, exc)
                results.append(SheetResult(sheet_name=sheet_name, file_path=None, error=str(exc)))
    finally:
        source_wb.close()

    if not results:
        raise SplitError(f"Workbook {source_file.name} has no data sheets")
    if not any(r.ok for r in results):
        last = results[-1]
        raise SplitError(
            f"All sheets failed to split for {source_file.name}. Last sheet error: {last.sheet_name} - {last.error}"
        )
    return results
