"""Input parsing + wiring, shared by aa_entry.py (Automation Anywhere) and ``python -m ai_response``."""
from __future__ import annotations

import csv
import json
import os
import re
from pathlib import Path
from typing import Any, Mapping

from .config import AIConfig, from_args
from .logging_setup import get_logger, setup_logging
from .odin_client import OdinClient
from .processor import AIResponseProcessor, RunResult, SheetJob
from .status_db import NoStatusDb, StatusDb

LOG = get_logger(__name__)

_ID_KEYS = ("FileID", "FileId", "file_id", "fileid")
_PATH_KEYS = ("SplittedFilePath", "FileToNormalizePath", "file_path", "path")
OUTPUT_COLUMNS = ("FileID", "NormalizedCSVLink", "FileName", "Category")


def _get(row: Mapping[str, Any], keys) -> str:
    for k in keys:
        if row.get(k) not in (None, ""):
            return str(row[k]).strip()
    return ""


def _file_id(value: str) -> str:
    return value.split(".")[0] if re.fullmatch(r"\d+\.0+", value) else value


def parse_files(value: Any) -> list[SheetJob]:
    """Accept the sheets to normalize in any of these forms:

    * a list of dicts / a JSON array:  [{"FileID": "77", "SplittedFilePath": "C:\\...\\Sheet_A.xlsx"}, ...]
    * a CSV file path with the columns FileID,SplittedFilePath (an AA DataTable written to CSV)
    * text "77|C:\\a.xlsx;77|C:\\b.xlsx" (FileID|path, separated by ; or new lines)
    """
    if value is None or value == "":
        return []
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("["):
            value = json.loads(text)
        elif text.lower().endswith(".csv") and "|" not in text and Path(text).is_file():
            with open(text, encoding="utf-8-sig", newline="") as fh:
                value = list(csv.DictReader(fh))
        else:
            value = []
            for part in re.split(r"[;\r\n]+", text):
                if part.strip():
                    file_id, _, path = part.partition("|")
                    value.append({"FileID": file_id, "SplittedFilePath": path})
    jobs = []
    for i, row in enumerate(value, 1):
        if not isinstance(row, Mapping):
            raise ValueError(f"files row {i} is not a dictionary: {row!r}")
        file_id, path = _file_id(_get(row, _ID_KEYS)), _get(row, _PATH_KEYS)
        if not file_id or not path:
            raise ValueError(f"files row {i} needs FileID and SplittedFilePath: {dict(row)}")
        jobs.append(SheetJob(file_id=file_id, path=Path(path)))
    return jobs


def write_links_csv(result: RunResult, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(result.links)
    LOG.info("Wrote %d link row(s) to %s", len(result.links), path)


def build(cfg: AIConfig) -> AIResponseProcessor:
    cfg.validate()
    client = OdinClient(cfg)
    status_db = StatusDb(cfg) if cfg.update_db else NoStatusDb()
    return AIResponseProcessor(client, status_db, cfg.max_parallel, cfg.poll_interval_seconds,
                               cfg.max_poll_attempts)


def run(args: Mapping[str, Any], build_processor=None) -> dict[str, Any]:
    """Run the sub task. Returns a plain dict (aa_entry turns it into JSON)."""
    cfg = from_args(args)
    _, log_file = setup_logging(cfg.log_dir, cfg.debug)
    LOG.info("Settings: base_uri=%s project=%s agent=%s max_parallel=%d poll=%ss x %d update_db=%s; "
             "secrets: SECR_ODIN_API_KEY=%s SECR_ODIN_API_SECRET=%s SECR_DB_USER=%s SECR_DB_PASSWORD=%s",
             cfg.base_uri, cfg.project_id, cfg.agent_id, cfg.max_parallel, cfg.poll_interval_seconds,
             cfg.max_poll_attempts, cfg.update_db,
             *("set" if os.environ.get(v) else "NOT SET" for v in
               ("SECR_ODIN_API_KEY", "SECR_ODIN_API_SECRET", "SECR_DB_USER", "SECR_DB_PASSWORD")))

    jobs = parse_files(args.get("files") or args.get("input_csv"))
    if not jobs:
        raise ValueError("No files to normalize (key 'files' or 'input_csv')")
    result = (build_processor or build)(cfg).run(jobs)

    output_csv = args.get("output_csv")
    if output_csv:
        write_links_csv(result, Path(str(output_csv)))

    failed = sum(1 for f in result.files if f.status == "FAILED_WHILE_NORMALIZATION")
    partial = sum(1 for f in result.files if f.status == "PARTIAL_FAILURE")
    status = "FAILED" if failed == len(result.files) else "PARTIAL" if failed or partial else "SUCCESS"
    return {
        "status": status,
        "message": f"{len(jobs)} sheet(s), {len(result.links)} CSV link(s); "
                   f"{len(result.files) - failed - partial} file(s) OK, {partial} partial, {failed} failed",
        "log_file": log_file,
        "output_csv": str(output_csv or ""),
        "seconds": result.seconds,
        "links": result.links,
        "files": [vars(f) for f in result.files],
    }
