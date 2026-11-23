"""Entry point for Automation Anywhere ("Python script: Open" + "Python script: Execute function").

In the bot:
    1. Python script: Open        -> file = <path>\\SECR_Python\\aa_entry.py, session "SECR"
    2. Python script: Execute function
           function name : run_batches
           argument      : a Dictionary variable (see below)
           assign output : a String variable, e.g. sPythonOutput (JSON text)
    3. Python script: Close       -> session "SECR"

Input dictionary keys (AA "Dictionary of String" is fine - every value may be text):
    batch_ids     required  list of ids, or text "40,41,42" / "40;41" / "40 41"
    config_path   optional  path to config.yaml (default: config.yaml next to this file)
    max_batches   optional  batches in parallel (default from config, normally 10)
    max_files     optional  files per batch in parallel (default from config, normally 4)
    debug         optional  "true" for verbose logging
    secrets       optional  dictionary of the SECR_* / AWS_* environment variables, if the bot
                            reads them from the Credential Vault instead of the machine environment

Output: always a JSON string (never raises), e.g.
    {"status": "SUCCESS", "exit_code": 0, "message": "...", "log_file": "...",
     "batches": [{"batch_id": "40", "state": "SUCCESS", "files": 3, "files_ok": 3,
                  "seconds": 512.4, "error": "", "file_results": [...]}]}
    status is SUCCESS (all files OK), PARTIAL (some files failed) or FAILED.
"""
from __future__ import annotations

import json
import os
import re
import sys
import traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:  # AA runs the file directly, so make the 'secr' package importable
    sys.path.insert(0, str(HERE))

_ALLOWED_SECRET_PREFIXES = ("SECR_", "AWS_")
_STATUS_BY_CODE = {0: "SUCCESS", 2: "PARTIAL", 1: "FAILED"}


def parse_batch_ids(value) -> list[str]:
    """Accept a list, a single number or text like '40,41;42 43'. Returns unique ids in order."""
    if value is None:
        return []
    items = value if isinstance(value, (list, tuple, set)) else re.split(r"[,;\s]+", str(value))
    ids: list[str] = []
    for item in items:
        if item is None:
            continue
        if isinstance(item, float) and item.is_integer():  # AA numbers can arrive as 40.0
            item = int(item)
        text = str(item).strip()
        if re.fullmatch(r"\d+\.0+", text):
            text = text.split(".")[0]
        if text and text not in ids:
            ids.append(text)
    return ids


def _as_bool(value) -> bool:
    return str(value).strip().lower() in ("1", "true", "yes", "y")


def _as_int(value):
    if value in (None, ""):
        return None
    return int(float(value))


def _apply_secrets(secrets) -> None:
    if not secrets:
        return
    if isinstance(secrets, str):
        secrets = json.loads(secrets)
    for key, val in dict(secrets).items():
        if not str(key).startswith(_ALLOWED_SECRET_PREFIXES):
            raise ValueError(f"secrets key '{key}' is not a SECR_* or AWS_* variable")
        os.environ[str(key)] = str(val)


def run_batches(args) -> str:
    """Process the given batches in parallel. ``args`` is the AA dictionary (or a JSON string)."""
    try:
        if isinstance(args, str):
            args = json.loads(args) if args.strip().startswith("{") else {"batch_ids": args}
        args = dict(args or {})

        batch_ids = parse_batch_ids(args.get("batch_ids"))
        if not batch_ids:
            return _result("FAILED", 1, "No batch ids were given (key 'batch_ids')")

        _apply_secrets(args.get("secrets"))

        from secr.config import load_config
        from secr.runner import apply_overrides, build, execute

        config_path = Path(args.get("config_path") or HERE / "config.yaml")
        cfg = load_config(config_path)
        apply_overrides(cfg, _as_int(args.get("max_batches")), _as_int(args.get("max_files")))
        processor, _db, notifier, log_file = build(cfg, debug=_as_bool(args.get("debug", "false")))
        results, code, summary = execute(processor, notifier, batch_ids, cfg.run.max_parallel_batches)

        return _result(
            _STATUS_BY_CODE.get(code, "FAILED"), code, summary, log_file,
            [
                {
                    "batch_id": r.batch_id,
                    "state": r.state,
                    "files": len(r.files),
                    "files_ok": sum(1 for f in r.files if f.ok),
                    "seconds": r.seconds,
                    "error": r.error,
                    "file_results": [
                        {"file_id": f.file_id, "file_name": f.file_name, "flow": f.flow,
                         "status": f.status, "detail": f.detail}
                        for f in r.files
                    ],
                }
                for r in results
            ],
        )
    except Exception as exc:  # AA only sees the returned text, so never let an exception escape
        return _result("FAILED", 1, f"{type(exc).__name__}: {exc}", trace=traceback.format_exc())


def _result(status, code, message, log_file="", batches=None, trace="") -> str:
    out = {"status": status, "exit_code": code, "message": message, "log_file": log_file,
           "batches": batches or []}
    if trace:
        out["trace"] = trace
    return json.dumps(out, default=str)


if __name__ == "__main__":  # manual test: python aa_entry.py 40,41
    print(run_batches({"batch_ids": " ".join(sys.argv[1:])}))
