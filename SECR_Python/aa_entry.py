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
    work_dir      optional  temp folder for downloads / split sheets / CSVs (default: run.work_dir in config.yaml),
                            e.g. "%TEMP%\\SECR_work"; keep it short and local
    log_dir       optional  folder for the log file (default: run.log_dir in config.yaml)
    force         optional  "true" to process every file again, also those already COMPLETED
    pending       optional  "true": pending mode. Pending batches are claimed atomically (set IN_PROGRESS) and
                            every free slot (max_batches) is kept busy: while batches run, new pending ones are
                            looked for, so with 2 running and a cap of 10, 8 more can start. Ends when nothing
                            runs and nothing is pending. batch_ids is then optional (they run first).
    pending_limit optional  take at most this many batches in total in one call (default: no limit)
    poll_seconds  optional  pending mode: look for new batches at least this often (default 60)
    max_runtime_minutes optional  pending mode: stop taking NEW batches after this many minutes (default: none)
    test_process  optional  "true": only insert into the Staging_* tables; the load / calculate stored
                            procedures are skipped (default false)
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
from dotenv import load_dotenv

load_dotenv()  # load .env if present, so AA can override with its own environment variables

# Secrets used by the SECR process (values come from .env or the machine's environment variables).
SECR_DB_USER = os.environ.get("SECR_DB_USER")
SECR_DB_PASSWORD = os.environ.get("SECR_DB_PASSWORD")
SECR_S3_BUCKET = os.environ.get("SECR_S3_BUCKET")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
SECR_ODIN_API_KEY = os.environ.get("SECR_ODIN_API_KEY")
SECR_ODIN_API_SECRET = os.environ.get("SECR_ODIN_API_SECRET")
SECR_EM_CONNECTION_STRING = os.environ.get("SECR_EM_CONNECTION_STRING")
SECR_NAV_CONNECTION_STRING = os.environ.get("SECR_NAV_CONNECTION_STRING")

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
        if val is None or str(val) == "":  # not set in .env / Credential Vault: keep what the machine has
            continue
        os.environ[str(key)] = str(val)


def _secret_keys(secrets) -> str:
    """Names of the secrets passed in (never the values)."""
    try:
        if isinstance(secrets, str):
            secrets = json.loads(secrets)
        return ", ".join(sorted(str(k) for k, v in dict(secrets or {}).items() if v not in (None, "")))
    except Exception:
        return "unreadable"


def run_batches(args) -> str:
    """Process the given batches in parallel. ``args`` is the AA dictionary (or a JSON string)."""
    try:
        if isinstance(args, str):
            args = json.loads(args) if args.strip().startswith("{") else {"batch_ids": args}
        args = dict(args or {})

        if str(args.get("batch_ids") or "").strip().lower() == "pending":  # batch_ids = "pending" also works
            args["pending"], args["batch_ids"] = "true", ""
        pending = _as_bool(args.get("pending", "false"))
        batch_ids = parse_batch_ids(args.get("batch_ids"))
        if not batch_ids and not pending:
            return _result("FAILED", 1, "No batch ids were given (key 'batch_ids', or pending = true)")

        _apply_secrets(args.get("secrets"))

        from secr.config import load_config
        from secr.runner import apply_overrides, build, execute, execute_pending

        config_path = Path(args.get("config_path") or HERE / "config.yaml")
        cfg = load_config(config_path)
        apply_overrides(cfg, _as_int(args.get("max_batches")), _as_int(args.get("max_files")),
                        force=_as_bool(args.get("force", "false")),
                        work_dir=args.get("work_dir"), log_dir=args.get("log_dir"), base=config_path.parent)
        if _as_int(args.get("poll_seconds")):
            cfg.run.pending_poll_seconds = _as_int(args.get("poll_seconds"))
        if args.get("max_runtime_minutes") not in (None, ""):
            cfg.run.pending_max_runtime_minutes = _as_int(args.get("max_runtime_minutes")) or 0
        cfg.run.test_process = _as_bool(args.get("test_process", "false"))
        processor, db, notifier, log_file = build(cfg, debug=_as_bool(args.get("debug", "false")))

        from secr.logging_setup import get_logger

        log = get_logger("secr.aa_entry")
        log.info("Called from Automation Anywhere: config %s, batch ids %s, pending=%s, max_batches=%s, "
                 "max_files=%s, debug=%s, test_process=%s, secrets passed: %s", config_path,
                 ", ".join(batch_ids) or "none", pending, args.get("max_batches"), args.get("max_files"),
                 args.get("debug"), cfg.run.test_process, _secret_keys(args.get("secrets")) or "none")
        if pending:
            results, code, summary = execute_pending(processor, notifier, cfg, db, batch_ids,
                                                     _as_int(args.get("pending_limit")) or 0)
            if not results:  # nothing waiting is a normal outcome for a scheduled bot, not a failure
                return _result("SUCCESS", 0, "No pending batches", log_file)
        else:
            results, code, summary = execute(processor, notifier, batch_ids, cfg.run.max_parallel_batches)
        log.info("Returning status %s to Automation Anywhere", _STATUS_BY_CODE.get(code, "FAILED"))

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
        import logging

        logging.getLogger("secr.aa_entry").exception("run_batches failed")
        return _result("FAILED", 1, f"{type(exc).__name__}: {exc}", trace=traceback.format_exc())


def _result(status, code, message, log_file="", batches=None, trace="") -> str:
    out = {"status": status, "exit_code": code, "message": message, "log_file": log_file,
           "batches": batches or []}
    if trace:
        out["trace"] = trace
    return json.dumps(out, default=str)


if __name__ == "__main__":
    # Manual test with the same dictionary the AA bot passes:
    #   python aa_entry.py          -> the pending batches (run.pending_batches_query in config.yaml)
    #   python aa_entry.py 40,41    -> only these batch ids
    given_ids = " ".join(sys.argv[1:])
    print(run_batches(
        {
            "pending": not given_ids,       # no ids on the command line: take the pending batches
            "pending_limit": 10,            # at most 10 pending batches per call ("" = all)
            "config_path": str(HERE / "config.yaml"),
            "work_dir": r"%TEMP%\SECR_work",   # local temp folder (deleted after each batch)
            "log_dir": str(HERE / "logs"),
            "max_batches": 10,
            "max_files": 4,
            "debug": True,
            "test_process": False,          # True: insert into Staging_* only, skip load/calculate procs
            "secrets": {
                "SECR_DB_USER": SECR_DB_USER,
                "SECR_DB_PASSWORD": SECR_DB_PASSWORD,
                "SECR_S3_BUCKET": SECR_S3_BUCKET,
                "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
                "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
                "SECR_ODIN_API_KEY": SECR_ODIN_API_KEY,
                "SECR_ODIN_API_SECRET": SECR_ODIN_API_SECRET,
                "SECR_EM_CONNECTION_STRING": SECR_EM_CONNECTION_STRING,
                "SECR_NAV_CONNECTION_STRING": SECR_NAV_CONNECTION_STRING,
            },
        }
    ))
