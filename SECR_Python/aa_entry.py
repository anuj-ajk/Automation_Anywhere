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
    E-mail helpers (for the Python_<status>_Technical / Python_<status>_Business templates in UserConfig.xlsx):
    batch_count, files_total, files_ok, message_html and trace_html (HTML-escaped),
    batches_html (technical table: states, errors, timings) and
    business_batches_html (business table: plain status words, no technical details).
    Per batch: created_by (FileUploadBatch.CreatedBy, via run.batch_owner_query) and created_by_email
    ("" when it is not an e-mail address). recipients: one entry per uploader e-mail with that person's
    status, exit_code, batch_ids, batch_count, files_total, files_ok and business_batches_html, so the bot
    loops over it and sends each uploader the Business e-mail for their own batches only.
    recipient_emails ("a@x;b@y") and batches_without_email ("42,43") are summaries of the same.
"""
from __future__ import annotations

import html
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
        owners = _lookup_owners(db, cfg.run.batch_owner_query, [r.batch_id for r in results], log)

        return _result(
            _STATUS_BY_CODE.get(code, "FAILED"), code, summary, log_file,
            [
                {
                    "batch_id": r.batch_id,
                    "created_by": owners.get(str(r.batch_id), ""),
                    "created_by_email": _email_or_blank(owners.get(str(r.batch_id), "")),
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


_STATE_COLOURS = {  # (text, background) for the state pills in batches_html
    "SUCCESS": ("#1A7F37", "#E9F7EE"), "COMPLETED": ("#1A7F37", "#E9F7EE"),
    "ALREADY_COMPLETED": ("#1A7F37", "#E9F7EE"),
    "FAILED": ("#CF222E", "#FDECEC"), "ERROR": ("#CF222E", "#FDECEC"),
    "COMPLETED_WITH_FILE_ERRORS": ("#9A6700", "#FFF8E1"), "PARTIAL_FAILURE": ("#9A6700", "#FFF8E1"),
    "LOCKED": ("#9A6700", "#FFF8E1"),
}
_FONT = "'Segoe UI', Calibri, Arial, Helvetica, sans-serif"


def _pill(state) -> str:
    fg, bg = _STATE_COLOURS.get(str(state).upper(), ("#57606A", "#F2F4F6"))
    return (f'<span style="display:inline-block;background:{bg};color:{fg};font-size:11px;font-weight:700;'
            f'padding:2px 8px;border-radius:10px;white-space:nowrap;">{html.escape(str(state))}</span>')


def _batches_html(batches) -> str:
    """An e-mail-safe HTML table (inline styles) of every batch and its files, for the {BatchesHtml} placeholder."""
    if not batches:
        return (f'<p style="margin:0;font-family:{_FONT};font-size:13px;color:#656D76;">'
                'No batches were processed in this run.</p>')
    th = (f'style="padding:8px 10px;font-family:{_FONT};font-size:11px;font-weight:700;letter-spacing:.5px;'
          'text-transform:uppercase;color:#656D76;text-align:left;border-bottom:1px solid #D0D7DE;"')
    td = f'style="padding:7px 10px;font-family:{_FONT};font-size:13px;color:#24292F;border-bottom:1px solid #EAEEF2;"'
    rows = []
    for b in batches:
        err = html.escape(str(b.get("error") or ""))
        rows.append(
            f'<tr style="background:#F6F8FA;"><td {td}><b>Batch {html.escape(str(b.get("batch_id")))}</b></td>'
            f'<td {td}>{_pill(b.get("state"))}</td>'
            f'<td {td}>{b.get("files_ok", 0)} / {b.get("files", 0)} files OK</td>'
            f'<td {td}>{float(b.get("seconds") or 0):.1f} s</td></tr>')
        if err:
            rows.append(f'<tr><td {td} colspan="4"><span style="color:#CF222E;">{err}</span></td></tr>')
        for f in b.get("file_results") or []:
            detail = html.escape(str(f.get("detail") or ""))
            rows.append(
                f'<tr><td {td}>&nbsp;&nbsp;File {html.escape(str(f.get("file_id")))}</td>'
                f'<td {td}>{_pill(f.get("status"))}</td>'
                f'<td {td} colspan="2">{html.escape(str(f.get("file_name") or ""))}'
                + (f'<br><span style="font-size:12px;color:#CF222E;">{detail}</span>' if detail else "")
                + '</td></tr>')
    return ('<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
            'style="border-collapse:collapse;border:1px solid #D0D7DE;">'
            f'<tr><th {th}>Batch / File</th><th {th}>State</th><th {th}>Files</th><th {th}>Time</th></tr>'
            + "".join(rows) + "</table>")


_BUSINESS_STATE = {  # plain words for the business e-mail (no technical state names)
    "SUCCESS": "Completed", "COMPLETED": "Completed", "ALREADY_COMPLETED": "Already completed",
    "COMPLETED_WITH_FILE_ERRORS": "Completed with issues", "PARTIAL_FAILURE": "Completed with issues",
    "FAILED": "Not completed", "ERROR": "Not completed", "LOCKED": "Will retry", "SKIPPED": "Skipped",
    "NO_FILES": "No files", "IN_PROGRESS": "In progress",
}


def _business_batches_html(batches) -> str:
    """Like _batches_html but for business readers: plain status words, no error details or timings."""
    if not batches:
        return (f'<p style="margin:0;font-family:{_FONT};font-size:13px;color:#656D76;">'
                'No batches were processed in this run.</p>')
    th = (f'style="padding:8px 10px;font-family:{_FONT};font-size:11px;font-weight:700;letter-spacing:.5px;'
          'text-transform:uppercase;color:#656D76;text-align:left;border-bottom:1px solid #D0D7DE;"')
    td = f'style="padding:7px 10px;font-family:{_FONT};font-size:13px;color:#24292F;border-bottom:1px solid #EAEEF2;"'

    def word(state):
        key = str(state or "").upper()
        fg, bg = _STATE_COLOURS.get(key, ("#57606A", "#F2F4F6"))
        text = _BUSINESS_STATE.get(key, str(state or "").replace("_", " ").capitalize())
        return (f'<span style="display:inline-block;background:{bg};color:{fg};font-size:11px;font-weight:700;'
                f'padding:2px 8px;border-radius:10px;white-space:nowrap;">{html.escape(text)}</span>')

    rows = []
    for b in batches:
        rows.append(
            f'<tr style="background:#F6F8FA;"><td {td} colspan="2"><b>Batch {html.escape(str(b.get("batch_id")))}</b>'
            f' &nbsp;<span style="color:#656D76;">({b.get("files_ok", 0)} of {b.get("files", 0)} files processed)</span></td>'
            f'<td {td}>{word(b.get("state"))}</td></tr>')
        for f in b.get("file_results") or []:
            rows.append(f'<tr><td {td} width="8">&nbsp;</td><td {td}>{html.escape(str(f.get("file_name") or ""))}</td>'
                        f'<td {td}>{word(f.get("status"))}</td></tr>')
    return ('<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
            'style="border-collapse:collapse;border:1px solid #D0D7DE;">'
            f'<tr><th {th} colspan="2">Batch / File</th><th {th}>Status</th></tr>'
            + "".join(rows) + "</table>")


_EMAIL = re.compile(r"^[^@\s,;<>]+@[^@\s,;<>]+\.[^@\s,;<>]+$")


def _email_or_blank(value) -> str:
    """The value if it looks like one e-mail address, else "" (CreatedBy may hold a user name or id)."""
    text = str(value or "").strip()
    return text if _EMAIL.match(text) else ""


def _lookup_owners(db, sql, batch_ids, log) -> dict[str, str]:
    """BatchId -> uploader (first column of ``sql``, normally FileUploadBatch.CreatedBy).
    Never raises: a failed lookup only means no per-user e-mail, the run result stays as it is."""
    owners: dict[str, str] = {}
    if not sql or db is None or not batch_ids:
        return owners
    try:
        with db.session() as s:
            for bid in dict.fromkeys(str(b) for b in batch_ids):
                rows = s.query(sql, (bid,))
                value = next(iter(rows[0].values()), None) if rows else None
                owners[bid] = "" if value is None else str(value).strip()
        missing = [b for b, v in owners.items() if not _email_or_blank(v)]
        if missing:
            log.warning("No valid uploader e-mail for batch(es) %s (run.batch_owner_query); they are left out of "
                        "'recipients'", ", ".join(missing))
    except Exception as exc:
        log.warning("Could not look up the batch uploaders (run.batch_owner_query): %s: %s",
                    type(exc).__name__, exc)
    return owners


def _overall(batches) -> tuple[str, int]:
    """Status and exit code for a subset of batches, same rules as orchestrator.exit_code."""
    states = {str(b.get("state")) for b in batches}
    if "FAILED" in states:
        return "FAILED", 1
    if states & {"COMPLETED_WITH_FILE_ERRORS", "LOCKED"}:
        return "PARTIAL", 2
    return "SUCCESS", 0


def _recipients(batches) -> list[dict]:
    """One entry per uploader e-mail with only that person's batches, ready for the Business e-mail."""
    by_email: dict[str, list[dict]] = {}
    for b in batches:
        email = b.get("created_by_email") or ""
        if email:
            by_email.setdefault(email.lower(), []).append(b)
    out = []
    for own in by_email.values():
        status, code = _overall(own)
        out.append({
            "email": own[0]["created_by_email"],
            "status": status,
            "exit_code": code,
            "batch_ids": ",".join(str(b.get("batch_id")) for b in own),
            "batch_count": len(own),
            "files_total": sum(int(b.get("files") or 0) for b in own),
            "files_ok": sum(int(b.get("files_ok") or 0) for b in own),
            "business_batches_html": _business_batches_html(own),
        })
    return out


def _result(status, code, message, log_file="", batches=None, trace="") -> str:
    batches = batches or []
    out = {"status": status, "exit_code": code, "message": message, "log_file": log_file,
           "batches": batches}
    if trace:
        out["trace"] = trace
    # Ready-made values for the Python_<status>_Technical / _Business e-mail templates in UserConfig.xlsx.
    out["batch_count"] = len(batches)
    out["files_total"] = sum(int(b.get("files") or 0) for b in batches)
    out["files_ok"] = sum(int(b.get("files_ok") or 0) for b in batches)
    out["message_html"] = html.escape(str(message or ""))
    out["trace_html"] = html.escape(str(trace or "No stack trace (the run did not raise an exception)."))
    out["batches_html"] = _batches_html(batches)                     # technical: states, errors, timings
    out["business_batches_html"] = _business_batches_html(batches)   # business: plain words only
    # Per uploader (FileUploadBatch.CreatedBy): send each person the Business e-mail for their batches only.
    out["recipients"] = _recipients(batches)
    out["recipient_emails"] = ";".join(r["email"] for r in out["recipients"])
    out["batches_without_email"] = ",".join(str(b.get("batch_id")) for b in batches
                                            if not b.get("created_by_email"))
    return json.dumps(out, default=str)


if __name__ == "__main__":
    # Manual test with the same dictionary the AA bot passes:
    #   python aa_entry.py          -> the pending batches (run.pending_batches_query in config.yaml)
    #   python aa_entry.py 40,41    -> only these batch ids
    given_ids = " ".join(sys.argv[1:])
    print(run_batches(
        {
            "batch_ids": given_ids,
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
