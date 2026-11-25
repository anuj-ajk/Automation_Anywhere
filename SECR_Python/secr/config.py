"""Configuration.

Non-secret settings come from a YAML file (see ``config.yaml``).
Secrets are read ONLY from environment variables, never from the YAML file:

    SECR_DB_USER, SECR_DB_PASSWORD             ESG MySQL (was CV "ESG Locker/ESGDB MySQL")
    SECR_S3_BUCKET                             (was CV "AWS_S3/ESG_BUCKET_NAME")
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY   standard boto3 variables (was CV "AWS_S3")
    SECR_ODIN_API_KEY, SECR_ODIN_API_SECRET    (was CV "AA ESG EKB Credential")
    SECR_EM_CONNECTION_STRING                  ODBC string for Energy Manager (was @sql_JupiterConnection)
    SECR_NAV_CONNECTION_STRING                 ODBC string for NAV (was CV "NAV_ESGDB_ConnectionString")
"""
from __future__ import annotations

import getpass
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

DEFAULT_AI_MESSAGE = (
    "extract and make sure not to skip any records then normalize and return the csv file link"
)


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


@dataclass
class DbConfig:
    host: str
    port: int = 3306
    database: str = "esgdb"
    user: str = ""
    password: str = ""
    connect_timeout: int = 30
    connect_retry_seconds: float = 5    # DB full (too many connections): try again this often until it accepts
    lock_wait_timeout_seconds: int = 0  # innodb_lock_wait_timeout for our sessions (0 = server default, 50s)


@dataclass
class S3Config:
    bucket: str = ""
    region: str = "eu-west-2"
    download_attempts: int = 3          # transient errors (network, throttling, 5xx) are retried
    retry_backoff_seconds: float = 5    # wait before retry n: backoff x 1, 2, 4 ... (max 120s)


@dataclass
class OdinConfig:
    base_uri: str = "https://api.getodin.ai"
    project_id: str = ""
    agent_id: str = ""
    api_key: str = ""
    api_secret: str = ""
    message: str = DEFAULT_AI_MESSAGE
    # The AA bot used a ~100 min timeout for the upload and treated a timeout as "accepted".
    submit_timeout_seconds: int = 300
    request_timeout_seconds: int = 120
    poll_interval_seconds: int = 40
    max_poll_attempts: int = 15
    max_parallel_sheets: int = 5        # sheets of ONE file submitted / polled / downloaded at the same time
    max_parallel_requests: int = 20     # Odin calls + CSV downloads in flight across ALL files and batches
    submit_attempts: int = 3            # create chat + upload, retried with a NEW chat on transient errors
    submit_retry_backoff_seconds: float = 10


@dataclass
class SourceConfig:
    environment: str = "DEV"           # DEV / UAT / PROD -> picks <ENV>_<TYPE>_EM_QUERY.txt
    query_dir: Path = Path(".")
    em_connection_string: str = ""
    nav_connection_string: str = ""
    query_timeout_seconds: int = 600


@dataclass
class RunConfig:
    max_parallel_batches: int = 10
    max_parallel_files_per_batch: int = 4
    max_db_connections: int = 20        # cap for THIS process, leaves room in the DB for other systems
    work_dir: Path = Path("work")
    log_dir: Path = Path("logs")
    keep_work_files: bool = False
    created_by: str = ""
    serialize_staging_loads: bool = True
    staging_lock_timeout_seconds: int = 900
    insert_chunk_size: int = 1000
    # Big files are bulk-inserted over several DB connections at once (only connections that are free).
    insert_connections: int = 4
    parallel_insert_min_rows: int = 20000
    pending_batches_query: str = ""
    # Who uploaded a batch, for e-mailing the result to that person only (aa_entry output "recipients").
    # One %s = the BatchId; the first column of the first row is used. "" = no lookup.
    batch_owner_query: str = "SELECT CreatedBy FROM FileUploadBatch WHERE BatchId = %s"
    # Rerun: files already in one of completed_statuses are not processed again (force / --force overrides).
    skip_completed_files: bool = True
    completed_statuses: tuple = ("COMPLETED", "DATA_NOT_AVAILABLE")
    # Delete this FileId's rows from the staging table before loading it again (no duplicates on rerun).
    clear_staging_before_load: bool = True
    # FileUploadBatch status when some files failed (COMPLETE is used if this code is not in RefBatchStatus).
    batch_status_on_file_errors: str = "PARTIAL_FAILURE"
    # FileUploadBatch status set as soon as a batch is picked up ("" = do not change it).
    batch_status_in_progress: str = "IN_PROGRESS"
    # Pending mode: batches in one of these statuses may be claimed (must match pending_batches_query).
    claimable_statuses: tuple = ("PROCESSING", "PARTIAL_FAILURE")
    # Optional stored procedure that claims pending batches atomically (sql/ClaimBatches.sql), e.g.
    # "ClaimPendingBatches". Empty: pending_batches_query + the same compare-and-set UPDATE from Python.
    claim_procedure: str = ""
    # Optional stored procedure that claims ONE batch (sql/ClaimBatches.sql), e.g. "ClaimBatch". Used in
    # pending mode when claim_procedure is empty. Empty: the same compare-and-set UPDATE from Python.
    claim_batch_procedure: str = ""
    # Explicit batch ids with claim_batch_procedure: also take a batch that is already IN_PROGRESS
    # (set by force / --force). Otherwise such a batch is skipped.
    take_in_progress_batches: bool = False
    pending_poll_seconds: int = 60       # pending mode: look for new batches at least this often
    pending_max_runtime_minutes: int = 0  # pending mode: stop taking NEW batches after this (0 = no limit)
    # Claim tracking (needs sql/AddClaimColumns.sql): a running batch refreshes ClaimedAt every
    # claim_heartbeat_seconds; an IN_PROGRESS batch without a refresh for stale_claim_minutes is released.
    claim_heartbeat_seconds: int = 60
    stale_claim_minutes: int = 30         # 0 = never release stale claims
    # Optional stored procedure that releases stale claims (sql/ClaimBatches.sql), e.g. "ReleaseStaleClaims".
    # Empty: the same release from Python.
    release_stale_procedure: str = ""
    # Test process: only insert into the Staging_* tables; the Load* / Calculate* (and other post-load)
    # stored procedures are NOT called. Set from the AA dictionary key "test_process".
    test_process: bool = False


@dataclass
class NotifyConfig:
    enabled: bool = False
    smtp_host: str = ""
    smtp_port: int = 25
    use_tls: bool = False
    from_address: str = ""
    to_addresses: list[str] = field(default_factory=list)
    cc_addresses: list[str] = field(default_factory=list)


@dataclass
class AppConfig:
    db: DbConfig
    s3: S3Config
    odin: OdinConfig
    sources: SourceConfig
    run: RunConfig
    notify: NotifyConfig


def _section(raw: dict[str, Any], name: str) -> dict[str, Any]:
    value = raw.get(name) or {}
    if not isinstance(value, dict):
        raise ValueError(f"Config section '{name}' must be a mapping")
    return value


def load_config(path: str | Path) -> AppConfig:
    import yaml  # local import keeps the rest of the package importable without PyYAML

    path = Path(path)
    with path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    base_dir = path.parent

    def resolve(p: str | Path) -> Path:
        p = Path(os.path.expanduser(os.path.expandvars(str(p))))  # allows e.g. %TEMP%\SECR_work
        return p if p.is_absolute() else (base_dir / p).resolve()

    db_raw = _section(raw, "esg_db")
    db = DbConfig(
        host=db_raw.get("host", ""),
        port=int(db_raw.get("port", 3306)),
        database=db_raw.get("database", "esgdb"),
        user=_env("SECR_DB_USER"),
        password=_env("SECR_DB_PASSWORD"),
        connect_timeout=int(db_raw.get("connect_timeout", 30)),
        connect_retry_seconds=float(db_raw.get("connect_retry_seconds", 5)),
        lock_wait_timeout_seconds=int(db_raw.get("lock_wait_timeout_seconds", 0) or 0),
    )

    s3_raw = _section(raw, "s3")
    s3 = S3Config(
        bucket=_env("SECR_S3_BUCKET", s3_raw.get("bucket", "")),
        region=s3_raw.get("region", "eu-west-2"),
        download_attempts=int(s3_raw.get("download_attempts", 3)),
        retry_backoff_seconds=float(s3_raw.get("retry_backoff_seconds", 5)),
    )

    o = _section(raw, "odin")
    odin = OdinConfig(
        base_uri=str(o.get("base_uri", "https://api.getodin.ai")).rstrip("/"),
        project_id=str(o.get("project_id", "")),
        agent_id=str(o.get("agent_id", "")),
        api_key=_env("SECR_ODIN_API_KEY"),
        api_secret=_env("SECR_ODIN_API_SECRET"),
        message=o.get("message", DEFAULT_AI_MESSAGE),
        submit_timeout_seconds=int(o.get("submit_timeout_seconds", 300)),
        request_timeout_seconds=int(o.get("request_timeout_seconds", 120)),
        poll_interval_seconds=int(o.get("poll_interval_seconds", 40)),
        max_poll_attempts=int(o.get("max_poll_attempts", 15)),
        max_parallel_sheets=int(o.get("max_parallel_sheets", 5)),
        max_parallel_requests=int(o.get("max_parallel_requests", 20)),
        submit_attempts=int(o.get("submit_attempts", 3)),
        submit_retry_backoff_seconds=float(o.get("submit_retry_backoff_seconds", 10)),
    )

    s = _section(raw, "sources")
    sources = SourceConfig(
        environment=str(s.get("environment", "DEV")).upper(),
        query_dir=resolve(s.get("query_dir", ".")),
        em_connection_string=_env("SECR_EM_CONNECTION_STRING"),
        nav_connection_string=_env("SECR_NAV_CONNECTION_STRING"),
        query_timeout_seconds=int(s.get("query_timeout_seconds", 600)),
    )

    r = _section(raw, "run")
    run = RunConfig(
        max_parallel_batches=int(r.get("max_parallel_batches", 10)),
        max_parallel_files_per_batch=int(r.get("max_parallel_files_per_batch", 4)),
        max_db_connections=int(r.get("max_db_connections", 20)),
        work_dir=resolve(r.get("work_dir", "work")),
        log_dir=resolve(r.get("log_dir", "logs")),
        keep_work_files=bool(r.get("keep_work_files", False)),
        created_by=r.get("created_by") or getpass.getuser(),
        serialize_staging_loads=bool(r.get("serialize_staging_loads", True)),
        staging_lock_timeout_seconds=int(r.get("staging_lock_timeout_seconds", 900)),
        insert_chunk_size=int(r.get("insert_chunk_size", 1000)),
        insert_connections=int(r.get("insert_connections", 4)),
        parallel_insert_min_rows=int(r.get("parallel_insert_min_rows", 20000)),
        pending_batches_query=r.get("pending_batches_query", "") or "",
        batch_owner_query=(r["batch_owner_query"] or "") if "batch_owner_query" in r
        else RunConfig.batch_owner_query,
        skip_completed_files=bool(r.get("skip_completed_files", True)),
        completed_statuses=tuple(str(c).strip().upper() for c in
                                 (r.get("completed_statuses") or ("COMPLETED", "DATA_NOT_AVAILABLE"))),
        clear_staging_before_load=bool(r.get("clear_staging_before_load", True)),
        batch_status_on_file_errors=str(r.get("batch_status_on_file_errors") or "PARTIAL_FAILURE").strip().upper(),
        batch_status_in_progress=str(r.get("batch_status_in_progress", "IN_PROGRESS") or "").strip().upper(),
        claimable_statuses=tuple(str(c).strip().upper() for c in
                                 (r.get("claimable_statuses") or ("PROCESSING", "PARTIAL_FAILURE"))),
        claim_procedure=str(r.get("claim_procedure") or "").strip(),
        claim_batch_procedure=str(r.get("claim_batch_procedure") or "").strip(),
        pending_poll_seconds=int(r.get("pending_poll_seconds", 60)),
        pending_max_runtime_minutes=int(r.get("pending_max_runtime_minutes", 0)),
        claim_heartbeat_seconds=int(r.get("claim_heartbeat_seconds", 60)),
        stale_claim_minutes=int(r.get("stale_claim_minutes", 30)),
        release_stale_procedure=str(r.get("release_stale_procedure") or "").strip(),
    )

    n = _section(raw, "notify")
    notify = NotifyConfig(
        enabled=bool(n.get("enabled", False)),
        smtp_host=n.get("smtp_host", ""),
        smtp_port=int(n.get("smtp_port", 25)),
        use_tls=bool(n.get("use_tls", False)),
        from_address=n.get("from_address", ""),
        to_addresses=list(n.get("to_addresses", []) or []),
        cc_addresses=list(n.get("cc_addresses", []) or []),
    )

    cfg = AppConfig(db=db, s3=s3, odin=odin, sources=sources, run=run, notify=notify)
    validate(cfg)
    return cfg


def min_db_connections(cfg: AppConfig) -> int:
    """Smallest per-process connection cap that can never deadlock.

    Each running batch holds one connection for its whole run (duplicate-run lock) and needs one more at a
    time for its own status updates. File workers hold at most one connection each and never wait for a
    second one (extra bulk-insert connections are only taken when free). So one slot left over after every
    batch has its lock connection is enough for work to keep moving; other workers wait their turn.
    """
    return cfg.run.max_parallel_batches + 1


def validate(cfg: AppConfig) -> None:
    problems = []
    if not cfg.db.host:
        problems.append("esg_db.host is not set")
    if not cfg.db.user or not cfg.db.password:
        problems.append("SECR_DB_USER / SECR_DB_PASSWORD environment variables are not set")
    if cfg.run.max_parallel_batches < 1:
        problems.append("run.max_parallel_batches must be >= 1")
    if cfg.run.max_parallel_files_per_batch < 1:
        problems.append("run.max_parallel_files_per_batch must be >= 1")
    needed = min_db_connections(cfg)
    if cfg.run.max_db_connections < needed:
        problems.append(
            f"run.max_db_connections ({cfg.run.max_db_connections}) must be at least "
            f"max_parallel_batches + 1 = {needed}"
        )
    if cfg.db.connect_retry_seconds <= 0:
        problems.append("esg_db.connect_retry_seconds must be > 0")
    if problems:
        raise ValueError("Invalid configuration:\n  - " + "\n  - ".join(problems))
