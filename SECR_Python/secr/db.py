"""ESG DB (MySQL) access.

Every query is parameterised, so the quote/backslash escaping the AA bots did by hand is
not needed. Each unit of work opens its own short-lived connection (PyMySQL connections
must not be shared between threads). Connections are capped per process (run.max_db_connections)
and, when the database itself has no free connection slot,
opening waits until it has one (see ``EsgDb``).
"""
from __future__ import annotations

import contextlib
import re
import threading
import time
from typing import Any, Iterable, Iterator, Sequence

from .config import DbConfig
from .logging_setup import module_logger

LOG = module_logger(__name__)
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
SLOW_QUERY_SECONDS = 5.0

LOCK_WAIT_TIMEOUT = 1205  # MySQL: "Lock wait timeout exceeded; try restarting transaction"
LOCK_WAIT_HINT = (
    "another database session holds a lock on these rows - most likely a transaction that was never committed "
    "(e.g. a MySQL Workbench tab that ran an UPDATE/DELETE without COMMIT). Run COMMIT or ROLLBACK in that "
    "session, or find it with SELECT * FROM information_schema.innodb_trx and KILL its trx_mysql_thread_id"
)


def _error_code(exc) -> int | None:
    code = exc.args[0] if getattr(exc, "args", None) else None
    return code if isinstance(code, int) else None


def is_lock_wait_timeout(exc) -> bool:
    """True for MySQL 1205: the row is locked by another session (e.g. an uncommitted transaction)."""
    return _error_code(exc) == LOCK_WAIT_TIMEOUT


def _short(sql: str) -> str:
    return " ".join(sql.split())[:160]

MAX_DESCRIPTION = 2000

_BATCH_FILES_SQL = """
SELECT FB.BatchId, FB.ReportId, R.ClientId, R.ReportStart, R.ReportEnd,
       BP.BusinessPartnerName, UF.CategorySelected, UF.FileId, UF.FileName,
       UF.StoragePath, UF.FileVersion, PS.Code AS StatusCode
FROM FileUploadBatch FB
JOIN UploadedFile UF          ON UF.BatchId = FB.BatchId
LEFT JOIN RefFileProcessingStatus PS ON PS.FileProcessingStatusId = UF.ProcessingStatusId
LEFT JOIN Report R            ON FB.ReportId = R.Id
LEFT JOIN BusinessPartner BP  ON R.ClientId = BP.BusinessPartnerId
LEFT JOIN RefReportType RT    ON R.ReportTypeId = RT.ReportTypeId
WHERE FB.BatchId = %s
  AND UF.IsDeleted = 0 AND UF.IsValid = 1
  AND RT.Code = 'SECR'
ORDER BY UF.FileId
"""

_STATUS_ID = (
    "(SELECT s.FileProcessingStatusId FROM RefFileProcessingStatus s WHERE s.Code = %s)"
)


def _clip(text: str | None) -> str | None:
    if text is None:
        return None
    return text[:MAX_DESCRIPTION]


def _quote_ident(name: str) -> str:
    # Identifiers come from code constants only, but quote them defensively.
    return "`" + name.replace("`", "``") + "`"


class EsgSession:
    """Thin wrapper around one PyMySQL connection."""

    def __init__(self, conn):
        self.conn = conn

    # ---------- generic helpers ----------
    @contextlib.contextmanager
    def _cursor(self, what: str):
        """A cursor whose lock wait timeouts (1205) say what usually causes them."""
        try:
            with self.conn.cursor() as cur:
                yield cur
        except Exception as exc:
            if _error_code(exc) != LOCK_WAIT_TIMEOUT:
                raise
            LOG.error("Lock wait timeout on %s: %s", _short(what), LOCK_WAIT_HINT)
            raise type(exc)(LOCK_WAIT_TIMEOUT, f"Lock wait timeout exceeded: {LOCK_WAIT_HINT}") from exc

    def query(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        started = time.monotonic()
        with self._cursor(sql) as cur:
            cur.execute(sql, params)
            rows = list(cur.fetchall())
        self._trace("query", sql, params, started, f"{len(rows)} row(s)")
        return rows

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> int:
        started = time.monotonic()
        with self._cursor(sql) as cur:
            affected = cur.execute(sql, params)
        self._trace("execute", sql, params, started, f"{affected} row(s) affected")
        return affected

    @staticmethod
    def _trace(kind: str, sql: str, params, started: float, result: str) -> None:
        elapsed = time.monotonic() - started
        if elapsed >= SLOW_QUERY_SECONDS:
            LOG.warning("Slow SQL %s (%.1fs, %s): %s", kind, elapsed, result, _short(sql))
        else:
            LOG.debug("SQL %s (%.2fs, %s): %s | params=%s", kind, elapsed, result, _short(sql), params)

    # ---------- batch / file lookups ----------
    def fetch_batch_files(self, batch_id: str) -> list[dict[str, Any]]:
        return self.query(_BATCH_FILES_SQL, (batch_id,))

    def data_source_type(self, client_name: str) -> str:
        rows = self.query(
            "SELECT DataSourceType FROM BusinessPartner WHERE BusinessPartnerName = %s",
            (client_name,),
        )
        return (rows[0].get("DataSourceType") or "").strip() if rows else ""

    # ---------- status updates ----------
    def set_file_status(
        self,
        file_id: str,
        code: str,
        description: str | None = None,
        *,
        append: bool = False,
        clear: bool = False,
    ) -> None:
        """Set UploadedFile status. ``append`` adds to the existing StatusDescription."""
        if clear:
            sql = f"UPDATE UploadedFile SET ProcessingStatusId = {_STATUS_ID}, StatusDescription = NULL WHERE FileId = %s"
            params: tuple = (code, file_id)
        elif description is None:
            sql = f"UPDATE UploadedFile SET ProcessingStatusId = {_STATUS_ID} WHERE FileId = %s"
            params = (code, file_id)
        elif append:
            sql = (
                f"UPDATE UploadedFile SET ProcessingStatusId = {_STATUS_ID}, "
                f"StatusDescription = LEFT(CONCAT(COALESCE(StatusDescription, ''), %s), {MAX_DESCRIPTION}) "
                "WHERE FileId = %s"
            )
            params = (code, description, file_id)
        else:
            sql = f"UPDATE UploadedFile SET ProcessingStatusId = {_STATUS_ID}, StatusDescription = %s WHERE FileId = %s"
            params = (code, _clip(description), file_id)
        if not self.execute(sql, params):
            # MySQL reports 0 also when the row already had this value, so this is only a hint.
            LOG.debug("Status %s for FileId %s changed no row (same value, unknown FileId or status code)",
                      code, file_id)

    # ---------- per-sheet results (sql/AddSheetResultsColumn.sql) ----------
    def get_sheet_results(self, file_id: str) -> str | None:
        """UploadedFile.SheetResults JSON text (None when empty). Fails if the column does not exist."""
        rows = self.query("SELECT SheetResults FROM UploadedFile WHERE FileId = %s", (file_id,))
        value = rows[0].get("SheetResults") if rows else None
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8")
        return value or None

    def set_file_progress(self, file_id: str, percent: int) -> int:
        """UploadedFile.ProgressPercent (sql/AddProgressColumn.sql). Fails if the column does not exist."""
        return self.execute("UPDATE UploadedFile SET ProgressPercent = %s WHERE FileId = %s", (int(percent), file_id))

    def save_sheet_results(self, file_id: str, results_json: str) -> int:
        return self.execute("UPDATE UploadedFile SET SheetResults = %s WHERE FileId = %s", (results_json, file_id))

    def move_batch_files(self, batch_id: str, from_code: str, to_code: str, description: str) -> int:
        """Move every file of the batch still in ``from_code`` to ``to_code`` (used on fatal errors)."""
        sql = (
            f"UPDATE UploadedFile SET ProcessingStatusId = {_STATUS_ID}, StatusDescription = %s "
            f"WHERE BatchId = %s AND ProcessingStatusId = {_STATUS_ID}"
        )
        return self.execute(sql, (to_code, _clip(description), batch_id, from_code))

    def get_batch_status(self, batch_id: str) -> str:
        """Current RefBatchStatus.Code of the batch ('' if it has none)."""
        rows = self.query(
            "SELECT B.Code AS Code FROM FileUploadBatch FB "
            "LEFT JOIN RefBatchStatus B ON B.BatchStatusId = FB.BatchStatusId WHERE FB.BatchId = %s",
            (batch_id,),
        )
        return str(rows[0].get("Code") or "") if rows else ""

    def batch_status_codes(self) -> list[str]:
        return [str(r["Code"]) for r in self.query("SELECT Code FROM RefBatchStatus ORDER BY BatchStatusId")]

    def claim_batch(self, batch_id: str, claimable_codes, in_progress_code: str,
                    owner: str | None = None, procedure: str = "") -> str | None:
        """Atomically take a pending batch (compare-and-set, same logic as the ClaimBatch procedure).

        ``procedure``: call that stored procedure (sql/ClaimBatches.sql ClaimBatch) instead of running the
        UPDATE from Python. Returns the status it had (to put back on failure), or None when it is not
        claimable or another system took it first.
        """
        if procedure:
            return self.claim_batch_via_proc(procedure, batch_id, claimable_codes, in_progress_code, owner or "")
        rows = self.query(
            "SELECT FB.BatchStatusId AS StatusId, B.Code AS Code FROM FileUploadBatch FB "
            "LEFT JOIN RefBatchStatus B ON B.BatchStatusId = FB.BatchStatusId WHERE FB.BatchId = %s",
            (batch_id,),
        )
        if not rows or str(rows[0].get("Code") or "").upper() not in {c.upper() for c in claimable_codes}:
            return None
        target = self.query("SELECT BatchStatusId FROM RefBatchStatus WHERE Code = %s", (in_progress_code,))
        if not target:
            raise ValueError(f"Batch status code '{in_progress_code}' does not exist in RefBatchStatus")
        if owner:  # claim tracking (sql/AddClaimColumns.sql): owner, time and the status to go back to
            changed = self.execute(
                "UPDATE FileUploadBatch SET ClaimPreviousStatusId = BatchStatusId, BatchStatusId = %s, "
                "ClaimedAt = NOW(), ClaimedBy = %s WHERE BatchId = %s AND BatchStatusId = %s",
                (target[0]["BatchStatusId"], owner, batch_id, rows[0]["StatusId"]),
            )
        else:
            changed = self.execute(
                "UPDATE FileUploadBatch SET BatchStatusId = %s WHERE BatchId = %s AND BatchStatusId = %s",
                (target[0]["BatchStatusId"], batch_id, rows[0]["StatusId"]),
            )
        return str(rows[0]["Code"]) if changed == 1 else None

    def claim_batch_via_proc(self, proc: str, batch_id: str, claimable_codes, in_progress_code: str,
                             owner: str = "") -> str | None:
        """CALL ClaimBatch(id, codes, in_progress, owner, @previous, @claimed): the previous code, or None."""
        if not _IDENTIFIER.match(proc):  # the name is also used in the @_<proc>_n variable names below
            raise ValueError(f"Invalid procedure name: {proc!r}")
        args = (int(batch_id), ",".join(claimable_codes), in_progress_code, owner, None, None)
        with self._cursor(f"CALL {proc}") as cur:
            cur.callproc(proc, args)
            while cur.nextset():
                pass
            # PyMySQL keeps the arguments in @_<proc>_<n>; the procedure wrote its OUT values there
            cur.execute(f"SELECT @_{proc}_4 AS PreviousCode, @_{proc}_5 AS Claimed")
            row = cur.fetchone() or {}
        if isinstance(row, (tuple, list)):
            row = {"PreviousCode": row[0], "Claimed": row[1]}
        claimed = int(row.get("Claimed") or 0) == 1
        previous = row.get("PreviousCode")
        if isinstance(previous, (bytes, bytearray)):
            previous = previous.decode()
        LOG.info("%s(%s): %s", proc, batch_id,
                 f"claimed from {previous or '<none>'}" if claimed else f"not claimed (status {previous or '<none>'})")
        return str(previous or "") if claimed else None

    def claim_pending_via_proc(self, proc: str, limit: int, claimable_codes, in_progress_code: str,
                               exclude, owner: str = "") -> list[tuple[str, str]]:
        """CALL ClaimPendingBatches(...) (sql/ClaimBatches.sql): returns [(BatchId, PreviousCode), ...]."""
        args = (int(limit), ",".join(claimable_codes), in_progress_code, ",".join(sorted(map(str, exclude))),
                owner)
        with self._cursor(f"CALL {proc}") as cur:
            cur.callproc(proc, args)
            rows = list(cur.fetchall())
            while cur.nextset():
                pass
        LOG.info("%s claimed %d batch(es): %s", proc, len(rows), ", ".join(str(r["BatchId"]) for r in rows) or "none")
        return [(str(r["BatchId"]), str(r.get("PreviousCode") or "")) for r in rows]

    def batch_status_exists(self, code: str) -> bool:
        return bool(self.query("SELECT 1 FROM RefBatchStatus WHERE Code = %s", (code,)))

    def set_batch_status(self, batch_id: str, code: str, *, claim_owner: str | None = None,
                         clear_claim: bool = False) -> None:
        """Set the batch status. ``claim_owner``: also record the claim (ClaimedAt/By, previous status);
        ``clear_claim``: also empty the claim columns (the batch is finished or released)."""
        # Look the code up first: with a sub-select, an unknown code would silently set BatchStatusId to NULL.
        rows = self.query("SELECT BatchStatusId FROM RefBatchStatus WHERE Code = %s", (code,))
        if not rows:
            raise ValueError(f"Batch status code '{code}' does not exist in RefBatchStatus")
        LOG.debug("Setting batch %s status to %s", batch_id, code)
        if claim_owner:
            # MySQL evaluates SET left to right: ClaimPreviousStatusId gets the status from BEFORE this update
            self.execute("UPDATE FileUploadBatch SET ClaimPreviousStatusId = BatchStatusId, BatchStatusId = %s, "
                         "ClaimedAt = NOW(), ClaimedBy = %s WHERE BatchId = %s",
                         (rows[0]["BatchStatusId"], claim_owner, batch_id))
        elif clear_claim:
            self.execute("UPDATE FileUploadBatch SET BatchStatusId = %s, ClaimedAt = NULL, ClaimedBy = NULL, "
                         "ClaimPreviousStatusId = NULL WHERE BatchId = %s", (rows[0]["BatchStatusId"], batch_id))
        else:
            self.execute("UPDATE FileUploadBatch SET BatchStatusId = %s WHERE BatchId = %s",
                         (rows[0]["BatchStatusId"], batch_id))

    # ---------- claim tracking (sql/AddClaimColumns.sql) ----------
    def has_claim_columns(self) -> bool:
        rows = self.query(
            "SELECT COUNT(*) AS n FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() "
            "AND TABLE_NAME = 'FileUploadBatch' AND COLUMN_NAME IN ('ClaimedAt', 'ClaimedBy', 'ClaimPreviousStatusId')"
        )
        return bool(rows) and int(rows[0]["n"]) == 3

    def heartbeat(self, batch_id: str, owner: str) -> int:
        """Refresh ClaimedAt while the batch runs, so it is not taken for a stale claim."""
        return self.execute("UPDATE FileUploadBatch SET ClaimedAt = NOW() WHERE BatchId = %s AND ClaimedBy = %s",
                            (batch_id, owner))

    def release_stale_via_proc(self, proc: str, in_progress_code: str, stale_minutes: int,
                               fallback_code: str) -> list[dict[str, Any]]:
        """CALL ReleaseStaleClaims(in_progress, minutes, fallback) (sql/ClaimBatches.sql): the released batches
        as [{BatchId, ClaimedBy, ClaimedAt}, ...]."""
        if not _IDENTIFIER.match(proc):
            raise ValueError(f"Invalid procedure name: {proc!r}")
        with self._cursor(f"CALL {proc}") as cur:
            cur.callproc(proc, (in_progress_code, int(stale_minutes), fallback_code))
            rows = list(cur.fetchall() or [])
            while cur.nextset():
                pass
        for r in rows:
            LOG.warning("Stale claim released by %s: batch %s (claimed by %s at %s)", proc, r.get("BatchId"),
                        r.get("ClaimedBy"), r.get("ClaimedAt"))
        return rows

    def release_stale_claims(self, in_progress_code: str, stale_minutes: int,
                             fallback_code: str) -> list[dict[str, Any]]:
        """Put IN_PROGRESS batches whose ClaimedAt is older than ``stale_minutes`` back to their previous status
        (``fallback_code`` if that is unknown). Same logic as the ReleaseStaleClaims procedure."""
        ids = self.query("SELECT Code, BatchStatusId FROM RefBatchStatus WHERE Code IN (%s, %s)",
                         (in_progress_code, fallback_code))
        by_code = {str(r["Code"]).upper(): r["BatchStatusId"] for r in ids}
        if in_progress_code.upper() not in by_code or fallback_code.upper() not in by_code:
            raise ValueError(f"'{in_progress_code}' or '{fallback_code}' does not exist in RefBatchStatus")
        stale = self.query(
            "SELECT BatchId, ClaimedBy, ClaimedAt FROM FileUploadBatch WHERE BatchStatusId = %s "
            "AND ClaimedAt IS NOT NULL AND ClaimedAt < NOW() - INTERVAL %s MINUTE",
            (by_code[in_progress_code.upper()], int(stale_minutes)),
        )
        released = []
        for row in stale:
            # repeat the age check per row: a heartbeat that arrived meanwhile keeps the batch claimed
            if self.execute(
                "UPDATE FileUploadBatch SET BatchStatusId = COALESCE(ClaimPreviousStatusId, %s), ClaimedAt = NULL, "
                "ClaimedBy = NULL, ClaimPreviousStatusId = NULL WHERE BatchId = %s AND BatchStatusId = %s "
                "AND ClaimedAt < NOW() - INTERVAL %s MINUTE",
                (by_code[fallback_code.upper()], row["BatchId"], by_code[in_progress_code.upper()], int(stale_minutes)),
            ) == 1:
                released.append(row)
                LOG.warning("Released stale claim on batch %s (claimed by %s at %s, no heartbeat for over %d min)",
                            row["BatchId"], row.get("ClaimedBy"), row.get("ClaimedAt"), stale_minutes)
        return released

    def add_recognized_category(self, file_id: str, category_name: str) -> None:
        """Append to UploadedFile.CategoryRecognized (comma list, no duplicates)."""
        self.execute(
            """
            UPDATE UploadedFile
            SET CategoryRecognized = CASE
                WHEN CategoryRecognized IS NULL OR CategoryRecognized = '' THEN %s
                WHEN FIND_IN_SET(%s, CategoryRecognized) > 0 THEN CategoryRecognized
                ELSE CONCAT(CategoryRecognized, ',', %s)
            END
            WHERE FileId = %s
            """,
            (category_name, category_name, category_name, file_id),
        )

    # ---------- loading ----------
    def delete_staging_rows(self, table: str, file_id: str) -> int:
        """Remove rows a previous (failed) run left in the staging table for this file."""
        return self.execute(f"DELETE FROM {_quote_ident(table)} WHERE FileId = %s", (file_id,))

    def bulk_insert(
        self, table: str, columns: Sequence[str], rows: Iterable[Sequence[Any]], chunk_size: int = 1000
    ) -> int:
        cols = ", ".join(_quote_ident(c) for c in columns)
        marks = ", ".join(["%s"] * len(columns))
        sql = f"INSERT INTO {_quote_ident(table)} ({cols}) VALUES ({marks})"
        total = 0
        started = time.monotonic()
        chunk: list[Sequence[Any]] = []
        LOG.debug("Bulk insert into %s (%d column(s), chunks of %d)", table, len(columns), chunk_size)
        with self._cursor(f"INSERT INTO {table}") as cur:
            for row in rows:
                chunk.append(row)
                if len(chunk) >= chunk_size:
                    cur.executemany(sql, chunk)
                    total += len(chunk)
                    chunk = []
                    LOG.debug("  %s: %d row(s) inserted so far", table, total)
            if chunk:
                cur.executemany(sql, chunk)
                total += len(chunk)
        LOG.info("Bulk insert into %s: %d row(s) in %.1fs", table, total, time.monotonic() - started)
        return total

    def call_proc(self, name: str, in_args: Sequence[Any], n_out: int) -> list[Any]:
        """Call a stored procedure with IN args followed by ``n_out`` OUT args; return the OUT values."""
        args = list(in_args) + [None] * n_out
        LOG.info("Calling procedure %s(%s)", name, ", ".join(str(a) for a in in_args))
        started = time.monotonic()
        with self._cursor(f"CALL {name}") as cur:
            cur.callproc(name, args)
            LOG.debug("Procedure %s returned after %.1fs", name, time.monotonic() - started)
            while cur.nextset():  # drain any result sets before the next statement
                pass
            if not n_out:
                return []
            first_out = len(in_args)
            select = ", ".join(f"@_{name}_{i} AS o{i}" for i in range(first_out, first_out + n_out))
            cur.execute(f"SELECT {select}")
            row = cur.fetchone() or {}
            outs = [row.get(f"o{i}") for i in range(first_out, first_out + n_out)]
            LOG.info("Procedure %s finished in %.1fs, OUT=%s", name, time.monotonic() - started, outs)
            return outs

    # ---------- named locks (work across processes and machines) ----------
    def try_lock(self, name: str, timeout_seconds: int) -> bool:
        if timeout_seconds:
            LOG.debug("Waiting up to %ss for DB lock %s", timeout_seconds, name)
        started = time.monotonic()
        rows = self.query("SELECT GET_LOCK(%s, %s) AS got", (name, timeout_seconds))
        got = bool(rows and rows[0].get("got") == 1)
        waited = time.monotonic() - started
        if got:
            (LOG.info if waited >= 1 else LOG.debug)("Got DB lock %s after %.1fs", name, waited)
        else:
            LOG.warning("DB lock %s not available (waited %.1fs)", name, waited)
        return got

    def release_lock(self, name: str) -> None:
        try:
            self.query("SELECT RELEASE_LOCK(%s) AS released", (name,))
            LOG.debug("Released DB lock %s", name)
        except Exception as exc:
            LOG.warning("Could not release DB lock %s: %s", name, exc)

    @contextlib.contextmanager
    def named_lock(self, name: str, timeout_seconds: int) -> Iterator[None]:
        if not self.try_lock(name, timeout_seconds):
            raise TimeoutError(f"Could not get DB lock '{name}' within {timeout_seconds}s")
        try:
            yield
        finally:
            self.release_lock(name)


# MySQL refuses the login when it has no room for another connection:
# 1040 Too many connections (server max_connections), 1203 / 1226 per-user limits (max_user_connections).
_NO_FREE_CONNECTION = {1040, 1203, 1226}


def _is_no_free_connection(exc: BaseException) -> bool:
    args = getattr(exc, "args", ())
    return bool(args) and args[0] in _NO_FREE_CONNECTION


class EsgDb:
    """Connection factory with two limits, both of which make a worker WAIT (never fail):

    1. This process's own cap, ``max_connections`` (run.max_db_connections), so other systems using the same
       database always have room left. A worker past the cap waits until one of our connections closes.
    2. The database itself: when MySQL refuses a login because it is full (1040 / 1203 / 1226), the worker
       keeps its slot and tries again every ``connect_retry_seconds``, or at once when one of ours closes,
       until the database accepts it.
    """

    WAIT_LOG_EVERY_SECONDS = 60

    def __init__(self, cfg: DbConfig, max_connections: int = 20):
        self.cfg = cfg
        self._max = max(1, int(max_connections))
        self._retry_seconds = max(0.1, float(getattr(cfg, "connect_retry_seconds", 5)))
        self._in_use = 0  # slots taken: connections open + being opened
        self._freed = threading.Condition()  # notified whenever one of our slots is given back
        LOG.info("ESG DB %s:%s/%s, max %d connection(s) from this process; retry every %.0fs when the DB is full",
                 cfg.host, cfg.port, cfg.database, self._max, self._retry_seconds)

    def _open(self):
        import pymysql
        from pymysql.cursors import DictCursor

        return pymysql.connect(
            host=self.cfg.host,
            port=self.cfg.port,
            user=self.cfg.user,
            password=self.cfg.password,
            database=self.cfg.database,
            charset="utf8mb4",
            autocommit=True,
            connect_timeout=self.cfg.connect_timeout,
            cursorclass=DictCursor,
            # esg_db.lock_wait_timeout_seconds: fail a write blocked by another session's lock sooner than
            # the server default (innodb_lock_wait_timeout, normally 50s). 0 = server default.
            init_command=(f"SET SESSION innodb_lock_wait_timeout = {int(self.cfg.lock_wait_timeout_seconds)}"
                          if self.cfg.lock_wait_timeout_seconds > 0 else None),
        )

    # ------------------------------------------------------------------ slots (this process's cap)
    def _take_slot(self, wait: bool) -> bool:
        started = next_log = None
        with self._freed:
            while self._in_use >= self._max:
                if not wait:
                    return False
                now = time.monotonic()
                if started is None:
                    started, next_log = now, now + self.WAIT_LOG_EVERY_SECONDS
                    LOG.warning("All %d DB connections allowed for this process are in use; waiting for a free one",
                                self._max)
                elif now >= next_log:
                    next_log = now + self.WAIT_LOG_EVERY_SECONDS
                    LOG.warning("Still waiting for one of the %d DB connections after %.0fs", self._max,
                                now - started)
                self._freed.wait(self.WAIT_LOG_EVERY_SECONDS)
            self._in_use += 1
        if started is not None:
            LOG.info("Got a DB connection slot after waiting %.1fs", time.monotonic() - started)
        return True

    def _give_slot_back(self) -> None:
        with self._freed:
            self._in_use -= 1
            self._freed.notify_all()

    # ------------------------------------------------------------------ the database's own limit
    def _connect(self, wait: bool):
        """Open a connection (a slot is already taken). DB full: wait for room (``wait``) or return None."""
        started = next_log = None
        while True:
            try:
                conn = self._open()
            except Exception as exc:
                if not _is_no_free_connection(exc):
                    LOG.error("Cannot connect to ESG DB %s:%s/%s: %s", self.cfg.host, self.cfg.port,
                              self.cfg.database, exc)
                    raise
                if not wait:
                    LOG.debug("ESG DB has no free connection right now (%s)", exc)
                    return None
                now = time.monotonic()
                if started is None:
                    started, next_log = now, now + self.WAIT_LOG_EVERY_SECONDS
                    LOG.warning("ESG DB refused a new connection (%s); waiting for the database to have room "
                                "(%d of %d connection(s) used by this process)", exc, self._in_use, self._max)
                elif now >= next_log:
                    next_log = now + self.WAIT_LOG_EVERY_SECONDS
                    LOG.warning("Still waiting for the ESG DB to accept a connection after %.0fs", now - started)
                with self._freed:
                    self._freed.wait(self._retry_seconds)
                continue
            if started is not None:
                LOG.info("ESG DB accepted the connection after waiting %.1fs", time.monotonic() - started)
            return conn

    # ------------------------------------------------------------------ public
    @contextlib.contextmanager
    def session(self) -> Iterator[EsgSession]:
        """A connection; waits as long as needed for a slot of this process and for room in the database."""
        self._take_slot(wait=True)
        try:
            conn = self._connect(wait=True)
        except BaseException:
            self._give_slot_back()
            raise
        with self._opened(conn) as s:
            yield s

    @contextlib.contextmanager
    def try_session(self) -> Iterator[EsgSession | None]:
        """An EXTRA connection only if a slot is free AND the database accepts it right now, else None.

        Never waits. Used for parallel bulk inserts and heartbeats: a worker that already holds a connection
        must not wait for more, or all workers could end up waiting for each other.
        """
        if not self._take_slot(wait=False):
            yield None
            return
        try:
            conn = self._connect(wait=False)
        except BaseException:
            self._give_slot_back()
            raise
        if conn is None:
            self._give_slot_back()
            yield None
            return
        with self._opened(conn) as s:
            yield s

    @contextlib.contextmanager
    def _opened(self, conn) -> Iterator[EsgSession]:
        """Use an open connection (its slot is taken); always closes it and gives the slot back."""
        LOG.debug("DB connection opened (%d/%d in use)", self._in_use, self._max)
        try:
            yield EsgSession(conn)
        finally:
            with contextlib.suppress(Exception):
                conn.close()
            self._give_slot_back()

    def log_server_limits(self) -> None:
        """Log the database's connection limits and how many connections it has now (information only)."""
        try:
            with self.session() as s:
                limits = s.query("SELECT @@max_connections AS max_connections, "
                                 "@@max_user_connections AS max_user_connections")[0]
                status = s.query("SHOW GLOBAL STATUS LIKE 'Threads_connected'")
            connected = status[0].get("Value") if status else "?"
            LOG.info("ESG DB connections: %s in use, server max_connections %s, max_user_connections %s (0 = no limit)",
                     connected, limits.get("max_connections"), limits.get("max_user_connections"))
        except Exception as exc:
            LOG.warning("Could not read the ESG DB connection limits: %s", exc)
