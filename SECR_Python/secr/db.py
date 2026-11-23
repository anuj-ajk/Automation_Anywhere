"""ESG DB (MySQL) access.

Every query is parameterised, so the quote/backslash escaping the AA bots did by hand is
not needed. Each unit of work opens its own short-lived connection (PyMySQL connections
must not be shared between threads); a semaphore caps the total number of connections.
"""
from __future__ import annotations

import contextlib
import threading
import time
from typing import Any, Iterable, Iterator, Sequence

from .config import DbConfig
from .logging_setup import module_logger

LOG = module_logger(__name__)
SLOW_QUERY_SECONDS = 5.0


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
    def query(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        started = time.monotonic()
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            rows = list(cur.fetchall())
        self._trace("query", sql, params, started, f"{len(rows)} row(s)")
        return rows

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> int:
        started = time.monotonic()
        with self.conn.cursor() as cur:
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

    def move_batch_files(self, batch_id: str, from_code: str, to_code: str, description: str) -> int:
        """Move every file of the batch still in ``from_code`` to ``to_code`` (used on fatal errors)."""
        sql = (
            f"UPDATE UploadedFile SET ProcessingStatusId = {_STATUS_ID}, StatusDescription = %s "
            f"WHERE BatchId = %s AND ProcessingStatusId = {_STATUS_ID}"
        )
        return self.execute(sql, (to_code, _clip(description), batch_id, from_code))

    def batch_status_exists(self, code: str) -> bool:
        return bool(self.query("SELECT 1 FROM RefBatchStatus WHERE Code = %s", (code,)))

    def set_batch_status(self, batch_id: str, code: str) -> None:
        # Look the code up first: with a sub-select, an unknown code would silently set BatchStatusId to NULL.
        rows = self.query("SELECT BatchStatusId FROM RefBatchStatus WHERE Code = %s", (code,))
        if not rows:
            raise ValueError(f"Batch status code '{code}' does not exist in RefBatchStatus")
        LOG.debug("Setting batch %s status to %s", batch_id, code)
        self.execute("UPDATE FileUploadBatch SET BatchStatusId = %s WHERE BatchId = %s",
                     (rows[0]["BatchStatusId"], batch_id))

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
        with self.conn.cursor() as cur:
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
        with self.conn.cursor() as cur:
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


class EsgDb:
    """Connection factory with a global connection cap."""

    def __init__(self, cfg: DbConfig, max_connections: int):
        self.cfg = cfg
        self._slots = threading.BoundedSemaphore(max_connections)
        self._max = max_connections
        self._open_count = 0
        self._count_lock = threading.Lock()
        LOG.info("ESG DB %s:%s/%s, max %d connection(s)", cfg.host, cfg.port, cfg.database, max_connections)

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
        )

    @contextlib.contextmanager
    def session(self) -> Iterator[EsgSession]:
        started = time.monotonic()
        if not self._slots.acquire(timeout=5):
            LOG.warning("All %d DB connections are in use; waiting for a free one", self._max)
            self._slots.acquire()
            LOG.info("Got a DB connection after waiting %.1fs", time.monotonic() - started)
        with self._opened() as s:
            yield s

    @contextlib.contextmanager
    def try_session(self) -> Iterator[EsgSession | None]:
        """An EXTRA connection only if one is free right now, else None (never waits).

        Used for parallel bulk inserts: a worker that already holds a connection must not wait for more,
        or all workers could end up waiting for each other.
        """
        if not self._slots.acquire(blocking=False):
            yield None
            return
        with self._opened() as s:
            yield s

    @contextlib.contextmanager
    def _opened(self) -> Iterator[EsgSession]:
        """Open a connection for a slot that is already taken; always gives the slot back."""
        try:
            try:
                conn = self._open()
            except Exception as exc:
                LOG.error("Cannot connect to ESG DB %s:%s/%s: %s", self.cfg.host, self.cfg.port,
                          self.cfg.database, exc)
                raise
            with self._count_lock:
                self._open_count += 1
                in_use = self._open_count
            LOG.debug("DB connection opened (%d/%d in use)", in_use, self._max)
            try:
                yield EsgSession(conn)
            finally:
                with contextlib.suppress(Exception):
                    conn.close()
                with self._count_lock:
                    self._open_count -= 1
        finally:
            self._slots.release()
