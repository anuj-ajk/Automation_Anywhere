"""ESG DB (MySQL) access.

Every query is parameterised, so the quote/backslash escaping the AA bots did by hand is
not needed. Each unit of work opens its own short-lived connection (PyMySQL connections
must not be shared between threads); a semaphore caps the total number of connections.
"""
from __future__ import annotations

import contextlib
import threading
from typing import Any, Iterable, Iterator, Sequence

from .config import DbConfig

MAX_DESCRIPTION = 2000

_BATCH_FILES_SQL = """
SELECT FB.BatchId, FB.ReportId, R.ClientId, R.ReportStart, R.ReportEnd,
       BP.BusinessPartnerName, UF.CategorySelected, UF.FileId, UF.FileName,
       UF.StoragePath, UF.FileVersion
FROM FileUploadBatch FB
JOIN UploadedFile UF          ON UF.BatchId = FB.BatchId
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
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return list(cur.fetchall())

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> int:
        with self.conn.cursor() as cur:
            return cur.execute(sql, params)

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
        self.execute(sql, params)

    def move_batch_files(self, batch_id: str, from_code: str, to_code: str, description: str) -> int:
        """Move every file of the batch still in ``from_code`` to ``to_code`` (used on fatal errors)."""
        sql = (
            f"UPDATE UploadedFile SET ProcessingStatusId = {_STATUS_ID}, StatusDescription = %s "
            f"WHERE BatchId = %s AND ProcessingStatusId = {_STATUS_ID}"
        )
        return self.execute(sql, (to_code, _clip(description), batch_id, from_code))

    def set_batch_status(self, batch_id: str, code: str) -> None:
        self.execute(
            "UPDATE FileUploadBatch SET BatchStatusId = "
            "(SELECT b.BatchStatusId FROM RefBatchStatus b WHERE b.Code = %s) WHERE BatchId = %s",
            (code, batch_id),
        )

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
    def bulk_insert(
        self, table: str, columns: Sequence[str], rows: Iterable[Sequence[Any]], chunk_size: int = 1000
    ) -> int:
        cols = ", ".join(_quote_ident(c) for c in columns)
        marks = ", ".join(["%s"] * len(columns))
        sql = f"INSERT INTO {_quote_ident(table)} ({cols}) VALUES ({marks})"
        total = 0
        chunk: list[Sequence[Any]] = []
        with self.conn.cursor() as cur:
            for row in rows:
                chunk.append(row)
                if len(chunk) >= chunk_size:
                    cur.executemany(sql, chunk)
                    total += len(chunk)
                    chunk = []
            if chunk:
                cur.executemany(sql, chunk)
                total += len(chunk)
        return total

    def call_proc(self, name: str, in_args: Sequence[Any], n_out: int) -> list[Any]:
        """Call a stored procedure with IN args followed by ``n_out`` OUT args; return the OUT values."""
        args = list(in_args) + [None] * n_out
        with self.conn.cursor() as cur:
            cur.callproc(name, args)
            while cur.nextset():  # drain any result sets before the next statement
                pass
            if not n_out:
                return []
            first_out = len(in_args)
            select = ", ".join(f"@_{name}_{i} AS o{i}" for i in range(first_out, first_out + n_out))
            cur.execute(f"SELECT {select}")
            row = cur.fetchone() or {}
            return [row.get(f"o{i}") for i in range(first_out, first_out + n_out)]

    # ---------- named locks (work across processes and machines) ----------
    def try_lock(self, name: str, timeout_seconds: int) -> bool:
        rows = self.query("SELECT GET_LOCK(%s, %s) AS got", (name, timeout_seconds))
        return bool(rows and rows[0].get("got") == 1)

    def release_lock(self, name: str) -> None:
        with contextlib.suppress(Exception):
            self.query("SELECT RELEASE_LOCK(%s) AS released", (name,))

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
        with self._slots:
            conn = self._open()
            try:
                yield EsgSession(conn)
            finally:
                with contextlib.suppress(Exception):
                    conn.close()
