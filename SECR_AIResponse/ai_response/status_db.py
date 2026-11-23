"""UploadedFile status / CategoryRecognized updates done by the bot (ESG DB, MySQL).

All statements are parameterised (the bot pasted values into the SQL and escaped quotes by hand).
"""
from __future__ import annotations

import contextlib
import time

from .config import AIConfig
from .logging_setup import get_logger

LOG = get_logger(__name__)
MAX_DESCRIPTION = 2000

NORMALIZING = "NORMALIZING"
NORMALIZATION_COMPLETED = "NORMALIZATION_COMPLETED"
PARTIAL_FAILURE = "PARTIAL_FAILURE"
FAILED_WHILE_NORMALIZATION = "FAILED_WHILE_NORMALIZATION"

_STATUS_ID = "(SELECT s.FileProcessingStatusId FROM RefFileProcessingStatus s WHERE s.Code = %s)"


def roll_up(success: int, failed: int) -> str:
    """Same rule as the bot: no failure -> COMPLETED, no success -> FAILED, else PARTIAL_FAILURE."""
    if failed == 0:
        return NORMALIZATION_COMPLETED
    if success == 0:
        return FAILED_WHILE_NORMALIZATION
    return PARTIAL_FAILURE


class StatusDb:
    def __init__(self, cfg: AIConfig):
        self.cfg = cfg
        LOG.info("Status updates go to ESG DB %s:%s/%s", cfg.db_host, cfg.db_port, cfg.db_name)

    @contextlib.contextmanager
    def _cursor(self):
        import pymysql  # one short connection per update: PyMySQL connections are not thread-safe

        conn = pymysql.connect(host=self.cfg.db_host, port=self.cfg.db_port, user=self.cfg.db_user,
                               password=self.cfg.db_password, database=self.cfg.db_name,
                               charset="utf8mb4", autocommit=True, connect_timeout=30)
        try:
            with conn.cursor() as cur:
                yield cur
        finally:
            with contextlib.suppress(Exception):
                conn.close()

    def _execute(self, what: str, sql: str, params: tuple) -> None:
        started = time.monotonic()
        try:
            with self._cursor() as cur:
                rows = cur.execute(sql, params)
        except Exception as exc:  # like the bot's nested try blocks: a failed status write never stops the run
            LOG.error("Could not %s: %s", what, exc)
            return
        LOG.debug("%s: %s row(s) in %.2fs", what, rows, time.monotonic() - started)

    def set_status(self, file_id: str, code: str, append_description: str | None = None) -> None:
        LOG.info("Status -> %s%s", code, f" ({append_description.strip()})" if append_description else "")
        if append_description:
            sql = (f"UPDATE UploadedFile SET ProcessingStatusId = {_STATUS_ID}, "
                   f"StatusDescription = LEFT(CONCAT(COALESCE(StatusDescription, ''), %s), {MAX_DESCRIPTION}) "
                   "WHERE FileId = %s")
            params: tuple = (code, append_description, file_id)
        else:
            sql = f"UPDATE UploadedFile SET ProcessingStatusId = {_STATUS_ID} WHERE FileId = %s"
            params = (code, file_id)
        self._execute(f"set status {code} for file {file_id}", sql, params)

    def add_category(self, file_id: str, category_name: str) -> None:
        LOG.info("CategoryRecognized += %s", category_name)
        self._execute(
            f"add category {category_name} for file {file_id}",
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


class NoStatusDb:
    """Used when update_db is false: only logs what would have been written."""

    def set_status(self, file_id: str, code: str, append_description: str | None = None) -> None:
        LOG.info("(update_db off) status would be %s%s", code,
                 f" ({append_description.strip()})" if append_description else "")

    def add_category(self, file_id: str, category_name: str) -> None:
        LOG.info("(update_db off) category recognized: %s", category_name)
