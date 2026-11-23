"""Energy Manager / NAV extraction for Daisy files (retrieve half of ST_Retrieve_and_Load_*Data)."""
from __future__ import annotations

import csv
import re
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from .config import SourceConfig

ENERGY_MANAGER = "energymanager"
NAV = "nav"

_PLACEHOLDER_RE = re.compile(r"'?@(ClientName|ReportStartDate|ReportEndDate)'?", re.IGNORECASE)


@dataclass(frozen=True)
class RetrieveSpec:
    em_query: str     # <ENV>_<em_query>_EM_QUERY.txt
    nav_proc: str     # NAV stored procedure
    csv_prefix: str


RETRIEVE_SPECS = {
    "portfolio": RetrieveSpec("PORTFOLIO", "esg.PortfolioData", "Portfolio"),
    "invoices": RetrieveSpec("INVOICE", "esg.InvoiceData", "Invoices"),
    "half hourly data": RetrieveSpec("HHD", "esg.hhdata", "HHD"),
}


class SourceError(Exception):
    pass


def normalize_source_type(value: str) -> str:
    """'EnergyManager', 'Energy Manager', 'energy manager' -> 'energymanager' (fixes the UAT/PROD mismatch)."""
    return re.sub(r"\s+", "", value or "").lower()


def to_date(value) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()


from .logging_setup import module_logger  # noqa: E402

LOG = module_logger(__name__)


class SourceSystems:
    def __init__(self, cfg: SourceConfig):
        self.cfg = cfg
        LOG.info("Source systems ready: environment %s, EM query folder %s, EM %s, NAV %s",
                 cfg.environment, cfg.query_dir,
                 "configured" if cfg.em_connection_string else "NOT configured",
                 "configured" if cfg.nav_connection_string else "NOT configured")

    def load_em_query(self, spec: RetrieveSpec) -> str:
        path = self.cfg.query_dir / f"{self.cfg.environment}_{spec.em_query}_EM_QUERY.txt"
        if not path.exists():
            raise SourceError(f"Energy Manager query file not found: {path.name}")
        sql = path.read_text(encoding="utf-8-sig").strip()
        if not sql:
            raise SourceError(f"Energy Manager query file is empty: {path.name}")
        LOG.info("Using Energy Manager query file %s (%d characters)", path.name, len(sql))
        return sql

    def export_to_csv(
        self, source_type: str, spec: RetrieveSpec, client: str, start, end, out_csv: Path
    ) -> int:
        """Run the extraction and write a CSV with a header. Returns the data row count.

        No file is written when no rows come back (same as the bot's 'no blank CSV' option).
        """
        kind = normalize_source_type(source_type)
        start_d, end_d = to_date(start), to_date(end)
        if kind == ENERGY_MANAGER:
            conn_str = self.cfg.em_connection_string
            sql, params = self._bind_em_query(self.load_em_query(spec), client, start_d, end_d)
        elif kind == NAV:
            conn_str = self.cfg.nav_connection_string
            sql = f"{{CALL {spec.nav_proc} (?, ?, ?)}}"
            params = [client, start_d.strftime("%Y%m%d"), end_d.strftime("%Y%m%d")]
        else:
            raise SourceError(f"Unsupported data source type '{source_type}'")
        if not conn_str:
            raise SourceError(f"No connection string configured for data source '{source_type}'")
        LOG.info("Querying %s (%s) for client %s, %s to %s", kind, spec.nav_proc if kind == NAV else spec.em_query,
                 client, start_d, end_d)
        LOG.debug("Source SQL: %s | params=%s", " ".join(sql.split())[:300], params)

        import pyodbc

        started = time.monotonic()
        try:
            conn = pyodbc.connect(conn_str, timeout=30)
        except Exception as exc:
            LOG.error("Cannot connect to %s: %s", kind, exc)
            raise
        try:
            conn.timeout = self.cfg.query_timeout_seconds
            cur = conn.cursor()
            cur.execute(sql, params)
            LOG.debug("%s query executed in %.1fs", kind, time.monotonic() - started)
            # Skip row-count-only results that stored procedures can emit before the data set.
            while cur.description is None and cur.nextset():
                pass
            if cur.description is None:
                LOG.info("%s returned no result set", kind)
                return 0
            header = [d[0] for d in cur.description]
            LOG.debug("%s columns: %s", kind, header)
            count = 0
            fh = None
            try:
                while True:
                    batch = cur.fetchmany(5000)
                    if not batch:
                        break
                    if fh is None:
                        out_csv.parent.mkdir(parents=True, exist_ok=True)
                        fh = out_csv.open("w", encoding="utf-8", newline="")
                        writer = csv.writer(fh)
                        writer.writerow(header)
                    writer.writerows(["" if v is None else v for v in row] for row in batch)
                    count += len(batch)
                    LOG.debug("  %d row(s) fetched so far", count)
            finally:
                if fh is not None:
                    fh.close()
            LOG.info("%s returned %d row(s) in %.1fs", kind, count, time.monotonic() - started)
            return count
        finally:
            conn.close()

    @staticmethod
    def _bind_em_query(sql: str, client: str, start: date, end: date) -> tuple[str, list]:
        """Turn '@ClientName' / '@ReportStartDate' / '@ReportEndDate' into ? parameters.

        The bot did a plain text replace, which breaks on client names with an apostrophe.
        """
        values = {"clientname": client, "reportstartdate": start.isoformat(), "reportenddate": end.isoformat()}
        params: list = []

        def repl(m: re.Match) -> str:
            params.append(values[m.group(1).lower()])
            return "?"

        return _PLACEHOLDER_RE.sub(repl, sql), params
