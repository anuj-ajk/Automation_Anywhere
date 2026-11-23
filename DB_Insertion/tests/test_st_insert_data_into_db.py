"""Tests for st_insert_data_into_db with a fake MySQL connection (no database needed)."""
from __future__ import annotations

import csv
import json
import shutil
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import st_insert_data_into_db as st  # noqa: E402

SAMPLE = HERE / "Portfolio_ESG_Normalized_Data.csv"


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def executemany(self, sql, rows):
        self.conn.inserted.extend(rows)
        self.conn.sql = sql

    def callproc(self, name, args):
        self.conn.calls.append((name, list(args)))
        message = self.conn.messages.get(name, "Data loaded successfully")
        n_in = len(args) - (2 if name == "CalculateInvoiceData" else 1)
        return tuple(args[:n_in]) + ((message, "OK") if name == "CalculateInvoiceData" else (message,))

    def execute(self, sql, params):  # only the staging row count is executed directly
        self.conn.counted.append((sql, params))
        staged = [r for r in self.conn.inserted if r[-1] == params[0]]
        self._count = 0 if (self.conn.procs_clear_staging and self.conn.calls) else len(staged)

    def fetchone(self):
        return (self._count,)

    def stored_results(self):
        return iter(())

    def close(self):
        pass


class FakeConn:
    def __init__(self, messages=None, procs_clear_staging=False):
        self.inserted, self.calls, self.messages = [], [], messages or {}
        self.counted, self.procs_clear_staging = [], procs_clear_staging
        self.sql = ""
        self.committed = self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.committed = True

    def rollback(self):
        pass

    def close(self):
        self.closed = True


def _args(tmp_path, **extra):
    args = {"ioCSVFilePath": str(SAMPLE), "ioInputFolderPath": str(tmp_path / "work"), "ioFileId": 77.0,
            "ioReportId": "5", "ESGDBServerName": "db.local", "DatabaseName": "esgdb", "MySQL_Port": "3306",
            "created_by": "tester", "log_dir": str(tmp_path / "logs"),
            "secrets": {"SECR_DB_USER": "u", "SECR_DB_PASSWORD": "p"}}
    args.update(extra)
    return args


def _patch_connect(monkeypatch, conn):
    seen = {}

    def fake_connect(host, port, database, user, password):
        seen.update(host=host, port=port, database=database, user=user)
        return conn

    monkeypatch.setattr(st, "connect", fake_connect)
    return seen


def test_full_load_with_aa_dictionary(tmp_path, monkeypatch):
    conn = FakeConn()
    seen = _patch_connect(monkeypatch, conn)
    out = json.loads(st.insert_data_into_db(_args(tmp_path)))

    assert out["status"] == "SUCCESS", out
    assert out["nInsertedRecords"] == 2 and out["category"] == "Portfolio"
    assert seen == {"host": "db.local", "port": 3306, "database": "esgdb", "user": "u"}
    assert "`esgdb`.`Staging_Portfolio`" in conn.sql and "`Current_Supplier_name`" in conn.sql
    assert all(row[-1] == "77" for row in conn.inserted)          # 77.0 from AA -> "77"
    assert conn.calls == [("LoadPortfolioData", ["77", "5", None]),
                          ("UpdatePortfolioDerivedFields", ["5", None])]
    assert conn.closed

    cleaned = Path(out["cleaned_file"])
    assert cleaned.name == "Cleaned_Portfolio_77.csv"
    rows = list(csv.reader(cleaned.open(encoding="utf-8")))
    assert rows[0][-1] == "FileId" and rows[2][2] == "Test Site Two" and rows[2][20] == "Post code checked"
    assert rows[1][4] == "NA"
    log = Path(out["log_file"]).read_text(encoding="utf-8")
    assert "SECR_DB_PASSWORD=set" in log and "=p" not in log


def test_category_override_and_invoice_procs(tmp_path, monkeypatch):
    csv_file = tmp_path / "some_link_name.csv"
    cols = [c if isinstance(c, str) else c[0] for c in st.COLUMN_MAPPINGS["Staging_Invoices"] if c != "FileId"]
    csv_file.write_text(",".join(cols) + "\n" + ",".join("x" for _ in cols) + "\n", encoding="utf-8")
    conn = FakeConn({"LoadInvoiceData": "Load successful", "CalculateInvoiceData": "Calculation successful"})
    _patch_connect(monkeypatch, conn)
    out = json.loads(st.insert_data_into_db(_args(tmp_path, ioCSVFilePath=str(csv_file), category="invoice")))
    assert out["status"] == "SUCCESS", out
    assert [c[0] for c in conn.calls] == ["LoadInvoiceData", "CalculateInvoiceData"]
    assert conn.calls[1][1][:2] == ["5", "tester"]


def test_sp_failure_is_reported(tmp_path, monkeypatch):
    _patch_connect(monkeypatch, FakeConn({"LoadPortfolioData": "Error: duplicate key"}))
    out = json.loads(st.insert_data_into_db(_args(tmp_path)))
    assert out["status"] == "FAILED" and "LoadPortfolioData" in out["sErrorMessage"]


def test_header_only_csv_is_no_data(tmp_path, monkeypatch):
    csv_file = tmp_path / "Metrics_ESG_x.csv"
    csv_file.write_text("Company_Level_Metric_Type,Units\n", encoding="utf-8")
    monkeypatch.setattr(st, "connect", lambda *a: (_ for _ in ()).throw(AssertionError("no DB expected")))
    out = json.loads(st.insert_data_into_db(_args(tmp_path, ioCSVFilePath=str(csv_file))))
    assert out["status"] == "NO_DATA"


def test_dry_run_needs_no_database(tmp_path, monkeypatch):
    monkeypatch.setattr(st, "connect", lambda *a: (_ for _ in ()).throw(AssertionError("no DB expected")))
    out = json.loads(st.insert_data_into_db(_args(tmp_path, dry_run="true", secrets=None)))
    assert out["status"] == "DRY_RUN" and Path(out["cleaned_file"]).exists()


def test_bad_input_comes_back_as_json(tmp_path, monkeypatch):
    monkeypatch.delenv("SECR_DB_USER", raising=False)
    out = json.loads(st.insert_data_into_db({"log_dir": str(tmp_path)}))
    assert out["status"] == "FAILED" and "csv_path" in out["sErrorMessage"]
    out = json.loads(st.insert_data_into_db({"csv_path": "x.csv", "secrets": {"PATH": "x"}}))
    assert out["status"] == "FAILED" and "secrets key" in out["sErrorMessage"]
    out = json.loads(st.insert_data_into_db(_args(tmp_path, ioFileId="")))
    assert out["status"] == "FAILED" and "file_id" in out["sErrorMessage"]


def test_unset_secret_is_not_written_as_none(monkeypatch):
    monkeypatch.setenv("SECR_DB_USER", "from-machine")
    st._apply_secrets({"SECR_DB_USER": None})
    assert st.os.environ["SECR_DB_USER"] == "from-machine"


def test_dry_run_reports_missing_columns(tmp_path):
    csv_file = tmp_path / "Metrics_ESG_x.csv"
    csv_file.write_text("Company_Level_Metric_Type,Units\nA,1\n", encoding="utf-8")
    out = json.loads(st.insert_data_into_db(_args(tmp_path, ioCSVFilePath=str(csv_file), dry_run=True)))
    assert out["status"] == "FAILED" and "Date_From" in out["sErrorMessage"]


def test_staging_counts_show_where_rows_went(tmp_path, monkeypatch):
    conn = FakeConn(procs_clear_staging=True)
    _patch_connect(monkeypatch, conn)
    out = json.loads(st.insert_data_into_db(_args(tmp_path)))
    assert out["status"] == "SUCCESS", out
    assert out["nStagingRowsAfterInsert"] == 2 and out["nStagingRowsAfterProcs"] == 0
    assert "`esgdb`.`Staging_Portfolio` WHERE FileId = %s" in conn.counted[0][0] and conn.counted[0][1] == ("77",)
    log = Path(out["log_file"]).read_text(encoding="utf-8")
    assert "moved/removed the staging rows" in log


def test_dry_run_is_clearly_marked(tmp_path):
    out = json.loads(st.insert_data_into_db(_args(tmp_path, dry_run=True)))
    assert out["status"] == "DRY_RUN" and "nothing was written" in out["sBotComments"]
    assert "DRY RUN - NOTHING IS WRITTEN" in Path(out["log_file"]).read_text(encoding="utf-8")
