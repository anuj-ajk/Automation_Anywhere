"""Offline tests: fake DB / S3 / Odin / source systems, real threading, CSV and Excel code.

Run:  python -m pytest -q tests      (or: python tests/test_pipeline.py)
"""
from __future__ import annotations

import contextlib
import csv
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from secr import status as st  # noqa: E402
from secr.ai_client import parse_agent_response  # noqa: E402
from secr.batch import BatchProcessor  # noqa: E402
from secr.csv_clean import clean_csv  # noqa: E402
from secr.daisy import DaisyFlow  # noqa: E402
from secr.excel_split import split_workbook  # noqa: E402
from secr.loader import NORMALIZED_SPECS, StagingLoader  # noqa: E402
from secr.logging_setup import get_logger  # noqa: E402
from secr.normalize import NormalizeFlow  # noqa: E402
from secr.orchestrator import exit_code, run_batches  # noqa: E402
from secr.source_systems import SourceSystems  # noqa: E402


# ---------------------------------------------------------------- fakes
class FakeSession:
    def __init__(self, db):
        self.db = db

    def fetch_batch_files(self, batch_id):
        return [dict(r) for r in self.db.batches.get(batch_id, [])]

    def data_source_type(self, client):
        return "Energy Manager"  # with a space on purpose: must still be recognised

    def set_file_status(self, file_id, code, description=None, *, append=False, clear=False):
        with self.db.lock:
            self.db.status[file_id] = code
            self.db.history.setdefault(file_id, []).append(code)

    def move_batch_files(self, *a):
        return 0

    def set_batch_status(self, batch_id, code):
        with self.db.lock:
            self.db.batch_status[batch_id] = code

    def add_recognized_category(self, file_id, name):
        pass

    def bulk_insert(self, table, columns, rows, chunk_size=1000):
        rows = list(rows)
        with self.db.lock:
            self.db.inserted[table] = self.db.inserted.get(table, 0) + len(rows)
        return len(rows)

    def call_proc(self, name, args, n_out):
        return ["Data loaded successfully", "SUCCESS"][:n_out]

    def try_lock(self, name, timeout):
        with self.db.lock:
            if name in self.db.named_locks:
                return False
            self.db.named_locks.add(name)
            return True

    def release_lock(self, name):
        with self.db.lock:
            self.db.named_locks.discard(name)

    @contextlib.contextmanager
    def named_lock(self, name, timeout):
        while not self.try_lock(name, timeout):
            time.sleep(0.001)
        try:
            yield
        finally:
            self.release_lock(name)


class FakeDb:
    def __init__(self, batches):
        self.batches = batches
        self.lock = threading.Lock()
        self.status, self.history, self.batch_status = {}, {}, {}
        self.inserted, self.named_locks = {}, set()

    @contextlib.contextmanager
    def session(self):
        yield FakeSession(self)


class FakeStorage:
    def __init__(self, src: Path):
        self.src = src

    def download(self, storage_path, destination: Path):
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(self.src.read_bytes())
        time.sleep(0.02)
        return destination


class FakeAI:
    def __init__(self, csv_url):
        self.csv_url = csv_url
        self.polls = {}

    def create_chat(self, name):
        return f"chat-{name}-{threading.get_ident()}-{time.monotonic_ns()}"

    def send_file(self, chat_id, path):
        return True

    def get_csv_links(self, chat_id):
        self.polls[chat_id] = self.polls.get(chat_id, 0) + 1
        if self.polls[chat_id] < 2:
            return None  # "still working" on the first poll
        return {"metrics": self.csv_url}


class FakeSources(SourceSystems):
    def __init__(self):
        pass

    def export_to_csv(self, source_type, spec, client, start, end, out_csv):
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        out_csv.write_text("Client,Site ID\nACME,1\nACME,2\n", encoding="utf-8")
        return 2


# ---------------------------------------------------------------- helpers
def _metrics_csv(path: Path) -> Path:
    cols = [c for c, _ in NORMALIZED_SPECS["metrics"].columns if c != "FileId"]
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(cols)
        w.writerow(["Revenue", "10", "2024-04-01", "2025-03-31", "GBP", "High", "", "", "", "", ""])
    return path


def _workbook(path: Path) -> Path:
    from openpyxl import Workbook

    wb = Workbook()
    wb.active.title = "Metrics"
    wb.active.append(["a", "b"])
    wb.active.append([1, 2])
    wb.create_sheet("Instructions").append(["skip me"])
    wb.create_sheet("Portfolio").append(["x"])
    wb.save(path)
    return path


def _row(batch, file_id, name="data.xlsx", category=None):
    return {"BatchId": batch, "ReportId": 5, "BusinessPartnerName": "ACME", "ReportStart": "2024-04-01",
            "ReportEnd": "2025-03-31", "FileId": file_id, "FileName": name, "StoragePath": f"k/{name}",
            "CategorySelected": category}


def _build(tmp: Path, batches, files_per_batch=4):
    import requests_stub  # noqa: F401  (patches secr.normalize.download_link)

    db = FakeDb(batches)
    log = get_logger()
    loader = StagingLoader("tester", True, 5, 1000, log)
    normalize = NormalizeFlow(db, FakeStorage(_workbook(tmp / "src.xlsx")),
                              FakeAI(str(_metrics_csv(tmp / "metrics.csv"))), loader,
                              poll_interval=0, max_poll_attempts=5)
    daisy = DaisyFlow(db, FakeSources(), loader)
    return db, BatchProcessor(db, normalize, daisy, tmp / "work", files_per_batch, False)


# ---------------------------------------------------------------- tests
def test_clean_csv(tmp_path):
    src = tmp_path / "in.csv"
    src.write_text('a,b\n"x\ny",\n', encoding="utf-8")
    res = clean_csv(src, tmp_path / "out.csv", file_id="77")
    rows = list(csv.reader((tmp_path / "out.csv").open(encoding="utf-8")))
    assert rows == [["a", "b", "FileId"], ["x y", "NA", "77"]]
    assert res.rows_processed == 2 and res.data_rows == 1 and res.blanks_filled == 1


def test_split_skips_instruction_sheets(tmp_path):
    results = split_workbook(_workbook(tmp_path / "w.xlsx"), tmp_path / "out")
    assert [r.sheet_name for r in results] == ["Metrics", "Portfolio"]
    assert all(r.file_path.exists() for r in results)
    csv_file = tmp_path / "x.csv"
    csv_file.write_text("a\n1\n")
    assert split_workbook(csv_file, tmp_path / "out")[0].file_path == csv_file


def test_parse_agent_response():
    assert parse_agent_response({"messages": []}) is None
    body = {"messages": [{"json_response": '{"normalized_csv_file_links": {"invoice": "u", "metrics": ""}}'}]}
    assert parse_agent_response(body) == {"invoice": "u"}


def test_em_query_binding():
    sql, params = SourceSystems._bind_em_query(
        "WHERE a = '@ClientName' AND d >= '@ReportStartDate' AND d <= '@ReportEndDate' OR x > '@ReportStartDate'",
        "O'Brien Ltd", __import__("datetime").date(2024, 4, 1), __import__("datetime").date(2025, 3, 31))
    assert sql.count("?") == 4 and "@" not in sql
    assert params == ["O'Brien Ltd", "2024-04-01", "2025-03-31", "2024-04-01"]


def test_ten_batches_in_parallel_with_varying_file_counts(tmp_path):
    # 25 batches with 0..7 files each, uploaded and Daisy mixed.
    batches = {}
    fid = 1000
    for b in range(1, 26):
        rows = []
        for i in range(b % 8):
            fid += 1
            daisy = i % 3 == 2
            rows.append(_row(str(b), fid, st.DAISY_FILE_NAME if daisy else f"f{fid}.xlsx",
                             ["Portfolio", "Invoices", "Half Hourly Data"][i % 3] if daisy else None))
        batches[str(b)] = rows
    db, processor = _build(tmp_path, batches)

    running, peak, guard = 0, 0, threading.Lock()

    def tracked(batch_id):
        nonlocal running, peak
        with guard:
            running += 1
            peak = max(peak, running)
        try:
            time.sleep(0.05)
            return processor.process(batch_id)
        finally:
            with guard:
                running -= 1

    results = run_batches(list(batches) + ["3", "3"], tracked, max_parallel_batches=10)

    assert [r.batch_id for r in results] == list(batches)        # duplicates removed, order kept
    assert peak == 10                                            # never more than 10 at a time
    for r in results:
        expected = len(batches[r.batch_id])
        assert r.state == ("NO_FILES" if expected == 0 else "SUCCESS"), (r.batch_id, r.state, r.files)
        assert len(r.files) == expected
    all_ids = [row["FileId"] for rows in batches.values() for row in rows]
    assert all(db.status[str(f)] == st.COMPLETED for f in all_ids)
    upload = next(r for rows in batches.values() for r in rows if r["FileName"] != st.DAISY_FILE_NAME)
    assert db.history[str(upload["FileId"])][:3] == [st.PARSING, st.PARSED, st.NORMALIZING]
    assert exit_code(results) == 0


def test_one_bad_file_does_not_fail_the_batch(tmp_path):
    batches = {"9": [_row("9", 1), _row("9", 2, st.DAISY_FILE_NAME, "Unknown Category")]}
    db, processor = _build(tmp_path, batches)
    result = processor.process("9")
    assert result.state == "COMPLETED_WITH_FILE_ERRORS"
    assert db.status["1"] == st.COMPLETED and db.status["2"] == st.FAILED
    assert db.batch_status["9"] == st.BATCH_COMPLETE


if __name__ == "__main__":
    import tempfile

    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            with tempfile.TemporaryDirectory() as d:
                args = [Path(d)] if fn.__code__.co_argcount else []
                fn(*args)
                print("ok", name)
