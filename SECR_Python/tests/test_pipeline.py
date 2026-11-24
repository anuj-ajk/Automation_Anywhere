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

    def batch_status_exists(self, code):
        return code in self.db.batch_codes

    def set_batch_status(self, batch_id, code, **claim):
        with self.db.lock:
            self.db.batch_status[batch_id] = code
            self.db.batch_history.setdefault(batch_id, []).append(code)
            self.db.claim_calls.append((batch_id, code, claim))

    def heartbeat(self, batch_id, owner):
        with self.db.lock:
            self.db.heartbeats.append((batch_id, owner))
        return 1

    def get_batch_status(self, batch_id):
        return self.db.batch_status.get(batch_id, "PROCESSING")

    def batch_status_codes(self):
        return sorted(self.db.batch_codes)

    def get_sheet_results(self, file_id):
        return self.db.sheet_results.get(str(file_id))

    def set_file_progress(self, file_id, percent):
        with self.db.lock:
            self.db.progress.setdefault(str(file_id), []).append(percent)
        return 1

    def save_sheet_results(self, file_id, results_json):
        with self.db.lock:
            self.db.sheet_results[str(file_id)] = results_json
        return 1

    def add_recognized_category(self, file_id, name):
        with self.db.lock:
            self.db.categories.setdefault(file_id, []).append(name)

    def delete_staging_rows(self, table, file_id):
        with self.db.lock:
            self.db.deleted.append((table, file_id))
        return 0

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
        self.deleted = []
        self.categories = {}
        self.batch_codes = {"COMPLETE", "PROCESSING", "PARTIAL_FAILURE", "IN_PROGRESS"}
        self.batch_history = {}
        self.claim_calls, self.heartbeats = [], []
        self.sheet_results = {}
        self.progress = {}

    @contextlib.contextmanager
    def try_session(self):
        yield FakeSession(self)

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
        from requests_stub import fake_url

        self.csv_url = fake_url(csv_url)  # https://fake.test/<local file>, copied by requests_stub
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


def _row(batch, file_id, name="data.xlsx", category=None, status=None):
    return {"BatchId": batch, "ReportId": 5, "BusinessPartnerName": "ACME", "ReportStart": "2024-04-01",
            "ReportEnd": "2025-03-31", "FileId": file_id, "FileName": name, "StoragePath": f"k/{name}",
            "CategorySelected": category, "StatusCode": status}


def _build(tmp: Path, batches, files_per_batch=4, skip_statuses=("COMPLETED", "DATA_NOT_AVAILABLE")):
    import requests_stub  # noqa: F401  (patches secr.normalize.download_link)

    db = FakeDb(batches)
    log = get_logger()
    loader = StagingLoader("tester", True, 5, 1000, log)
    normalize = NormalizeFlow(db, FakeStorage(_workbook(tmp / "src.xlsx")),
                              FakeAI(str(_metrics_csv(tmp / "metrics.csv"))), loader,
                              poll_interval=0, max_poll_attempts=5)
    daisy = DaisyFlow(db, FakeSources(), loader)
    return db, BatchProcessor(db, normalize, daisy, tmp / "work", files_per_batch, False,
                              skip_statuses=skip_statuses)


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
    body = {"messages": [{"json_response":
                          '{"normalized_csv_file_links": {"invoice": "https://x/u.csv", "metrics": ""}}'}]}
    assert parse_agent_response(body) == {"invoice": "https://x/u.csv"}


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
    assert db.batch_status["9"] == st.BATCH_PARTIAL_FAILURE


if __name__ == "__main__":
    import tempfile

    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            with tempfile.TemporaryDirectory() as d:
                args = [Path(d)] if fn.__code__.co_argcount else []
                fn(*args)
                print("ok", name)


def test_error_inside_procedure_names_the_procedure(tmp_path):
    from secr.loader import DAISY_SPECS, LoadError, StagingLoader
    from secr.logging_setup import get_logger

    class Session:
        def delete_staging_rows(self, table, file_id):
            return 0

        def bulk_insert(self, table, columns, rows, chunk):
            return len(list(rows))

        def call_proc(self, name, args, n_out):
            raise RuntimeError("(1136, \"Column count doesn't match value count at row 1\")")

    spec = DAISY_SPECS["invoices"]
    raw = tmp_path / "inv.csv"
    raw.write_text(",".join(c for c, _ in spec.columns if c != "FileId") + "\n" +
                   ",".join("x" for c, _ in spec.columns if c != "FileId") + "\n", encoding="utf-8")
    loader = StagingLoader("tester", False, 10, 1000, get_logger())
    try:
        loader.load(Session(), spec, raw, tmp_path, "48", "2")
    except LoadError as exc:
        assert "inside stored procedure LoadInvoiceData" in str(exc) and "1136" in str(exc)
    else:
        raise AssertionError("LoadError expected")


def test_rerun_processes_only_files_not_completed(tmp_path):
    rows = [_row("40", 1, status="COMPLETED"), _row("40", 2, status="FAILED"),
            _row("40", 3, name="Source: Daisy", category="Portfolio", status="DATA_NOT_AVAILABLE"),
            _row("40", 4, status="PARTIAL_FAILURE"), _row("40", 5, status=None)]
    db, processor = _build(tmp_path, {"40": rows})
    result = processor.process("40")
    assert sorted(db.history) == ["2", "4", "5"]                  # only these were worked on
    by_id = {f.file_id: f for f in result.files}
    assert by_id["1"].detail == "skipped: already COMPLETED" and by_id["3"].status == "DATA_NOT_AVAILABLE"
    assert len(result.files) == 5 and result.state == "SUCCESS"
    assert db.batch_status["40"] == "COMPLETE"


def test_rerun_of_completed_batch_does_nothing(tmp_path):
    db, processor = _build(tmp_path, {"40": [_row("40", 1, status="COMPLETED")]})
    result = processor.process("40")
    assert result.state == "ALREADY_COMPLETED" and db.history == {} and db.inserted == {}
    assert db.batch_status["40"] == "COMPLETE"


def test_force_processes_completed_files_again(tmp_path):
    db, processor = _build(tmp_path, {"40": [_row("40", 1, status="COMPLETED")]}, skip_statuses=())
    assert processor.process("40").state == "SUCCESS" and "1" in db.history


def test_staging_rows_cleared_once_per_file_and_table(tmp_path):
    db, processor = _build(tmp_path, {"40": [_row("40", 1), _row("40", 2)]})
    processor.process("40")
    # the workbook has 2 sheets, both loading Staging_Metrics: each file clears that table once
    assert sorted(db.deleted) == [("Staging_Metrics", "1"), ("Staging_Metrics", "2")]


def test_batch_status_partial_failure_with_fallback(tmp_path):
    batches = {"40": [_row("40", 1), _row("40", 2, name="Source: Daisy", category="Unknown")],
               "41": [_row("41", 3)]}
    db, processor = _build(tmp_path, batches)
    assert processor.process("40").state == "COMPLETED_WITH_FILE_ERRORS"
    assert processor.process("41").state == "SUCCESS"
    assert db.batch_status == {"40": "PARTIAL_FAILURE", "41": "COMPLETE"}

    db.batch_codes.discard("PARTIAL_FAILURE")          # code not in RefBatchStatus -> fall back
    db.batch_status.clear()
    processor.process("40")
    assert db.batch_status["40"] == "COMPLETE"


def test_local_paths_stay_under_windows_limit(monkeypatch):
    import pytest
    from pathlib import PureWindowsPath
    from secr import paths

    name = "Inspired ESG_SECR_DSF_Acorn Mobility_FY25_V1_20261103_161448552.xlsx"
    short = paths.short_name(name)
    assert short == "Inspired ESG_SECR_DSF_Acorn Mobility_FY2.xlsx" and len(short) <= 45
    assert paths.short_name('a<b|c?.CSV') == "a_b_c_.csv"

    # The share from the failing run: old layout was 261 characters with boto3's temp suffix.
    share = (r"\man-fs-v02\Public\!CC\Automation Anywhere\Automation Processes"
             r"\Environmental Social and Governance\Automation_Anywhere\SECR_Python\work")
    old = PureWindowsPath(share, "batch_52_20261123_160815_690616", "file_77", "source", name)
    new = PureWindowsPath(share, "b52_160815690616", "f77", "src", short)
    assert len(str(old)) + 9 > paths.WINDOWS_MAX_PATH >= len(str(new)) + 9

    monkeypatch.setattr(paths.os, "name", "nt")
    with pytest.raises(OSError, match="over the Windows limit"):
        paths.check_path_length(Path("C:/" + "x" * 300))
    paths.check_path_length(Path("C:/short.xlsx"), extra=9)


class CountingAI(FakeAI):
    """Thread-safe fake that records how many Odin calls run at the same time."""

    def __init__(self, csv_url, answer_after=2, delay=0.02):
        super().__init__(csv_url)
        self.lock = threading.Lock()
        self.active = self.peak = 0
        self.answer_after, self.delay = answer_after, delay
        self.created = 0

    def _call(self):
        with self.lock:
            self.active += 1
            self.peak = max(self.peak, self.active)
        time.sleep(self.delay)
        with self.lock:
            self.active -= 1

    def create_chat(self, name):
        self._call()
        with self.lock:
            self.created += 1
            return f"chat-{self.created}"

    def send_file(self, chat_id, path):
        self._call()
        return True

    def get_csv_links(self, chat_id):
        self._call()
        with self.lock:
            self.polls[chat_id] = self.polls.get(chat_id, 0) + 1
            n = self.polls[chat_id]
        return None if n < self.answer_after else {"metrics": self.csv_url}


def _many_sheets(path: Path, n: int) -> Path:
    from openpyxl import Workbook

    wb = Workbook()
    wb.active.title = "S1"
    wb.active.append(["a"])
    for i in range(2, n + 1):
        wb.create_sheet(f"S{i}").append(["a"])
    wb.save(path)
    return path


def _flow(tmp, ai, sheets=6, per_file=4, in_flight=20):
    import requests_stub  # noqa: F401

    db = FakeDb({})
    sleeps = []
    flow = NormalizeFlow(db, FakeStorage(_many_sheets(tmp / "many.xlsx", sheets)), ai,
                         StagingLoader("tester", True, 5, 1000, get_logger()), poll_interval=40,
                         max_poll_attempts=5, max_parallel_sheets=per_file, max_parallel_requests=in_flight,
                         sleep=sleeps.append)
    return db, flow, sleeps


def test_sheets_of_one_file_run_in_parallel(tmp_path):
    from secr.models import BatchContext

    ai = CountingAI(str(_metrics_csv(tmp_path / "m.csv")), answer_after=3)
    db, flow, sleeps = _flow(tmp_path, ai, sheets=6, per_file=4)
    ctx = BatchContext("40", "5", "ACME", "2024-04-01", "2025-03-31", tmp_path / "w")
    out = flow.run_file(ctx, _row("40", 77), get_logger())
    assert out.status == st.COMPLETED, out
    assert ai.peak == 4                         # 6 sheets, at most 4 at a time
    assert sleeps == [40, 40]                   # all 6 chats answered on poll 3: 2 waits, not 2 per sheet
    assert db.inserted["Staging_Metrics"] == 6
    assert db.history["77"].count(st.NORMALIZING) == 1


def test_odin_calls_are_capped_across_files(tmp_path):
    from secr.models import BatchContext

    ai = CountingAI(str(_metrics_csv(tmp_path / "m.csv")), answer_after=1)
    db, flow, _ = _flow(tmp_path, ai, sheets=5, per_file=5, in_flight=3)
    ctx = BatchContext("40", "5", "ACME", "2024-04-01", "2025-03-31", tmp_path / "w")
    with __import__("concurrent.futures").futures.ThreadPoolExecutor(4) as pool:
        outs = list(pool.map(lambda i: flow.run_file(ctx, _row("40", i), get_logger()), range(1, 5)))
    assert all(o.status == st.COMPLETED for o in outs)
    assert ai.peak == 3                         # 4 files x 5 sheets, never more than 3 calls at once


class RecSession:
    """Records inserts, procedure calls and named locks in one shared event list."""

    def __init__(self, events, name="main"):
        self.events, self.name = events, name

    def delete_staging_rows(self, table, file_id):
        return 0

    def bulk_insert(self, table, columns, rows, chunk):
        rows = list(rows)
        time.sleep(0.02)
        self.events.append(("insert", self.name, len(rows)))
        return len(rows)

    def call_proc(self, name, args, n_out):
        self.events.append(("proc", name))
        return ["completed successfully", "OK"][:n_out]

    @contextlib.contextmanager
    def named_lock(self, name, timeout):
        self.events.append(("lock", name))
        try:
            yield
        finally:
            self.events.append(("unlock", name))


class RecDb:
    def __init__(self, events, free):
        self.events, self.free = events, free

    @contextlib.contextmanager
    def try_session(self):
        if self.free <= 0:
            yield None
            return
        self.free -= 1
        yield RecSession(self.events, f"extra{self.free}")


def _invoice_csv(path, n):
    from secr.loader import DAISY_SPECS

    cols = [c for c, _ in DAISY_SPECS["invoices"].columns if c != "FileId"]
    path.write_text(",".join(cols) + "\n" + "".join(",".join("x" for _ in cols) + "\n" for _ in range(n)),
                    encoding="utf-8")
    return path


def test_big_file_is_inserted_over_free_connections(tmp_path):
    from secr.loader import DAISY_SPECS

    events = []
    loader = StagingLoader("tester", True, 5, 1000, get_logger(), db=RecDb(events, free=10),
                           insert_connections=4, parallel_insert_min_rows=100)
    res = loader.load(RecSession(events), DAISY_SPECS["invoices"], _invoice_csv(tmp_path / "i.csv", 1000),
                      tmp_path, "48", "2")
    inserts = [e for e in events if e[0] == "insert"]
    assert res.rows_inserted == 1000 and len(inserts) == 4 and sum(e[2] for e in inserts) == 1000

    events.clear()  # no free connection: falls back to one, never waits
    loader.db = RecDb(events, free=0)
    loader._cleared.clear()
    res = loader.load(RecSession(events), DAISY_SPECS["invoices"], _invoice_csv(tmp_path / "j.csv", 1000),
                      tmp_path, "49", "2")
    assert [e for e in events if e[0] == "insert"] == [("insert", "main", 1000)]


def test_report_level_procs_run_after_staging_lock(tmp_path):
    from secr.loader import DAISY_SPECS

    events = []
    loader = StagingLoader("tester", True, 5, 1000, get_logger())
    loader.load(RecSession(events), DAISY_SPECS["invoices"], _invoice_csv(tmp_path / "i.csv", 3),
                tmp_path, "48", "2")
    assert [e for e in events if e[0] != "insert"] == [
        ("lock", "secr_load_Staging_Invoices"), ("proc", "LoadInvoiceData"),
        ("unlock", "secr_load_Staging_Invoices"),
        ("lock", "secr_report_2_CalculateInvoiceData"), ("proc", "CalculateInvoiceData"),
        ("unlock", "secr_report_2_CalculateInvoiceData"),
    ]


def test_na_links_are_not_downloaded_or_counted(tmp_path):
    from secr.ai_client import is_real_link, parse_agent_response
    from secr.models import BatchContext

    keys = ["portfolio", "landlord", "transport", "other_fuels", "metrics", "energy_efficiency_narrative",
            "invoice", "half_hourly_data"]
    body = {"messages": [{"json_response": {"normalized_csv_file_links": {
        k: ("https://x/p.csv" if k == "portfolio" else "NA") for k in keys}}}]}
    assert parse_agent_response(body) == {"portfolio": "https://x/p.csv"}
    for bad in ("NA", " n/a ", "", None, "null", "Not available", "ftp://x/y.csv", "x.csv"):
        assert not is_real_link(bad), bad
    assert is_real_link("https://bucket.s3.amazonaws.com/a.csv?sig=1")

    class NAAI(CountingAI):  # the shape seen in batch 52: one real link per sheet, NA for the rest
        def get_csv_links(self, chat_id):
            return {k: (self.csv_url if k == "metrics" else "NA") for k in keys}

    ai = NAAI(str(_metrics_csv(tmp_path / "m.csv")))
    db, flow, _ = _flow(tmp_path, ai, sheets=3, per_file=3)
    ctx = BatchContext("52", "5", "ACME", "2024-04-01", "2025-03-31", tmp_path / "w")
    out = flow.run_file(ctx, _row("52", 77), get_logger())
    assert out.status == st.COMPLETED and "NA" not in out.detail, out
    assert db.categories["77"] == ["Metrics"]
    assert db.inserted["Staging_Metrics"] == 3


def test_batch_marked_in_progress_when_picked_up(tmp_path):
    db, processor = _build(tmp_path, {"40": [_row("40", 1)], "41": []})
    processor.process("40")
    assert db.batch_history["40"] == ["IN_PROGRESS", "COMPLETE"]   # in progress first, final status last
    processor.process("41")                                         # no files: put back to PROCESSING
    assert db.batch_history["41"] == ["IN_PROGRESS", "PROCESSING"]


def test_failed_batch_gets_previous_status_back(tmp_path):
    db, processor = _build(tmp_path, {"40": [_row("40", 1)]})
    processor.normalize = None                                      # forces a batch-level failure
    assert processor.process("40").state == "FAILED"
    assert db.batch_history["40"] == ["IN_PROGRESS", "PROCESSING"]  # picked up again by --pending


def test_missing_in_progress_code_leaves_status_alone(tmp_path):
    db, processor = _build(tmp_path, {"40": [_row("40", 1)]})
    db.batch_codes.discard("IN_PROGRESS")
    processor.process("40")
    assert db.batch_history["40"] == ["COMPLETE"]


def test_claimed_batch_restores_its_previous_status_on_failure(tmp_path):
    db, processor = _build(tmp_path, {"40": [_row("40", 1)]})
    processor.note_claimed("40", "PARTIAL_FAILURE")        # the pending loop already set it IN_PROGRESS
    processor.normalize = None                              # forces a batch-level failure
    assert processor.process("40").state == "FAILED"
    assert db.batch_history["40"] == ["PARTIAL_FAILURE"]    # not set IN_PROGRESS again; old status back



# ---------------------------------------------------------------- claim tracking (ClaimedAt / ClaimedBy)
def test_claim_is_recorded_refreshed_and_cleared(tmp_path):
    class SlowAI(FakeAI):
        def get_csv_links(self, chat_id):
            time.sleep(0.3)                                  # keep the batch busy for a few heartbeats
            return {"metrics": self.csv_url}

    db, processor = _build(tmp_path, {"40": [_row("40", 1)]})
    processor.normalize.ai = SlowAI(str(_metrics_csv(tmp_path / "m.csv")))
    processor.track_claims, processor.claim_owner, processor.heartbeat_seconds = True, "host:1:abc", 0.05
    assert processor.process("40").state == "SUCCESS"
    assert db.claim_calls[0] == ("40", "IN_PROGRESS", {"claim_owner": "host:1:abc"})   # claimed with owner
    assert db.claim_calls[-1] == ("40", "COMPLETE", {"clear_claim": True})             # claim emptied
    assert len(db.heartbeats) >= 2 and set(db.heartbeats) == {("40", "host:1:abc")}
    n = len(db.heartbeats)
    time.sleep(0.2)
    assert len(db.heartbeats) == n                           # heartbeat stopped with the batch


def test_failed_batch_clears_claim_when_put_back(tmp_path):
    db, processor = _build(tmp_path, {"40": [_row("40", 1)]})
    processor.track_claims, processor.claim_owner = True, "host:1:abc"
    processor.normalize = None
    processor.process("40")
    assert db.claim_calls[-1] == ("40", "PROCESSING", {"clear_claim": True})


class _SqlRecorder:
    """Fake EsgSession base: answers RefBatchStatus lookups and records the SQL that was run."""

    def __init__(self, stale=(), changed=(1,)):
        self.sql, self.stale, self.changed = [], list(stale), list(changed)

    def query(self, sql, params=None):
        if "FROM RefBatchStatus WHERE Code IN" in sql:
            return [{"Code": "IN_PROGRESS", "BatchStatusId": 4}, {"Code": "PROCESSING", "BatchStatusId": 3}]
        if "FROM RefBatchStatus WHERE Code" in sql:
            return [{"BatchStatusId": 4}]
        if "ClaimedAt < NOW()" in sql:
            return self.stale
        return [{"StatusId": 3, "Code": "PROCESSING"}]

    def execute(self, sql, params=None):
        self.sql.append((" ".join(sql.split()), params))
        return self.changed.pop(0) if self.changed else 1


def test_claim_sql_records_owner_and_previous_status():
    from secr.db import EsgSession

    s = _SqlRecorder()
    assert EsgSession.claim_batch(s, "52", ("PROCESSING",), "IN_PROGRESS", "host:1:abc") == "PROCESSING"
    sql, params = s.sql[0]
    assert "ClaimPreviousStatusId = BatchStatusId, BatchStatusId = %s" in sql and "ClaimedAt = NOW()" in sql
    assert sql.endswith("WHERE BatchId = %s AND BatchStatusId = %s") and params == (4, "host:1:abc", "52", 3)

    s = _SqlRecorder()
    EsgSession.set_batch_status(s, "52", "COMPLETE", clear_claim=True)
    assert "ClaimedAt = NULL, ClaimedBy = NULL, ClaimPreviousStatusId = NULL" in s.sql[0][0]


def test_stale_claims_are_released_unless_a_heartbeat_arrived():
    from secr.db import EsgSession

    stale = [{"BatchId": 52, "ClaimedBy": "deadhost:9:x", "ClaimedAt": "2026-11-24 09:00"},
             {"BatchId": 53, "ClaimedBy": "livehost:7:y", "ClaimedAt": "2026-11-24 09:00"}]
    s = _SqlRecorder(stale=stale, changed=[1, 0])            # 53 got a heartbeat between SELECT and UPDATE
    released = EsgSession.release_stale_claims(s, "IN_PROGRESS", 30, "PROCESSING")
    assert [r["BatchId"] for r in released] == [52]
    sql, params = s.sql[0]
    assert "COALESCE(ClaimPreviousStatusId, %s)" in sql and "ClaimedAt < NOW() - INTERVAL %s MINUTE" in sql
    assert params == (3, 52, 4, 30)                          # fallback PROCESSING, only while still IN_PROGRESS


def test_claimer_releases_stale_claims_first(tmp_path):
    import contextlib
    from secr import runner

    _db, processor = _build(tmp_path, {})
    processor.track_claims, processor.claim_owner = True, "host:1:abc"
    order = []

    class S:
        def release_stale_claims(self, code, minutes, fallback):
            order.append(("release", code, minutes, fallback))
            return [{"BatchId": 52}]

        def batch_status_exists(self, code):
            return True

        def claim_batch(self, b, codes, code, owner=None, procedure=""):
            order.append(("claim", b, owner, procedure))
            return "PROCESSING"

    class Db:
        @contextlib.contextmanager
        def session(self):
            yield S()

    class Cfg:
        class run:
            pending_batches_query, claim_procedure, claim_batch_procedure = "SELECT 1", "", "ClaimBatch"
            batch_status_in_progress, claimable_statuses = "IN_PROGRESS", ("PROCESSING", "PARTIAL_FAILURE")
            stale_claim_minutes, release_stale_procedure = 30, ""

    original = runner.pending_batch_ids
    runner.pending_batch_ids = lambda cfg, db, limit=None: ["52"]
    try:
        assert runner.make_claimer(Cfg, Db(), processor)(5, set()) == ["52"]
    finally:
        runner.pending_batch_ids = original
    assert order == [("release", "IN_PROGRESS", 30, "PROCESSING"), ("claim", "52", "host:1:abc", "ClaimBatch")]


# ---------------------------------------------------------------- ClaimBatch stored procedure
class _ProcCursor:
    """Fake PyMySQL cursor: CALL ClaimBatch sets the OUT values, SELECT @_ClaimBatch_n reads them back."""

    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def callproc(self, name, args):
        self.conn.calls.append((name, args))

    def nextset(self):
        return None

    def execute(self, sql, params=None):
        self.conn.calls.append(sql)

    def fetchone(self):
        return {"PreviousCode": self.conn.previous, "Claimed": self.conn.claimed}


class _ProcConn:
    def __init__(self, previous, claimed):
        self.previous, self.claimed, self.calls = previous, claimed, []

    def cursor(self):
        return _ProcCursor(self)


def test_claim_batch_calls_the_procedure():
    from secr.db import EsgSession

    class S:
        conn = _ProcConn(b"PARTIAL_FAILURE", 1)
        claim_batch_via_proc = EsgSession.claim_batch_via_proc
        _cursor = EsgSession._cursor

    s = S()
    previous = EsgSession.claim_batch(s, "52", ("PROCESSING", "PARTIAL_FAILURE"), "IN_PROGRESS", "host:1:abc",
                                      procedure="ClaimBatch")
    assert previous == "PARTIAL_FAILURE"
    assert s.conn.calls[0] == ("ClaimBatch", (52, "PROCESSING,PARTIAL_FAILURE", "IN_PROGRESS", "host:1:abc",
                                              None, None))
    assert s.conn.calls[1] == "SELECT @_ClaimBatch_4 AS PreviousCode, @_ClaimBatch_5 AS Claimed"

    s.conn = _ProcConn("IN_PROGRESS", 0)                     # another system owns it
    assert EsgSession.claim_batch(s, "52", ("PROCESSING",), "IN_PROGRESS", procedure="ClaimBatch") is None


def test_claim_batch_procedure_name_is_checked():
    import pytest
    from secr.db import EsgSession

    class S:
        conn = _ProcConn("PROCESSING", 1)

    with pytest.raises(ValueError):
        EsgSession.claim_batch_via_proc(S(), "ClaimBatch; DROP TABLE x", "52", ("PROCESSING",), "IN_PROGRESS")
    assert S.conn.calls == []


# ---------------------------------------------------------------- explicit batch ids claimed through ClaimBatch
def _claiming_processor(tmp_path, status, take_in_progress=False):
    db, processor = _build(tmp_path, {"40": [_row("40", 1)]})
    db.batch_status["40"] = status
    db.proc_calls = []

    def claim_batch(self, batch_id, codes, in_progress, owner=None, procedure=""):
        db.proc_calls.append((batch_id, tuple(codes), in_progress, owner, procedure))
        current = db.batch_status.get(batch_id, "PROCESSING")
        if current not in codes:
            return None
        db.batch_status[batch_id] = in_progress
        db.batch_history.setdefault(batch_id, []).append(in_progress)
        return current

    FakeSession.claim_batch = claim_batch
    processor.claim_batch_procedure = "ClaimBatch"
    processor.claimable_statuses = ("PROCESSING", "PARTIAL_FAILURE")
    processor.take_in_progress = take_in_progress
    return db, processor


def test_explicit_batch_is_claimed_through_claim_batch(tmp_path):
    db, processor = _claiming_processor(tmp_path, "PARTIAL_FAILURE")
    try:
        assert processor.process("40").state == "SUCCESS"
    finally:
        del FakeSession.claim_batch
    assert db.proc_calls == [("40", ("PROCESSING", "PARTIAL_FAILURE"), "IN_PROGRESS", None, "ClaimBatch")]
    assert db.batch_history["40"] == ["IN_PROGRESS", "COMPLETE"]   # IN_PROGRESS came from the procedure only


def test_explicit_batch_in_progress_elsewhere_is_skipped(tmp_path):
    db, processor = _claiming_processor(tmp_path, "IN_PROGRESS")
    try:
        result = processor.process("40")
    finally:
        del FakeSession.claim_batch
    assert result.state == "SKIPPED" and "IN_PROGRESS" in result.error
    assert db.batch_status["40"] == "IN_PROGRESS" and "40" not in db.batch_history   # left alone


def test_force_takes_a_batch_in_progress(tmp_path):
    db, processor = _claiming_processor(tmp_path, "IN_PROGRESS", take_in_progress=True)
    processor.normalize = None                                  # make it fail: the status is put back
    try:
        processor.process("40")
    finally:
        del FakeSession.claim_batch
    assert db.batch_history["40"] == ["IN_PROGRESS", "PROCESSING"]   # back to a claimable status, not IN_PROGRESS


def test_explicit_rerun_of_completed_batch_sets_status_directly(tmp_path):
    db, processor = _claiming_processor(tmp_path, "COMPLETE")
    try:
        assert processor.process("40").state == "SUCCESS"
    finally:
        del FakeSession.claim_batch
    assert len(db.proc_calls) == 1                               # tried the procedure first
    assert db.batch_history["40"] == ["IN_PROGRESS", "COMPLETE"]


# ---------------------------------------------------------------- ReleaseStaleClaims stored procedure
def test_release_stale_calls_the_procedure():
    from secr.db import EsgSession

    class Cursor:
        def __init__(self, calls):
            self.calls = calls

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def callproc(self, name, args):
            self.calls.append((name, args))

        def fetchall(self):
            return [{"BatchId": 52, "ClaimedBy": "deadhost:9:x", "ClaimedAt": "2026-11-24 09:00"}]

        def nextset(self):
            return None

    class S:
        calls = []
        conn = type("Conn", (), {"cursor": lambda self: Cursor(S.calls)})()
        _cursor = EsgSession._cursor

    rows = EsgSession.release_stale_via_proc(S(), "ReleaseStaleClaims", "IN_PROGRESS", 30, "PROCESSING")
    assert [r["BatchId"] for r in rows] == [52]
    assert S.calls == [("ReleaseStaleClaims", ("IN_PROGRESS", 30, "PROCESSING"))]
    import pytest
    with pytest.raises(ValueError):
        EsgSession.release_stale_via_proc(S(), "x; DROP TABLE y", "IN_PROGRESS", 30, "PROCESSING")


def test_release_stale_uses_the_procedure_when_configured(tmp_path):
    import contextlib
    from secr import runner

    _db, processor = _build(tmp_path, {})
    processor.track_claims = True
    calls = []

    class S:
        def release_stale_via_proc(self, proc, code, minutes, fallback):
            calls.append(("proc", proc, code, minutes, fallback))
            return []

        def release_stale_claims(self, code, minutes, fallback):
            calls.append(("python", code, minutes, fallback))
            return []

    class Db:
        @contextlib.contextmanager
        def session(self):
            yield S()

    class Cfg:
        class run:
            batch_status_in_progress, claimable_statuses = "IN_PROGRESS", ("PROCESSING", "PARTIAL_FAILURE")
            stale_claim_minutes, release_stale_procedure = 30, "ReleaseStaleClaims"

    runner.release_stale(processor, Cfg, Db())
    Cfg.run.release_stale_procedure = ""
    runner.release_stale(processor, Cfg, Db())
    assert calls == [("proc", "ReleaseStaleClaims", "IN_PROGRESS", 30, "PROCESSING"),
                     ("python", "IN_PROGRESS", 30, "PROCESSING")]


# ---------------------------------------------------------------- retries (S3 download, Odin submit)
def _http_error(status):
    import requests

    resp = requests.Response()
    resp.status_code = status
    return requests.HTTPError(f"HTTP {status}", response=resp)


class FlakyAI(CountingAI):
    """create_chat / send_file fail with the given errors first, then work."""

    def __init__(self, csv_url, chat_errors=(), send_errors=()):
        super().__init__(csv_url, answer_after=1, delay=0)
        self.chat_errors, self.send_errors = list(chat_errors), list(send_errors)
        self.sent_to = []

    def create_chat(self, name):
        with self.lock:
            if self.chat_errors:
                raise self.chat_errors.pop(0)
        return super().create_chat(name)

    def send_file(self, chat_id, path):
        with self.lock:
            self.sent_to.append(chat_id)
            if self.send_errors:
                raise self.send_errors.pop(0)
        return True


def test_odin_submit_retries_transient_errors_in_a_new_chat(tmp_path):
    import requests
    from secr.models import BatchContext

    ai = FlakyAI(str(_metrics_csv(tmp_path / "m.csv")), chat_errors=[requests.ConnectionError("reset")],
                 send_errors=[_http_error(503)])
    db, flow, sleeps = _flow(tmp_path, ai, sheets=1, per_file=1)
    flow.submit_retry_backoff = 10
    out = flow.run_file(BatchContext("40", "5", "ACME", "2024-04-01", "2025-03-31", tmp_path / "w"),
                        _row("40", 1), get_logger())
    assert out.status == st.COMPLETED, out
    assert sleeps[:2] == [10, 20]               # backoff doubles
    assert ai.sent_to == ["chat-1", "chat-2"]   # upload retried in a fresh chat
    assert list(ai.polls) == ["chat-2"]         # only the chat that got the file is polled


def test_odin_submit_does_not_retry_client_errors(tmp_path):
    from secr.models import BatchContext

    ai = FlakyAI(str(_metrics_csv(tmp_path / "m.csv")), chat_errors=[_http_error(401)])
    db, flow, sleeps = _flow(tmp_path, ai, sheets=1, per_file=1)
    out = flow.run_file(BatchContext("40", "5", "ACME", "2024-04-01", "2025-03-31", tmp_path / "w"),
                        _row("40", 1), get_logger())
    assert out.status == st.FAILED_WHILE_NORMALIZATION, out
    assert ai.created == 0 and sleeps == []


def test_odin_submit_gives_up_after_the_configured_attempts(tmp_path):
    import requests
    from secr.models import BatchContext

    ai = FlakyAI(str(_metrics_csv(tmp_path / "m.csv")), chat_errors=[requests.Timeout("t")] * 5)
    db, flow, sleeps = _flow(tmp_path, ai, sheets=1, per_file=1)
    flow.submit_attempts = 3
    out = flow.run_file(BatchContext("40", "5", "ACME", "2024-04-01", "2025-03-31", tmp_path / "w"),
                        _row("40", 1), get_logger())
    assert out.status == st.FAILED_WHILE_NORMALIZATION, out
    assert len(ai.chat_errors) == 2 and len(sleeps) == 2


def _s3_storage(client, attempts=3):
    from secr.storage import S3Storage

    storage = S3Storage.__new__(S3Storage)  # skip boto3 client creation
    storage.bucket, storage._client = "bucket", client
    storage.attempts, storage.backoff, storage.sleeps = attempts, 5, []
    storage._sleep = storage.sleeps.append
    return storage


class _FlakyS3:
    def __init__(self, errors):
        self.errors, self.calls = list(errors), 0

    def download_file(self, bucket, key, dest):
        self.calls += 1
        if self.errors:
            err = self.errors.pop(0)
            if err == "empty":
                Path(dest).write_bytes(b"")
                return
            raise err
        Path(dest).write_bytes(b"data")


def test_s3_download_retries_transient_errors(tmp_path):
    from botocore.exceptions import ClientError, EndpointConnectionError

    client = _FlakyS3([EndpointConnectionError(endpoint_url="https://s3"),
                       ClientError({"Error": {"Code": "SlowDown"}, "ResponseMetadata": {"HTTPStatusCode": 503}},
                                   "GetObject"),
                       "empty"])
    storage = _s3_storage(client, attempts=4)
    out = storage.download("k/a.xlsx", tmp_path / "a.xlsx")
    assert out.read_bytes() == b"data" and client.calls == 4
    assert storage.sleeps == [5, 10, 20]


def test_s3_download_does_not_retry_missing_key(tmp_path):
    import pytest
    from botocore.exceptions import ClientError

    client = _FlakyS3([ClientError({"Error": {"Code": "404"}, "ResponseMetadata": {"HTTPStatusCode": 404}},
                                   "HeadObject")])
    storage = _s3_storage(client)
    with pytest.raises(ClientError):
        storage.download("k/a.xlsx", tmp_path / "a.xlsx")
    assert client.calls == 1 and storage.sleeps == []


# ---------------------------------------------------------------- per-sheet results (UploadedFile.SheetResults)
class SheetAI(FakeAI):
    """Chat ids carry the sheet file name; sheets listed in ``broken`` get a link that cannot be downloaded."""

    def __init__(self, csv_url, broken=()):
        super().__init__(csv_url)
        self.broken = set(broken)
        self.created = []
        self.lock = threading.Lock()

    def create_chat(self, name):
        with self.lock:
            self.created.append(name)
            return f"chat-{name}-{len(self.created)}"

    def get_csv_links(self, chat_id):
        links = super().get_csv_links(chat_id)
        if links and any(f"Sheet_{b}.xlsx" in chat_id for b in self.broken):
            return {"metrics": "https://fake.test/no/such/file.csv?token=SECRET-SIGNED-TOKEN"}
        return links


def _sheet_results(db, file_id):
    import json

    data = json.loads(db.sheet_results[str(file_id)])
    return data, {s["sheet"]: s for s in data["sheets"]}


def _ctx(tmp_path):
    from secr.models import BatchContext

    return BatchContext("52", "5", "ACME", "2024-04-01", "2025-03-31", tmp_path / "w")


def test_failed_sheet_is_recorded_with_stage_and_reason(tmp_path):
    ai = SheetAI(str(_metrics_csv(tmp_path / "m.csv")), broken={"S2"})
    db, flow, _ = _flow(tmp_path, ai, sheets=3)
    out = flow.run_file(_ctx(tmp_path), dict(_row("52", 77), FileVersion=1), get_logger())
    assert out.status == st.PARTIAL_FAILURE
    data, sheets = _sheet_results(db, 77)
    assert data["file_version"] == "1"
    assert [s["sheet"] for s in data["sheets"]] == ["S1", "S2", "S3"]
    assert sheets["S1"]["status"] == "COMPLETED" and sheets["S1"]["file"] == "Sheet_S1.xlsx"
    assert sheets["S1"]["categories"] == [{"category": "metrics", "status": "COMPLETED", "rows": 1}]
    bad = sheets["S2"]
    assert bad["status"] == "FAILED" and bad["stage"] == "download" and bad["attempts"] == 1
    assert bad["reason"].startswith("metrics:") and "SECRET" not in db.sheet_results["77"]
    assert "SECRET" not in out.detail                       # signed token never stored or returned


def test_rerun_retries_only_the_failed_sheets(tmp_path):
    ai = SheetAI(str(_metrics_csv(tmp_path / "m.csv")), broken={"S2"})
    db, flow, _ = _flow(tmp_path, ai, sheets=3)
    row = dict(_row("52", 77), FileVersion=1)
    flow.run_file(_ctx(tmp_path), row, get_logger())
    assert sorted(ai.created) == ["Sheet_S1.xlsx", "Sheet_S2.xlsx", "Sheet_S3.xlsx"]
    assert db.deleted == [("Staging_Metrics", "77")]

    ai.broken.clear()
    ai.created.clear()
    db.deleted.clear()
    flow.loader._cleared.clear()  # a new run starts with a new loader
    out = flow.run_file(_ctx(tmp_path), dict(row, StatusCode=st.PARTIAL_FAILURE), get_logger())

    assert out.status == st.COMPLETED, out
    assert ai.created == ["Sheet_S2.xlsx"]                  # only the failed sheet went to the AI again
    assert db.deleted == []                                 # S1 / S3 rows in Staging_Metrics are kept
    assert db.inserted["Staging_Metrics"] == 3              # 2 on the first run + 1 on the retry
    _, sheets = _sheet_results(db, 77)
    assert all(s["status"] == "COMPLETED" for s in sheets.values())
    assert sheets["S2"]["attempts"] == 2 and sheets["S1"]["attempts"] == 1


def test_all_sheets_completed_earlier_sets_completed_without_ai(tmp_path):
    ai = SheetAI(str(_metrics_csv(tmp_path / "m.csv")))
    db, flow, _ = _flow(tmp_path, ai, sheets=2)
    row = dict(_row("52", 77), FileVersion=1)
    flow.run_file(_ctx(tmp_path), row, get_logger())
    ai.created.clear()
    out = flow.run_file(_ctx(tmp_path), row, get_logger())
    assert out.status == st.COMPLETED and "already completed" in out.detail
    assert ai.created == []


def test_force_or_new_file_version_processes_every_sheet(tmp_path):
    ai = SheetAI(str(_metrics_csv(tmp_path / "m.csv")), broken={"S2"})
    db, flow, _ = _flow(tmp_path, ai, sheets=2)
    row = dict(_row("52", 77), FileVersion=1)
    flow.run_file(_ctx(tmp_path), row, get_logger())

    ai.broken.clear()
    ai.created.clear()
    flow.run_file(_ctx(tmp_path), dict(row, FileVersion=2), get_logger())    # a new upload of the file
    assert sorted(ai.created) == ["Sheet_S1.xlsx", "Sheet_S2.xlsx"]
    assert _sheet_results(db, 77)[0]["file_version"] == "2"

    ai.created.clear()
    flow.resume_sheets = False                                                 # force = true
    flow.run_file(_ctx(tmp_path), dict(row, FileVersion=2), get_logger())
    assert sorted(ai.created) == ["Sheet_S1.xlsx", "Sheet_S2.xlsx"]
    assert all(s["attempts"] == 1 for s in _sheet_results(db, 77)[1].values())


def test_without_the_sheet_results_column_every_sheet_is_processed(tmp_path):
    ai = SheetAI(str(_metrics_csv(tmp_path / "m.csv")))
    db, flow, _ = _flow(tmp_path, ai, sheets=2)

    def missing(*_a):
        raise RuntimeError("(1054, \"Unknown column 'SheetResults' in 'field list'\")")

    FakeSession.get_sheet_results = missing
    try:
        out = flow.run_file(_ctx(tmp_path), _row("52", 77), get_logger())
    finally:
        del FakeSession.get_sheet_results
    assert out.status == st.COMPLETED and db.sheet_results == {}
    assert flow._sheet_results_available is False


def test_sheet_results_json_round_trip():
    from secr import sheet_results as sr

    res = sr.FileSheetResults("3")
    rec = res.record("TRANSPORT", "Sheet_TRANSPORT.xlsx")
    rec.category_failed("transport", sr.STAGE_DOWNLOAD,
                        "400 Client Error for url: https://api.getodin.ai/x/T.csv?token=abc.def.ghi")
    rec.finish()
    again = sr.FileSheetResults.parse(res.to_json())
    back = again.sheets["TRANSPORT"]
    assert again.file_version == "3" and back.status == sr.FAILED and back.stage == sr.STAGE_DOWNLOAD
    assert back.reason == "transport: 400 Client Error for url: https://api.getodin.ai/x/T.csv"
    assert again.completed() == {} and sr.FileSheetResults.parse(None).sheets == {}


def test_lock_wait_timeout_names_the_likely_cause():
    import pymysql
    import pytest

    from secr.db import LOCK_WAIT_HINT, EsgSession

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql, params):
            raise pymysql.err.OperationalError(1205, "Lock wait timeout exceeded; try restarting transaction")

    class Conn:
        def cursor(self):
            return Cursor()

    with pytest.raises(pymysql.err.OperationalError) as err:
        EsgSession(Conn()).execute("UPDATE UploadedFile SET ProcessingStatusId = 1 WHERE FileId = %s", (77,))
    assert err.value.args[0] == 1205 and "never committed" in str(err.value) and "innodb_trx" in LOCK_WAIT_HINT

    class OtherError(Cursor):
        def execute(self, sql, params):
            raise pymysql.err.OperationalError(1054, "Unknown column")

    Conn.cursor = lambda self: OtherError()
    with pytest.raises(pymysql.err.OperationalError) as err:
        EsgSession(Conn()).query("SELECT x")
    assert err.value.args == (1054, "Unknown column")              # other errors are untouched


def _locked(*_a, **_k):
    import pymysql

    raise pymysql.err.OperationalError(1205, "Lock wait timeout exceeded; try restarting transaction")


def test_batch_with_locked_row_is_skipped_completely(tmp_path):
    db, processor = _claiming_processor(tmp_path, "PROCESSING")
    FakeSession.claim_batch = _locked                         # ClaimBatch waits on the locked row
    calls = []
    FakeSession.set_batch_status = lambda self, *a, **k: calls.append(a)
    try:
        result = processor.process("40")
    finally:
        del FakeSession.claim_batch
        del FakeSession.set_batch_status
    assert result.state == "LOCKED" and "locked" in result.error
    assert calls == []                                        # no second wait on the same lock
    assert db.history == {} and db.inserted == {}             # no file touched, nothing loaded
    assert exit_code([result]) == 2                           # AA sees PARTIAL, not SUCCESS


def test_batch_skipped_when_setting_in_progress_hits_a_lock(tmp_path):
    db, processor = _build(tmp_path, {"40": [_row("40", 1)]})
    FakeSession.set_batch_status = _locked
    try:
        result = processor.process("40")
    finally:
        del FakeSession.set_batch_status
    assert result.state == "LOCKED" and db.history == {}


# ---------------------------------------------------------------- UploadedFile.ProgressPercent
def test_progress_rises_to_100_for_uploaded_and_daisy_files(tmp_path):
    batches = {"40": [_row("40", 1), _row("40", 2, st.DAISY_FILE_NAME, "Portfolio"),
                      _row("40", 3, st.DAISY_FILE_NAME, "Unknown Category")]}
    db, processor = _build(tmp_path, batches)
    processor.process("40")
    upload = db.progress["1"]
    assert upload[0] == 0 and upload[-1] == 100 and upload == sorted(set(upload))   # only goes up, no repeats
    assert {5, 10, 20, 30, 70, 75}.issubset(upload)
    assert db.progress["2"] == [0, 10, 60, 100]
    assert db.progress["3"] == [0, 100]                    # failed early: still finished at 100


def test_progress_restarts_from_zero_on_a_rerun(tmp_path):
    ai = SheetAI(str(_metrics_csv(tmp_path / "m.csv")), broken={"S2"})
    db, flow, _ = _flow(tmp_path, ai, sheets=2)
    flow.run_file(_ctx(tmp_path), _row("52", 77), get_logger())
    flow.run_file(_ctx(tmp_path), _row("52", 77), get_logger())
    runs = db.progress["77"]
    second = runs[runs.index(0, 1):]
    assert second[0] == 0 and second[-1] == 100


def test_progress_without_the_column_is_switched_off(tmp_path):
    from secr.progress import ProgressReporter

    class NoColumn(FakeSession):
        def set_file_progress(self, file_id, percent):
            raise RuntimeError(1054, "Unknown column 'ProgressPercent' in 'field list'")

    calls = []

    class Db(FakeDb):
        @contextlib.contextmanager
        def session(self):
            calls.append(1)
            yield NoColumn(self)

    reporter = ProgressReporter(Db({}))
    reporter.start("1")
    reporter.set("1", 50)
    reporter.set("2", 10)
    assert calls == [1] and reporter._available is False   # tried once, then never again


def test_progress_between():
    from secr.progress import between

    assert [between(30, 70, d, 4) for d in range(5)] == [30, 40, 50, 60, 70]
    assert between(30, 70, 1, 1000) == 30 and between(75, 99, 0, 0) == 99
