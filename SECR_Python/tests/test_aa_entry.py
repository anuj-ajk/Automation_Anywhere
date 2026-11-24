"""Tests for the Automation Anywhere entry function (aa_entry.run_batches) with fake services."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

import aa_entry  # noqa: E402
import secr.runner as runner  # noqa: E402
from test_pipeline import _build, _row  # noqa: E402


def test_parse_batch_ids():
    assert aa_entry.parse_batch_ids("40, 41;42 43,40") == ["40", "41", "42", "43"]
    assert aa_entry.parse_batch_ids(["40", 41, 42.0, "43.0"]) == ["40", "41", "42", "43"]
    assert aa_entry.parse_batch_ids(None) == []


def test_run_batches_returns_json(tmp_path, monkeypatch):
    monkeypatch.setenv("SECR_DB_USER", "u")
    monkeypatch.setenv("SECR_DB_PASSWORD", "p")
    _db, processor = _build(tmp_path, {"40": [_row("40", 1)], "41": [_row("41", 2), _row("41", 3)], "42": []})
    seen = {}

    def fake_build(cfg, debug=False):
        seen["batches"] = cfg.run.max_parallel_batches
        _, _, log_file = runner.setup_logging(tmp_path / "logs", runner.logging.DEBUG)
        runner._log_settings(cfg, runner.get_logger())
        return processor, None, runner.Notifier(cfg.notify, runner.get_logger()), log_file

    monkeypatch.setattr(runner, "build", fake_build)
    config = tmp_path / "config.yaml"
    config.write_text(f"esg_db:\n  host: localhost\nrun:\n  log_dir: {(tmp_path / 'logs').as_posix()}\n",
                      encoding="utf-8")
    out = json.loads(aa_entry.run_batches({
        "batch_ids": "40,41,42",
        "config_path": str(config),
        "max_batches": "3",
    }))
    assert out["status"] == "SUCCESS" and out["exit_code"] == 0
    assert seen["batches"] == 3
    assert [b["batch_id"] for b in out["batches"]] == ["40", "41", "42"]
    assert [b["files"] for b in out["batches"]] == [1, 2, 0]
    assert out["batches"][2]["state"] == "NO_FILES"

    # Every line of a file's work carries its batch and file, low-level modules included.
    for handler in list(runner.logging.getLogger("secr").handlers):
        handler.flush()
    text = Path(out["log_file"]).read_text(encoding="utf-8")
    assert "[batch 41][file 3]" in text
    assert "Step 1/4 download from S3 - done" in text
    assert "Called from Automation Anywhere" in text
    assert "SECR_DB_PASSWORD=set" in text and "=p\n" not in text  # secret values are never logged


def test_errors_come_back_as_json():
    out = json.loads(aa_entry.run_batches({"batch_ids": ""}))
    assert out["status"] == "FAILED" and "batch_ids" in out["message"]
    out = json.loads(aa_entry.run_batches({"batch_ids": "40", "config_path": "missing.yaml"}))
    assert out["status"] == "FAILED" and "FileNotFoundError" in out["message"]
    out = json.loads(aa_entry.run_batches({"batch_ids": "40", "secrets": {"PATH": "x"}}))
    assert out["status"] == "FAILED" and "not a SECR_" in out["message"]


def test_unset_secrets_are_not_written_as_none(monkeypatch):
    monkeypatch.setenv("SECR_ODIN_API_KEY", "from-machine")
    monkeypatch.delenv("SECR_NAV_CONNECTION_STRING", raising=False)
    aa_entry._apply_secrets({"SECR_ODIN_API_KEY": None, "SECR_NAV_CONNECTION_STRING": ""})
    assert aa_entry.os.environ["SECR_ODIN_API_KEY"] == "from-machine"
    assert "SECR_NAV_CONNECTION_STRING" not in aa_entry.os.environ
    assert aa_entry._secret_keys({"SECR_DB_USER": "u", "SECR_DB_PASSWORD": None}) == "SECR_DB_USER"


def test_pending_batches_from_aa(tmp_path, monkeypatch):
    monkeypatch.setenv("SECR_DB_USER", "u")
    monkeypatch.setenv("SECR_DB_PASSWORD", "p")
    _db, processor = _build(tmp_path, {"40": [_row("40", 1)], "41": [_row("41", 2)], "43": [_row("43", 3)]})
    monkeypatch.setattr(runner, "build",
                        lambda cfg, debug=False: (processor, "db", runner.Notifier(cfg.notify, runner.get_logger()), "x.log"))
    queue: list = []

    def fake_claimer(cfg, db, proc):
        def fetch_new(n, exclude):
            taken = [b for b in queue if b not in exclude][:n]
            for b in taken:
                queue.remove(b)
            return taken
        return fetch_new

    monkeypatch.setattr(runner, "make_claimer", fake_claimer)
    config = tmp_path / "config.yaml"
    config.write_text("esg_db:\n  host: localhost\nrun:\n  pending_batches_query: SELECT 1\n", encoding="utf-8")

    queue[:] = ["41", "40"]
    out = json.loads(aa_entry.run_batches({"pending": "true", "batch_ids": "43", "config_path": str(config)}))
    assert out["status"] == "SUCCESS", out
    assert [b["batch_id"] for b in out["batches"]] == ["43", "41", "40"]

    queue[:] = ["41", "40"]
    out = json.loads(aa_entry.run_batches({"batch_ids": "pending", "pending_limit": "1", "config_path": str(config)}))
    assert [b["batch_id"] for b in out["batches"]] == ["41"]

    queue[:] = []
    out = json.loads(aa_entry.run_batches({"pending": True, "config_path": str(config)}))
    assert out["status"] == "SUCCESS" and out["message"] == "No pending batches" and out["batches"] == []


def test_pending_batch_ids_reads_the_query():
    import contextlib

    class S:
        def query(self, sql):
            assert sql == "SELECT BatchId FROM x"
            return [{"BatchId": 52}, {"BatchId": 53}, {"BatchId": 54}]

    class Db:
        @contextlib.contextmanager
        def session(self):
            yield S()

    class Cfg:
        class run:
            pending_batches_query = "SELECT BatchId FROM x"

    assert runner.pending_batch_ids(Cfg, Db()) == ["52", "53", "54"]
    assert runner.pending_batch_ids(Cfg, Db(), limit=2) == ["52", "53"]


def test_work_and_log_dir_from_aa(tmp_path, monkeypatch):
    monkeypatch.setenv("SECR_DB_USER", "u")
    monkeypatch.setenv("SECR_DB_PASSWORD", "p")
    monkeypatch.setenv("SECR_TEST_ROOT", str(tmp_path / "local"))
    _db, processor = _build(tmp_path, {"40": [_row("40", 1)]})
    seen = {}

    def fake_build(cfg, debug=False):
        seen["work"], seen["log"] = cfg.run.work_dir, cfg.run.log_dir
        return processor, None, runner.Notifier(cfg.notify, runner.get_logger()), "x.log"

    monkeypatch.setattr(runner, "build", fake_build)
    conf_dir = tmp_path / "conf"
    conf_dir.mkdir()
    config = conf_dir / "config.yaml"
    config.write_text("esg_db:\n  host: localhost\nrun:\n  work_dir: ./work\n  log_dir: ./logs\n", encoding="utf-8")

    aa_entry.run_batches({"batch_ids": "40", "config_path": str(config),
                          "work_dir": "%SECR_TEST_ROOT%/SECR_work", "log_dir": "run_logs"})
    assert seen["work"] == (tmp_path / "local" / "SECR_work").resolve()      # %VAR% expanded
    assert seen["log"] == (conf_dir / "run_logs").resolve()                  # relative to the config folder

    aa_entry.run_batches({"batch_ids": "40", "config_path": str(config)})   # not passed: config.yaml values
    assert seen["work"] == (conf_dir / "work").resolve() and seen["log"] == (conf_dir / "logs").resolve()


# ---------------------------------------------------------------- pending loop + atomic claims
def _result_after(delay):
    import time
    from secr.models import BatchResult

    def process(b):
        time.sleep(delay(b))
        return BatchResult(b, "SUCCESS")
    return process


def test_free_slots_are_topped_up_while_batches_run():
    import threading
    import time
    from secr.orchestrator import run_pending

    lock, active, peak = threading.Lock(), [0], [0]
    arrivals = {"start": [str(i) for i in range(1, 3)]}       # 2 pending at the start
    calls = []

    def process(b):
        with lock:
            active[0] += 1
            peak[0] = max(peak[0], active[0])
        if b == "1":
            arrivals["start"] += [str(i) for i in range(3, 13)]  # 10 new batches arrive while 1 and 2 run
        time.sleep(0.15 if b in ("1", "2") else 0.05)
        with lock:
            active[0] -= 1
        from secr.models import BatchResult
        return BatchResult(b, "SUCCESS")

    def fetch_new(n, exclude):
        calls.append(n)
        taken = [b for b in arrivals["start"] if b not in exclude][:n]
        arrivals["start"] = [b for b in arrivals["start"] if b not in taken]
        return taken

    results = run_pending(process, fetch_new, max_parallel_batches=10, poll_seconds=0.02)
    assert sorted(int(r.batch_id) for r in results) == list(range(1, 13))
    assert calls[0] == 10 and 8 in calls        # first call: 10 free; with 2 running: 8 free
    assert peak[0] == 10                        # the cap is never exceeded, and is used fully


def test_pending_stops_taking_new_after_limits():
    from secr.orchestrator import run_pending

    pool = [str(i) for i in range(1, 20)]

    def fetch_new(n, exclude):
        taken = [b for b in pool if b not in exclude][:n]
        return taken

    results = run_pending(_result_after(lambda b: 0.01), fetch_new, max_parallel_batches=3, poll_seconds=0.01,
                          max_batches=5)
    assert [r.batch_id for r in results] == ["1", "2", "3", "4", "5"]
    ticks = iter([0, 0, 100, 100, 100, 100, 100, 100])
    results = run_pending(_result_after(lambda b: 0.01), fetch_new, max_parallel_batches=2, poll_seconds=0.01,
                          max_runtime_seconds=50, clock=lambda: next(ticks, 100))
    assert [r.batch_id for r in results] == ["1", "2"]   # deadline passed: no new batches, running ones finish


class _ClaimSession:
    """Fake DB for claim_batch: one batch row whose status another system may change first."""

    def __init__(self, status, taken_by_other=False):
        self.status, self.taken_by_other = status, taken_by_other
        self.updates = []

    def query(self, sql, params=None):
        if "FROM FileUploadBatch" in sql:
            return [{"StatusId": {"PROCESSING": 3, "IN_PROGRESS": 4, "COMPLETE": 5}[self.status],
                     "Code": self.status}]
        return [{"BatchStatusId": 4}]

    def execute(self, sql, params=None):
        self.updates.append(params)
        return 0 if self.taken_by_other else 1


def test_claim_batch_is_compare_and_set():
    from secr.db import EsgSession

    s = _ClaimSession("PROCESSING")
    assert EsgSession.claim_batch(s, "52", ("PROCESSING", "PARTIAL_FAILURE"), "IN_PROGRESS") == "PROCESSING"
    assert s.updates == [(4, "52", 3)]                     # ... WHERE BatchId = 52 AND BatchStatusId = 3

    s = _ClaimSession("PROCESSING", taken_by_other=True)   # another system changed it first: 0 rows
    assert EsgSession.claim_batch(s, "52", ("PROCESSING",), "IN_PROGRESS") is None

    s = _ClaimSession("IN_PROGRESS")                       # already owned: not claimable, no UPDATE
    assert EsgSession.claim_batch(s, "52", ("PROCESSING",), "IN_PROGRESS") is None and s.updates == []


def test_claimer_skips_batches_taken_elsewhere(tmp_path):
    import contextlib

    _db, processor = _build(tmp_path, {})

    class S:
        def batch_status_exists(self, code):
            return True

        def claim_batch(self, b, codes, code, owner=None, procedure=""):
            return None if b == "41" else "PROCESSING"     # 41 was taken by another system

    class Db:
        @contextlib.contextmanager
        def session(self):
            yield S()

    class Cfg:
        class run:
            pending_batches_query = "SELECT 1"
            claim_procedure, claim_batch_procedure = "", ""
            batch_status_in_progress = "IN_PROGRESS"
            claimable_statuses = ("PROCESSING",)

    original = runner.pending_batch_ids
    runner.pending_batch_ids = lambda cfg, db, limit=None: ["40", "41", "42", "43"]
    try:
        fetch_new = runner.make_claimer(Cfg, Db(), processor)
        assert fetch_new(2, {"40"}) == ["42", "43"]        # 40 excluded (already run), 41 lost the race
    finally:
        runner.pending_batch_ids = original
    assert processor._claimed == {"42": "PROCESSING", "43": "PROCESSING"}
