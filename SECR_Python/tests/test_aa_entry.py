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
    asked = {}

    def fake_pending(cfg, db, limit=None):
        asked["limit"] = limit
        return queue[:limit] if limit else list(queue)

    monkeypatch.setattr(runner, "pending_batch_ids", fake_pending)
    config = tmp_path / "config.yaml"
    config.write_text("esg_db:\n  host: localhost\nrun:\n  pending_batches_query: SELECT 1\n", encoding="utf-8")

    queue = ["41", "40"]
    out = json.loads(aa_entry.run_batches({"pending": "true", "batch_ids": "43", "config_path": str(config)}))
    assert out["status"] == "SUCCESS", out
    assert [b["batch_id"] for b in out["batches"]] == ["43", "41", "40"]

    out = json.loads(aa_entry.run_batches({"batch_ids": "pending", "pending_limit": "1", "config_path": str(config)}))
    assert asked["limit"] == 1 and [b["batch_id"] for b in out["batches"]] == ["41"]

    queue = []
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
