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
        return processor, None, runner.Notifier(cfg.notify, runner.get_logger()), "fake.log"

    monkeypatch.setattr(runner, "build", fake_build)
    out = json.loads(aa_entry.run_batches({
        "batch_ids": "40,41,42",
        "config_path": str(ROOT / "config.example.yaml"),
        "max_batches": "3",
    }))
    assert out["status"] == "SUCCESS" and out["exit_code"] == 0
    assert seen["batches"] == 3
    assert [b["batch_id"] for b in out["batches"]] == ["40", "41", "42"]
    assert [b["files"] for b in out["batches"]] == [1, 2, 0]
    assert out["batches"][2]["state"] == "NO_FILES"


def test_errors_come_back_as_json():
    out = json.loads(aa_entry.run_batches({"batch_ids": ""}))
    assert out["status"] == "FAILED" and "batch_ids" in out["message"]
    out = json.loads(aa_entry.run_batches({"batch_ids": "40", "config_path": "missing.yaml"}))
    assert out["status"] == "FAILED" and "FileNotFoundError" in out["message"]
    out = json.loads(aa_entry.run_batches({"batch_ids": "40", "secrets": {"PATH": "x"}}))
    assert out["status"] == "FAILED" and "not a SECR_" in out["message"]
