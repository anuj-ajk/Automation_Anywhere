"""Tests with a fake Odin client and a fake status DB (no network, no MySQL)."""
from __future__ import annotations

import csv
import json
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import aa_entry  # noqa: E402
from ai_response import runner  # noqa: E402
from ai_response.processor import AIResponseProcessor, SheetJob  # noqa: E402


class FakeClient:
    """Behaviour by file name: 'ok_<n>_<cat>[+<cat>]' answers after n polls, 'never' never answers,
    'nosend' fails to create the chat, 'empty' answers without links, 'flaky' errors once then answers."""

    def __init__(self):
        self.lock = threading.Lock()
        self.polls: dict[str, int] = {}
        self.names: dict[str, str] = {}
        self.active = self.peak = 0

    def create_chat(self, name):
        if name.startswith("nosend"):
            raise RuntimeError("HTTP 500")
        with self.lock:
            chat_id = f"c{len(self.names) + 1}"
            self.names[chat_id] = name
        return chat_id

    def send_file(self, chat_id, path):
        with self.lock:
            self.active += 1
            self.peak = max(self.peak, self.active)
        time.sleep(0.02)
        with self.lock:
            self.active -= 1
        return True

    def get_csv_links(self, chat_id):
        name = Path(self.names[chat_id]).stem
        with self.lock:
            self.polls[chat_id] = self.polls.get(chat_id, 0) + 1
            n = self.polls[chat_id]
        if name.startswith("never"):
            return None
        if name.startswith("empty"):
            return {}
        if name.startswith("flaky") and n == 1:
            raise RuntimeError("HTTP 502")
        if name.startswith("flaky"):
            return {"metrics": f"https://x/{name}_metrics.csv"}
        _, wait, cats = name.split("_", 2)
        cats = cats.split("_")[0]
        if n < int(wait):
            return None
        return {c: f"https://x/{name}_{c}.csv" for c in cats.split("+")}


class FakeDb:
    def __init__(self):
        self.status: dict[str, list] = {}
        self.categories: dict[str, list] = {}

    def set_status(self, file_id, code, append_description=None):
        self.status.setdefault(file_id, []).append((code, append_description))

    def add_category(self, file_id, name):
        self.categories.setdefault(file_id, []).append(name)


def _jobs(tmp_path, spec):
    jobs = []
    for file_id, name in spec:
        p = tmp_path / f"{name}.xlsx"
        p.write_bytes(b"x")
        jobs.append(SheetJob(file_id, p))
    return jobs


def _processor(client, db, max_parallel=10, attempts=5):
    sleeps = []
    proc = AIResponseProcessor(client, db, max_parallel, 40, attempts, sleep=sleeps.append)
    return proc, sleeps


def test_statuses_links_and_parallel_polling(tmp_path):
    client, db = FakeClient(), FakeDb()
    proc, sleeps = _processor(client, db)
    jobs = _jobs(tmp_path, [
        ("77", "ok_1_portfolio"), ("77", "ok_3_metrics+invoice"),        # all good
        ("78", "ok_2_transport"), ("78", "never_a"), ("78", "nosend_b"),  # partial
        ("79", "empty_a"),                                               # failed
        ("80", "flaky_a"),                                               # transient error, then OK
    ])
    result = proc.run(jobs)

    by_id = {f.file_id: f for f in result.files}
    assert by_id["77"].status == "NORMALIZATION_COMPLETED" and by_id["77"].success == 3
    assert by_id["78"].status == "PARTIAL_FAILURE" and by_id["78"].failed == 2
    assert "Time out" in by_id["78"].description and "not created" in by_id["78"].description
    assert by_id["79"].status == "FAILED_WHILE_NORMALIZATION"
    assert by_id["80"].status == "NORMALIZATION_COMPLETED"

    # All chats are polled in the same rounds: 5 rounds -> 4 waits, not one wait cycle per sheet.
    assert sleeps == [40] * 4
    assert [r["Category"] for r in result.links if r["FileID"] == "77"] == ["portfolio", "metrics", "invoice"]
    assert db.categories["77"] == ["Portfolio", "Metrics", "Invoice"]
    assert db.status["77"][0] == ("NORMALIZING", None) and db.status["77"][-1][0] == "NORMALIZATION_COMPLETED"
    assert "NORMALIZING" not in [c for c, _ in db.status.get("missing", [])]


def test_submit_runs_in_parallel_with_cap(tmp_path):
    client, db = FakeClient(), FakeDb()
    proc, _ = _processor(client, db, max_parallel=4)
    proc.run(_jobs(tmp_path, [(str(i), f"ok_1_metrics_{i}") for i in range(12)]))
    assert client.peak == 4


def test_parse_files_forms(tmp_path):
    a = runner.parse_files('[{"FileID": "77.0", "SplittedFilePath": "C:/a.xlsx"}]')
    assert (a[0].file_id, a[0].path.name) == ("77", "a.xlsx")
    b = runner.parse_files("77|C:/a.xlsx;78|C:/b.xlsx")
    assert [j.file_id for j in b] == ["77", "78"]
    f = tmp_path / "in.csv"
    f.write_text("FileID,SplittedFilePath\n77,C:/a.xlsx\n", encoding="utf-8")
    assert runner.parse_files(str(f))[0].path.name == "a.xlsx"


def test_run_writes_output_csv_and_json(tmp_path, monkeypatch):
    monkeypatch.setenv("SECR_ODIN_API_KEY", "k")
    monkeypatch.setenv("SECR_ODIN_API_SECRET", "s")
    jobs_csv = tmp_path / "in.csv"
    sheet = tmp_path / "ok_1_portfolio.xlsx"
    sheet.write_bytes(b"x")
    jobs_csv.write_text(f"FileID,SplittedFilePath\n77,{sheet}\n", encoding="utf-8")
    out_csv = tmp_path / "out.csv"

    def fake_build(cfg):
        assert cfg.project_id == "P" and cfg.agent_id == "A" and cfg.update_db is False
        return _processor(FakeClient(), FakeDb())[0]

    monkeypatch.setattr(runner, "build", fake_build)
    out = json.loads(aa_entry.get_ai_responses({
        "input_csv": str(jobs_csv), "output_csv": str(out_csv), "Agent_Project_ID": "P", "Agent_ID": "A",
        "update_db": "false", "log_dir": str(tmp_path / "logs"),
    }))
    assert out["status"] == "SUCCESS", out
    rows = list(csv.DictReader(out_csv.open(encoding="utf-8")))
    assert rows[0]["FileID"] == "77" and rows[0]["Category"] == "portfolio"
    log = Path(out["log_file"]).read_text(encoding="utf-8")
    assert "[file 77]" in log and "SECR_ODIN_API_KEY=set" in log and "=k " not in log


def test_errors_come_back_as_json():
    out = json.loads(aa_entry.get_ai_responses({"files": ""}))
    assert out["status"] == "FAILED"
    out = json.loads(aa_entry.get_ai_responses({"files": "1|x", "secrets": {"PATH": "x"}}))
    assert out["status"] == "FAILED" and "secrets key" in out["message"]
