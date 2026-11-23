"""Automation Anywhere entry for the AI normalization sub task (replaces ST_GetAIResponse_ParallelExecution).

In the bot:
    1. Python script: Open             -> file = <path>\\SECR_AIResponse\\aa_entry.py, session "AIResp"
    2. Python script: Execute function -> function get_ai_responses, argument = Dictionary, output = sAIOutput
    3. Python script: Close            -> session "AIResp"

Dictionary keys (every value may be text):
    files            the split sheets: JSON array [{"FileID": "77", "SplittedFilePath": "C:\\...xlsx"}, ...]
                     or text "77|C:\\a.xlsx;78|C:\\b.xlsx"
    input_csv        instead of files: a CSV with columns FileID,SplittedFilePath (DataTable written to CSV)
    output_csv       optional: CSV written with FileID,NormalizedCSVLink,FileName,Category
                     (read it back into tNormalizedCSVFileLinkTable)
    project_id / Agent_Project_ID, agent_id / Agent_ID, base_uri / ENV_Base_URI      (from dProcessConfig)
    db_host / ESGDBServerName, db_name / DatabaseName, db_port / MySQL_Port          (from dProcessConfig)
    update_db        default true: write UploadedFile status + CategoryRecognized like the bot
    max_parallel     default 10, poll_interval_seconds default 40, max_poll_attempts default 15
    debug            "true" for a detailed log;  log_dir  optional
    secrets          optional dictionary: SECR_ODIN_API_KEY, SECR_ODIN_API_SECRET, SECR_DB_USER, SECR_DB_PASSWORD

Output: always a JSON string (never raises):
    {"status": "SUCCESS" | "PARTIAL" | "FAILED", "message": "...", "log_file": "...", "output_csv": "...",
     "links": [{"FileID", "NormalizedCSVLink", "FileName", "Category"}],
     "files": [{"file_id", "status", "sheets", "success", "failed", "description"}]}
"""
from __future__ import annotations

import json
import logging
import os
import sys
import traceback
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()  # load .env file if present

SECR_ODIN_API_KEY = os.environ.get("SECR_ODIN_API_KEY")
SECR_ODIN_API_SECRET = os.environ.get("SECR_ODIN_API_SECRET")
SECR_DB_USER = os.environ.get("SECR_DB_USER")
SECR_DB_PASSWORD = os.environ.get("SECR_DB_PASSWORD")

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:  # AA runs this file directly, so make the package importable
    sys.path.insert(0, str(HERE))

_ALLOWED_SECRETS = ("SECR_ODIN_API_KEY", "SECR_ODIN_API_SECRET", "SECR_DB_USER", "SECR_DB_PASSWORD")


def _apply_secrets(secrets) -> None:
    if not secrets:
        return
    if isinstance(secrets, str):
        secrets = json.loads(secrets)
    for key, val in dict(secrets).items():
        if key not in _ALLOWED_SECRETS:
            raise ValueError(f"secrets key '{key}' is not one of {', '.join(_ALLOWED_SECRETS)}")
        os.environ[key] = str(val)


def get_ai_responses(args) -> str:
    try:
        if isinstance(args, str):
            args = json.loads(args)
        args = dict(args or {})
        _apply_secrets(args.pop("secrets", None))

        from ai_response.runner import run

        return json.dumps(run(args), default=str)
    except Exception as exc:  # AA only sees the returned text
        logging.getLogger("ai_response.aa_entry").exception("get_ai_responses failed")
        return json.dumps({"status": "FAILED", "message": f"{type(exc).__name__}: {exc}", "log_file": "",
                           "output_csv": "", "links": [], "files": [], "trace": traceback.format_exc()})


if __name__ == "__main__":  # manual test: python aa_entry.py "<json dictionary>"
    print(get_ai_responses(
        {
            "input_csv": r"X:\Automation Anywhere\Automation Processes\Environmental Social and Governance\Automation_Anywhere\SECR_AIResponse\tests\input_csv.csv",
            "output_csv": r"X:\Automation Anywhere\Automation Processes\Environmental Social and Governance\Automation_Anywhere\SECR_AIResponse\tests\output_csv.csv",
            "Agent_Project_ID": "356316e20dcb48658ce127",
            "Agent_ID": "63622",
            "ENV_Base_URI": "https://api.getodin.ai",
            "ESGDBServerName": "esg-instance.cm18ojbg8cjx.eu-west-2.rds.amazonaws.com",
            "DatabaseName": "esgdb",
            "MySQL_Port": 3306,
            "update_db": False,
            "max_parallel": 10,
            "poll_interval_seconds": 40,
            "max_poll_attempts": 15,
            "debug": True,
            "log_dir": r"X:\Automation Anywhere\Automation Processes\Environmental Social and Governance\Automation_Anywhere\SECR_AIResponse\tests\logs",
            "secrets": {
                "SECR_ODIN_API_KEY": SECR_ODIN_API_KEY,
                "SECR_ODIN_API_SECRET": SECR_ODIN_API_SECRET,
                "SECR_DB_USER": SECR_DB_USER,
                "SECR_DB_PASSWORD": SECR_DB_PASSWORD
            }
        }
    ))
