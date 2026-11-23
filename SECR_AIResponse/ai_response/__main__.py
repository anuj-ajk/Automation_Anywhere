"""Command line: python -m ai_response --input-csv sheets.csv --project-id P --agent-id A [--no-db] ...

Exit code: 0 all files OK, 2 some sheets failed, 1 everything failed or bad input.
"""
from __future__ import annotations

import argparse
import json
import sys

from .runner import run


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="ai_response", description="Odin AI normalization (ST_GetAIResponse_ParallelExecution)")
    p.add_argument("--input-csv", required=True, help="CSV with FileID,SplittedFilePath")
    p.add_argument("--output-csv", help="CSV to write FileID,NormalizedCSVLink,FileName,Category")
    p.add_argument("--project-id", required=True)
    p.add_argument("--agent-id", required=True)
    p.add_argument("--base-uri", default="https://api.getodin.ai")
    p.add_argument("--db-host", help="ESG DB server (needed unless --no-db)")
    p.add_argument("--db-name", default="esgdb")
    p.add_argument("--db-port", default="3306")
    p.add_argument("--no-db", action="store_true", help="Do not write UploadedFile status / category")
    p.add_argument("--max-parallel", default="10")
    p.add_argument("--poll-interval", default="40")
    p.add_argument("--max-polls", default="15")
    p.add_argument("--debug", action="store_true")
    a = p.parse_args(argv)
    try:
        out = run({
            "input_csv": a.input_csv, "output_csv": a.output_csv, "project_id": a.project_id,
            "agent_id": a.agent_id, "base_uri": a.base_uri, "db_host": a.db_host, "db_name": a.db_name,
            "db_port": a.db_port, "update_db": not a.no_db, "max_parallel": a.max_parallel,
            "poll_interval_seconds": a.poll_interval, "max_poll_attempts": a.max_polls, "debug": a.debug,
        })
    except Exception as exc:
        print(f"FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print(json.dumps({k: out[k] for k in ("status", "message", "log_file", "output_csv")}, indent=2))
    return {"SUCCESS": 0, "PARTIAL": 2}.get(out["status"], 1)


if __name__ == "__main__":
    sys.exit(main())
