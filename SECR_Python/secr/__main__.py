"""Command line entry point.

Examples:
    python -m secr --config config.yaml run --batch-ids 40 41 42
    python -m secr --config config.yaml run --batch-file batches.txt
    python -m secr --config config.yaml run --pending
    python -m secr --config config.yaml run --batch-ids 40 --max-batches 1 --max-files 2
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .config import load_config
from .db import EsgDb
from .runner import apply_overrides, build, execute, pending_batch_ids


def _parse(argv):
    p = argparse.ArgumentParser(prog="secr", description="SECR Normalizer Uploaded Data (Python port)")
    p.add_argument("--config", default="config.yaml", help="Path to the YAML config file")
    p.add_argument("--debug", action="store_true", help="Verbose logging")
    sub = p.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Process one or more batches")
    src = run.add_mutually_exclusive_group(required=True)
    src.add_argument("--batch-ids", nargs="+", help="Batch ids to process")
    src.add_argument("--batch-file", type=Path, help="Text file with one batch id per line")
    src.add_argument("--pending", action="store_true", help="Use run.pending_batches_query from the config")
    run.add_argument("--max-batches", type=int, help="Override run.max_parallel_batches")
    run.add_argument("--max-files", type=int, help="Override run.max_parallel_files_per_batch")
    run.add_argument("--work-dir", help="Override run.work_dir (temp folder, e.g. %%TEMP%%\\SECR_work)")
    run.add_argument("--log-dir", help="Override run.log_dir")
    run.add_argument("--force", action="store_true",
                     help="Process every file again, also those already COMPLETED / DATA_NOT_AVAILABLE")
    return p.parse_args(argv)


def _batch_ids(args, cfg, db: EsgDb) -> list[str]:
    if args.batch_ids:
        return args.batch_ids
    if args.batch_file:
        return [ln.strip() for ln in args.batch_file.read_text(encoding="utf-8").splitlines()
                if ln.strip() and not ln.startswith("#")]
    try:
        return pending_batch_ids(cfg, db)
    except ValueError as exc:
        raise SystemExit(f"--pending: {exc}")


def main(argv=None) -> int:
    args = _parse(argv)
    cfg = load_config(args.config)
    apply_overrides(cfg, args.max_batches, args.max_files, force=args.force, work_dir=args.work_dir,
                    log_dir=args.log_dir, base=Path(args.config).resolve().parent)
    processor, db, notifier, _ = build(cfg, debug=args.debug)
    _, code, _ = execute(processor, notifier, _batch_ids(args, cfg, db), cfg.run.max_parallel_batches)
    return code


if __name__ == "__main__":
    sys.exit(main())
