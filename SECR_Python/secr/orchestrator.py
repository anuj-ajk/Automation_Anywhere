"""Run many batches in parallel (default 10 at a time)."""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Iterable

from .logging_setup import get_logger
from .models import BatchResult


def unique_ids(batch_ids: Iterable) -> list[str]:
    seen, out = set(), []
    for b in batch_ids:
        b = str(b).strip()
        if b and b not in seen:
            seen.add(b)
            out.append(b)
    return out


def run_batches(
    batch_ids: Iterable,
    process_batch: Callable[[str], BatchResult],
    max_parallel_batches: int = 10,
) -> list[BatchResult]:
    """Process every batch id, at most ``max_parallel_batches`` at the same time.

    A batch that raises is reported as FAILED; it never stops the other batches.
    Results are returned in the order the ids were given.
    """
    log = get_logger(__name__)
    given = list(batch_ids)
    ids = unique_ids(given)
    if len(ids) != len(given):
        log.info("Removed %d duplicate / empty batch id(s)", len(given) - len(ids))
    if not ids:
        log.warning("No batches to process")
        return []
    workers = max(1, min(max_parallel_batches, len(ids)))
    log.info("Processing %d batch(es), up to %d in parallel", len(ids), workers)
    started = time.monotonic()

    results: dict[str, BatchResult] = {}
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="batch") as pool:
        futures = {pool.submit(process_batch, b): b for b in ids}
        for fut in as_completed(futures):
            b = futures[fut]
            try:
                results[b] = fut.result()
            except Exception as exc:  # defensive: process_batch already catches its own errors
                log.exception("Batch %s crashed", b)
                results[b] = BatchResult(b, "FAILED", error=str(exc))
            done = len(results)
            log.info("Progress: %d/%d batch(es) done (batch %s: %s, %.1fs); %d still running or queued",
                     done, len(ids), b, results[b].state, results[b].seconds, len(ids) - done)

    log.info("All batches done in %.1fs", time.monotonic() - started)
    return [results[b] for b in ids]


def summarize(results: list[BatchResult]) -> str:
    lines = [f"{'Batch':>8}  {'State':<27} {'Files':>5} {'OK':>4} {'Secs':>7}  Error"]
    for r in results:
        ok = sum(1 for f in r.files if f.ok)
        lines.append(f"{r.batch_id:>8}  {r.state:<27} {len(r.files):>5} {ok:>4} {r.seconds:>7.1f}  {r.error}")
    return "\n".join(lines)


def exit_code(results: list[BatchResult]) -> int:
    """0 = all good, 1 = at least one batch failed, 2 = only file-level errors."""
    if any(r.state == "FAILED" for r in results):
        return 1
    if any(r.state == "COMPLETED_WITH_FILE_ERRORS" for r in results):
        return 2
    return 0
