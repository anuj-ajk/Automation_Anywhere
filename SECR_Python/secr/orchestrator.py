"""Run many batches in parallel (default 10 at a time)."""
from __future__ import annotations

import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
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


def run_pending(
    process_batch: Callable[[str], BatchResult],
    fetch_new: Callable[[int, set], list],
    max_parallel_batches: int = 10,
    poll_seconds: float = 60,
    max_runtime_seconds: float = 0,
    max_batches: int = 0,
    initial_ids: Iterable = (),
    clock: Callable[[], float] = time.monotonic,
) -> list[BatchResult]:
    """Keep up to ``max_parallel_batches`` batches running and top up free slots with new pending batches.

    * ``fetch_new(n, exclude)`` claims up to ``n`` new batch ids (never one in ``exclude``).
    * While batches are running, new ones are looked for whenever a batch finishes and at least every
      ``poll_seconds``: with 2 running and a cap of 10, up to 8 more are taken.
    * Stops taking new batches after ``max_runtime_seconds`` (0 = no limit) or ``max_batches`` in total
      (0 = no limit); the running ones always finish. Ends when nothing runs and nothing is pending.
    Results are returned in the order the batches were started.
    """
    log = get_logger(__name__)
    started = clock()
    queue = unique_ids(initial_ids)
    seen: set[str] = set()
    order: list[str] = []
    results: dict[str, BatchResult] = {}
    log.info("Pending mode: up to %d batch(es) at a time, looking for new ones every %ss%s%s",
             max_parallel_batches, poll_seconds,
             f", taking new batches for {max_runtime_seconds:.0f}s" if max_runtime_seconds else "",
             f", at most {max_batches} batch(es)" if max_batches else "")

    with ThreadPoolExecutor(max_workers=max(1, max_parallel_batches), thread_name_prefix="batch") as pool:
        running: dict = {}
        while True:
            accepting = (not max_runtime_seconds or clock() - started < max_runtime_seconds) and \
                        (not max_batches or len(seen) < max_batches)
            free = max_parallel_batches - len(running)
            if max_batches:
                free = min(free, max_batches - len(seen))
            new: list[str] = []
            if accepting and free > 0:
                while queue and len(new) < free:
                    b = queue.pop(0)
                    if b not in seen:
                        new.append(b)
                if len(new) < free:
                    try:
                        fetched = fetch_new(free - len(new), seen | set(new))
                    except Exception:
                        log.exception("Could not look for pending batches; will try again")
                        fetched = []
                    new += [b for b in unique_ids(fetched) if b not in seen and b not in new]
            for b in new:
                seen.add(b)
                order.append(b)
                running[pool.submit(process_batch, b)] = b
            if new:
                log.info("Started %d batch(es): %s (%d running, %d free slot(s))", len(new), ", ".join(new),
                         len(running), max_parallel_batches - len(running))

            if not running:
                if accepting and queue:
                    continue
                break  # nothing running and nothing new: done

            done, _ = wait(running, timeout=poll_seconds, return_when=FIRST_COMPLETED)
            for fut in done:
                b = running.pop(fut)
                try:
                    results[b] = fut.result()
                except Exception as exc:  # defensive: process_batch already catches its own errors
                    log.exception("Batch %s crashed", b)
                    results[b] = BatchResult(b, "FAILED", error=str(exc))
                log.info("Batch %s finished: %s (%.1fs); %d still running", b, results[b].state,
                         results[b].seconds, len(running))

    log.info("Pending mode done: %d batch(es) in %.1fs", len(order), clock() - started)
    return [results[b] for b in order]


def summarize(results: list[BatchResult]) -> str:
    lines = [f"{'Batch':>8}  {'State':<27} {'Files':>5} {'OK':>4} {'Secs':>7}  Error"]
    for r in results:
        ok = sum(1 for f in r.files if f.ok)
        lines.append(f"{r.batch_id:>8}  {r.state:<27} {len(r.files):>5} {ok:>4} {r.seconds:>7.1f}  {r.error}")
    return "\n".join(lines)


def exit_code(results: list[BatchResult]) -> int:
    """0 = all good, 1 = at least one batch failed, 2 = file-level errors or a batch skipped because its row
    was locked (LOCKED: nothing done, run it again)."""
    if any(r.state == "FAILED" for r in results):
        return 1
    if any(r.state in ("COMPLETED_WITH_FILE_ERRORS", "LOCKED") for r in results):
        return 2
    return 0
