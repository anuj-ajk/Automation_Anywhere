"""Daisy / Energy Manager sync for one 'Source: Daisy' file.

Port of MainTask_Retrive_Data_Daisy_EM + ST_Retrieve_and_Load_{Portfolio,Invoices,HH}Data.
Fixes over the bot: the data source type is matched ignoring spaces/case ('Energy Manager'
== 'EnergyManager'), unknown categories are marked FAILED instead of being skipped, and
client names are passed as parameters, not pasted into SQL.
"""
from __future__ import annotations

import time
from datetime import datetime
from typing import Any

from . import progress as pg
from . import status as st
from .db import EsgDb
from .loader import StagingLoader, spec_for_daisy_category
from .logging_setup import ContextLogger, get_logger, log_context, timed_step
from .models import BatchContext, FileOutcome
from .source_systems import RETRIEVE_SPECS, SourceSystems


class DaisyFlow:
    def __init__(self, db: EsgDb, sources: SourceSystems, loader: StagingLoader):
        self.db = db
        self.sources = sources
        self.loader = loader
        self.progress = pg.ProgressReporter(db)  # shared with the Normalize flow by runner.build

    def run_file(self, ctx: BatchContext, row: dict[str, Any], log: ContextLogger) -> FileOutcome:
        file_id = str(row["FileId"])
        category = str(row.get("CategorySelected") or "")
        log = get_logger(__name__, batch_id=ctx.batch_id, file_id=file_id)
        started = time.monotonic()
        with log_context(batch_id=ctx.batch_id, file_id=file_id):
            log.info("Daisy sync started (category: %s)", category or "<empty>")
            self.progress.start(file_id)
            outcome = self._run(ctx, file_id, category, log)
            self.progress.set(file_id, pg.DONE, log)  # finished, whatever the outcome
            log.info("Daisy sync finished: %s in %.1fs", outcome.status, time.monotonic() - started)
            return outcome

    def _run(self, ctx: BatchContext, file_id: str, category: str, log: ContextLogger) -> FileOutcome:
        name = f"Source: Daisy ({category})"
        try:
            load_spec = spec_for_daisy_category(category)
            retrieve_spec = RETRIEVE_SPECS.get(category.strip().lower())
            if load_spec is None or retrieve_spec is None:
                raise ValueError(f"Unsupported Daisy category '{category}'")

            with self.db.session() as s:
                source_type = s.data_source_type(ctx.client_name)
            log.info("Data source type for %s: %s", ctx.client_name, source_type or "<not set>")
            if not source_type:
                raise ValueError(f"Data source not configured for {ctx.client_name}")
            log.info("%s load from %s for %s, %s to %s", category, source_type, ctx.client_name,
                     ctx.report_start, ctx.report_end)

            self.progress.set(file_id, pg.DAISY_QUERYING, log)
            work = ctx.work_dir / f"d{file_id}"
            csv_path = work / (f"{retrieve_spec.csv_prefix}_FileID{file_id}_ReportID{ctx.report_id}_"
                               f"{datetime.now():%Y%m%d_%H%M%S}.csv")
            with timed_step(log, f"Export {category} from {source_type}"):
                rows = self.sources.export_to_csv(source_type, retrieve_spec, ctx.client_name,
                                                  ctx.report_start, ctx.report_end, csv_path)
            log.info("%d row(s) exported to %s", rows, csv_path.name if rows else "<no file>")
            if rows == 0:
                return self._no_data(file_id, name, category, source_type, log)
            self.progress.set(file_id, pg.DAISY_EXPORTED, log)

            with timed_step(log, f"Load {category} into {load_spec.staging_table}"), self.db.session() as s:
                result = self.loader.load(s, load_spec, csv_path, work, file_id, ctx.report_id)
                if result.skipped_empty:
                    return self._no_data(file_id, name, category, source_type, log, session=s)
                s.set_file_status(file_id, st.COMPLETED, clear=True)
            log.info("Status -> %s", st.COMPLETED)
            log.info("%s loaded: %d row(s)", category, result.rows_inserted)
            return FileOutcome(file_id, name, "daisy", st.COMPLETED)
        except Exception as exc:
            msg = f"Daisy {category} load: {exc}"
            log.exception(msg)
            try:
                with self.db.session() as s:
                    s.set_file_status(file_id, st.FAILED, msg)
                log.info("Status -> %s", st.FAILED)
            except Exception as db_exc:
                log.error("Unable to update FAILED status: %s", db_exc)
            return FileOutcome(file_id, name, "daisy", st.FAILED, msg)

    def _no_data(self, file_id, name, category, source_type, log, session=None) -> FileOutcome:
        msg = f"No {category} data returned from {source_type} for the selected client and period"
        log.warning("%s -> status %s", msg, st.DATA_NOT_AVAILABLE)
        if session is not None:
            session.set_file_status(file_id, st.DATA_NOT_AVAILABLE, msg)
        else:
            with self.db.session() as s:
                s.set_file_status(file_id, st.DATA_NOT_AVAILABLE, msg)
        return FileOutcome(file_id, name, "daisy", st.DATA_NOT_AVAILABLE, msg)
