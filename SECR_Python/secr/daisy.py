"""Daisy / Energy Manager sync for one 'Source: Daisy' file.

Port of MainTask_Retrive_Data_Daisy_EM + ST_Retrieve_and_Load_{Portfolio,Invoices,HH}Data.
Fixes over the bot: the data source type is matched ignoring spaces/case ('Energy Manager'
== 'EnergyManager'), unknown categories are marked FAILED instead of being skipped, and
client names are passed as parameters, not pasted into SQL.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from . import status as st
from .db import EsgDb
from .loader import StagingLoader, spec_for_daisy_category
from .logging_setup import ContextLogger
from .models import BatchContext, FileOutcome
from .source_systems import RETRIEVE_SPECS, SourceSystems


class DaisyFlow:
    def __init__(self, db: EsgDb, sources: SourceSystems, loader: StagingLoader):
        self.db = db
        self.sources = sources
        self.loader = loader

    def run_file(self, ctx: BatchContext, row: dict[str, Any], log: ContextLogger) -> FileOutcome:
        file_id = str(row["FileId"])
        category = str(row.get("CategorySelected") or "")
        log = log.child(file_id=file_id)
        name = f"Source: Daisy ({category})"
        try:
            load_spec = spec_for_daisy_category(category)
            retrieve_spec = RETRIEVE_SPECS.get(category.strip().lower())
            if load_spec is None or retrieve_spec is None:
                raise ValueError(f"Unsupported Daisy category '{category}'")

            with self.db.session() as s:
                source_type = s.data_source_type(ctx.client_name)
            if not source_type:
                raise ValueError(f"Data source not configured for {ctx.client_name}")
            log.info("%s load from %s for %s, %s to %s", category, source_type, ctx.client_name,
                     ctx.report_start, ctx.report_end)

            work = ctx.work_dir / f"daisy_{file_id}"
            csv_path = work / (f"{retrieve_spec.csv_prefix}_FileID{file_id}_ReportID{ctx.report_id}_"
                               f"{datetime.now():%Y%m%d_%H%M%S}.csv")
            rows = self.sources.export_to_csv(source_type, retrieve_spec, ctx.client_name,
                                              ctx.report_start, ctx.report_end, csv_path)
            if rows == 0:
                return self._no_data(file_id, name, category, source_type, log)

            with self.db.session() as s:
                result = self.loader.load(s, load_spec, csv_path, work, file_id, ctx.report_id)
                if result.skipped_empty:
                    return self._no_data(file_id, name, category, source_type, log, session=s)
                s.set_file_status(file_id, st.COMPLETED, clear=True)
            log.info("%s loaded: %d row(s)", category, result.rows_inserted)
            return FileOutcome(file_id, name, "daisy", st.COMPLETED)
        except Exception as exc:
            msg = f"Daisy {category} load: {exc}"
            log.error(msg)
            try:
                with self.db.session() as s:
                    s.set_file_status(file_id, st.FAILED, msg)
            except Exception as db_exc:
                log.error("Unable to update FAILED status: %s", db_exc)
            return FileOutcome(file_id, name, "daisy", st.FAILED, msg)

    def _no_data(self, file_id, name, category, source_type, log, session=None) -> FileOutcome:
        msg = f"No {category} data returned from {source_type} for the selected client and period"
        log.info(msg)
        if session is not None:
            session.set_file_status(file_id, st.DATA_NOT_AVAILABLE, msg)
        else:
            with self.db.session() as s:
                s.set_file_status(file_id, st.DATA_NOT_AVAILABLE, msg)
        return FileOutcome(file_id, name, "daisy", st.DATA_NOT_AVAILABLE, msg)
