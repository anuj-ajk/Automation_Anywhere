"""Staging-table load + stored procedures.

Port of ST_InsertDataIntoDB (AI-normalized CSVs) and the load half of
ST_Retrieve_and_Load_{Portfolio,Invoices,HH}Data (Daisy CSVs).
The CSV -> table column mappings are copied from the bots' "Database: Insert in bulk" actions.
"""
from __future__ import annotations

import contextlib
import csv
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence

from .csv_clean import clean_csv
from .db import EsgSession
from .logging_setup import current_context, log_context


def _m(*cols) -> tuple[tuple[str, str], ...]:
    """Build a mapping; plain strings map to a table column of the same name."""
    return tuple((c, c) if isinstance(c, str) else c for c in cols)


ERROR_COLS = ("Normalization_Confidence", "Reason_for_Confidence", "Error_Code",
              "Error_Field", "Error_Value", "Error_Message", "FileId")
HH_SLOTS = ["0030", "0100", "0130", "0200", "0230", "0300", "0330", "0400", "0430", "0500",
            "0530", "0600", "0630", "0700", "0730", "0800", "0830", "0900", "0930", "1000",
            "1030", "1100", "1130", "1200", "1230", "1300", "1330", "1400", "1430", "1500",
            "1530", "1600", "1630", "1700", "1730", "1800", "1830", "1900", "1930", "2000",
            "2030", "2100", "2130", "2200", "2230", "2300", "2330", "0000"]


@dataclass(frozen=True)
class ProcCall:
    name: str
    in_args: tuple[str, ...]          # names from: file_id, report_id, created_by
    n_out: int = 1                    # p_Message [, p_Status]
    has_status: bool = False          # second OUT param is a status ('ERROR' = failure)

    @property
    def report_level(self) -> bool:
        """Works on the whole report (e.g. CalculateInvoiceData), not on this file's staging rows."""
        return "file_id" not in self.in_args


@dataclass(frozen=True)
class LoadSpec:
    key: str
    staging_table: str
    columns: tuple[tuple[str, str], ...]  # (csv column, table column)
    procs: tuple[ProcCall, ...] = field(default_factory=tuple)


_LOAD = lambda name: ProcCall(name, ("file_id", "report_id"))  # noqa: E731
_CALC_INVOICE = ProcCall("CalculateInvoiceData", ("report_id", "created_by"), n_out=2, has_status=True)

# ---------- AI-normalized CSVs (keys = Odin category keys) ----------
NORMALIZED_SPECS: dict[str, LoadSpec] = {
    "portfolio": LoadSpec(
        "portfolio", "Staging_Portfolio",
        _m("Supply_ID", "Site_Code", "Site_Name", "Street_Address_Line_1", "Street_Address_Line_2",
           "Building_Type", "Town_or_City", "Post_Code", "Country", "Subsidiary_Company", "Utility_Type",
           "Utility_Supplier_Landlord", "MPAN_MPRN_or_Reference", "MPAN_Top_Line",
           ("Supplier_name", "Current_Supplier_name"),
           ("Supplier_Contract_Start_Date", "Contract_Start_Date_Current_Supplier"),
           ("Supplier_Contract_End_Date", "Contract_End_Date_Current_Supplier"),
           "Date_Supply_Held_From", "Date_Supply_Held_To", *ERROR_COLS),
        (_LOAD("LoadPortfolioData"), ProcCall("UpdatePortfolioDerivedFields", ("report_id",))),
    ),
    "landlord": LoadSpec(
        "landlord", "Staging_Landlord",
        _m("Supply_ID", "Site_name", "MPAN_MPRN_Meter_name", "Fuel_Type", "Metered_supply_service_charge",
           "Date_From", "Date_To", "kWh", "Cost_inc_VAT", "Cost_exc_VAT", "Cost_Unit", "Floor_area",
           "Floor_area_Unit", "Reason_for_Confidence", "Error_Code", "Error_Field", "Error_Value",
           "Error_Message", "FileId"),
        (_LOAD("LoadLandlordData"),),
    ),
    "transport": LoadSpec(
        "transport", "Staging_Transport",
        _m("Site_Name_or_Reference", "Transport_Category_Emission_Source", "Vehicle", "Fuel",
           "Measurement_Unit_Type_1", "Units_1", "Measurement_Unit_1",
           "Measurement_Unit_Type_2", "Units_2", "Measurement_Unit_2",
           "Measurement_Unit_Type_3", "Units_3", "Measurement_Unit_3",
           "Measurement_Unit_Type_4", "Units_4", "Measurement_Unit_4",
           "Vehicle_Class", "Date_From", "Date_To", "Non_UK_Data", "Subsidiary_Data", "Job_Type",
           "Vehicle_Registration", *ERROR_COLS),
        (_LOAD("LoadTransportData"),),
    ),
    "other_fuels": LoadSpec(
        "other_fuels", "Staging_Other_Fuels",
        _m("Site_Name_or_Reference", "Emission_Source", "Fuel", "Measurement_Unit_Type", "Date_From",
           "Date_To", "Units", "Measurement_Unit", "Non_UK_Data", "Subsidiary_Data", "Job_Type", *ERROR_COLS),
        (_LOAD("LoadOtherFuelData"),),
    ),
    "metrics": LoadSpec(
        "metrics", "Staging_Metrics",
        _m("Company_Level_Metric_Type", "Units", "Date_From", "Date_To", "Measurement_Unit", *ERROR_COLS),
        (_LOAD("LoadMetricInputData"),),
    ),
    "energy_efficiency_narrative": LoadSpec(
        "energy_efficiency_narrative", "Staging_Energy_Efficiency_Narrative",
        _m("Reporting_Year", "Project_Title", "Brief_Description", "Impact_on_Business_Operations",
           "Initiative_Type", *ERROR_COLS),
        (_LOAD("LoadEnergyEfficiencyNarrativeData"),),
    ),
    "invoice": LoadSpec(
        "invoice", "Staging_Invoices",
        _m("Name", ("Address1", "Address_1"), ("Address2", "Address_2"), "Town", ("PostCode", "Post_Code"),
           "Utility", ("MeasurementUnit", "Measurement_Unit"), ("ReferenceNumber", "Reference_Number"),
           "Supplier", ("MPANMPRWaterSPID", "M1_MPAN_MPR_Water_SPID"), ("TopLine", "Top_Line"),
           ("EstimatedInvoice", "Estimated_Invoice"), "Units", "Cost", "VAT", "CCL", ("InvoiceDate", "Date"),
           ("PreviousInvoiceDate", "Previous_Date"), ("InvoiceTaxDate", "Invoice_Tax_Date"),
           ("InvoiceType", "Invoice_Type"), *ERROR_COLS),
        (_LOAD("LoadInvoiceData"), _CALC_INVOICE),
    ),
    "half_hourly_data": LoadSpec(
        "half_hourly_data", "Staging_HHData",
        _m(("MPANCore1", "M1_MPAN_MPR_Water_SPID"), "Utility", "Date",
           *[f"HH_{s}" for s in HH_SLOTS], *ERROR_COLS),
        (ProcCall("LoadHHData", ("file_id", "report_id", "created_by")),),
    ),
}

# ---------- Daisy CSVs exported from Energy Manager / NAV (keys = UploadedFile.CategorySelected) ----------
_DAISY_HH_SLOTS = [(f"{s[:2]}:{s[2:]}" if s != "0000" else "24:00", f"HH_{s}") for s in HH_SLOTS]

DAISY_SPECS: dict[str, LoadSpec] = {
    "portfolio": LoadSpec(
        "portfolio", "Staging_Portfolio",
        _m("Client", ("Site ID", "Site_ID"), ("Meter Id", "Meter_Id"), ("Sugar ID", "Sugar_ID"),
           ("Site Code", "Site_Code"), ("Site Name", "Site_Name"), ("Address1", "Street_Address_Line_1"),
           ("Address2", "Street_Address_Line_2"), ("Town", "Town_or_City"), "County",
           ("Post Code", "Post_Code"), "Country", ("Subsidiary Company", "Subsidiary_Company"),
           ("Utility", "Utility_Type"), ("Utility Supplier / Landlord", "Utility_Supplier_Landlord"),
           ("MPAN/MPR", "MPAN_MPRN_or_Reference"), ("M1_Code3", "MPAN_Top_Line"),
           ("Current Supplier", "Current_Supplier_name"),
           ("Contract Start Date", "Contract_Start_Date_Current_Supplier"),
           ("Contract End Date", "Contract_End_Date_Current_Supplier"),
           ("Held From", "Date_Supply_Held_From"), ("Held To", "Date_Supply_Held_To"),
           ("Contract Status", "Contract_Status"), "FileId"),
        (_LOAD("LoadPortfolioData"),),
    ),
    "invoices": LoadSpec(
        "invoices", "Staging_Invoices",
        _m(("Cust", "Name"), ("Address1", "Address_1"), ("Address2", "Address_2"), "Town",
           ("Postcode", "Post_Code"), "Utility", ("Units", "Measurement_Unit"),
           ("Reference Number", "Reference_Number"), "Supplier",
           ("M1 MPAN / MPR / Water SPID", "M1_MPAN_MPR_Water_SPID"), ("Top Line", "Top_Line"),
           ("Estimated Invoice", "Estimated_Invoice"), ("Invoice Units", "Units"), "Cost", "VAT", "CCL",
           ("From Date", "Previous_Date"), "Date", ("Invoice Tax Date", "Invoice_Tax_Date"),
           ("InvType", "Invoice_Type"), "FileId"),
        (_LOAD("LoadInvoiceData"), _CALC_INVOICE),
    ),
    "half hourly data": LoadSpec(
        "half hourly data", "Staging_HHData",
        _m(("Cust", "Name"), ("Address1", "Address_1"), ("Address2", "Address_2"), "Town",
           ("Postcode", "Post_Code"), "Utility", "Units", "Date",
           ("M1 MPAN / MPR / Water SPID", "M1_MPAN_MPR_Water_SPID"), *_DAISY_HH_SLOTS,
           "Blank1", "Blank2", ("TotalUnits", "Total_Units"), "FileId"),
        (ProcCall("LoadHHData", ("file_id", "report_id", "created_by")),),
    ),
}


class LoadError(Exception):
    pass


@dataclass
class LoadResult:
    rows_inserted: int
    skipped_empty: bool
    messages: list[str]


class StagingLoader:
    def __init__(self, created_by: str, serialize: bool, lock_timeout: int, chunk_size: int, log,
                 clear_staging: bool = True, db=None, insert_connections: int = 1,
                 parallel_insert_min_rows: int = 20000):
        self.created_by = created_by
        self.serialize = serialize
        self.lock_timeout = lock_timeout
        self.chunk_size = chunk_size
        self.log = log
        self.clear_staging = clear_staging
        # Parallel bulk insert: big files are split over up to ``insert_connections`` connections.
        self.db = db
        self.insert_connections = max(1, int(insert_connections))
        self.parallel_insert_min_rows = max(1, int(parallel_insert_min_rows))
        # (file_id, table) pairs already cleared in this run: the sheets of one file that share a staging
        # table must not delete each other's rows, only what an earlier run left behind.
        self._cleared: set[tuple[str, str]] = set()
        self._cleared_lock = threading.Lock()

    def load(
        self, session: EsgSession, spec: LoadSpec, raw_csv: Path, work_dir: Path, file_id: str, report_id: str
    ) -> LoadResult:
        self.log.info("Loading %s into %s (procedures: %s)", raw_csv.name, spec.staging_table,
                      ", ".join(p.name for p in spec.procs))
        cleaned = clean_csv(raw_csv, work_dir / f"Cleaned_{raw_csv.stem}.csv", file_id=file_id)
        self.log.debug("Cleaned %s: %d rows, %d blanks filled", raw_csv.name, cleaned.rows_processed,
                       cleaned.blanks_filled)
        if cleaned.data_rows == 0:
            self.log.info("%s has no data rows; nothing to load", raw_csv.name)
            return LoadResult(0, True, [])

        columns, rows = self._read_mapped(cleaned.output_file, spec)
        self.log.info("%d data row(s), %d mapped column(s)", len(rows), len(columns))
        args = {"file_id": file_id, "report_id": report_id, "created_by": self.created_by}

        file_procs = tuple(p for p in spec.procs if not p.report_level)
        report_procs = tuple(p for p in spec.procs if p.report_level)

        # 1. Staging part: the staging tables and the Load* procedures are shared by every batch, so only one
        #    worker (across all processes, via a MySQL named lock) inserts + runs Load* for a table at a time.
        lock_name = f"secr_load_{spec.staging_table}"
        if self.serialize:
            self.log.debug("Waiting for staging lock %s (only one load per staging table at a time)", lock_name)
            with session.named_lock(lock_name, self.lock_timeout):
                inserted, messages = self._insert_and_call(session, spec, columns, rows, args, file_procs)
        else:
            inserted, messages = self._insert_and_call(session, spec, columns, rows, args, file_procs)

        # 2. Report-level procedures (UpdatePortfolioDerivedFields, CalculateInvoiceData) do not read the staging
        #    table, so they run AFTER the staging lock is released: other batches can load the same table
        #    meanwhile. They are serialised per report and procedure instead.
        for proc in report_procs:
            proc_lock = f"secr_report_{report_id}_{proc.name}"
            if self.serialize:
                with session.named_lock(proc_lock, self.lock_timeout):
                    messages.append(self._call(session, proc, args, inserted, spec))
            else:
                messages.append(self._call(session, proc, args, inserted, spec))
        return LoadResult(inserted, False, messages)

    def _clear_previous_rows(self, session, table: str, file_id: str) -> None:
        if not self.clear_staging:
            return
        with self._cleared_lock:
            if (file_id, table) in self._cleared:
                return
            self._cleared.add((file_id, table))
        deleted = session.delete_staging_rows(table, file_id)
        if deleted:
            self.log.warning("Deleted %d row(s) left in %s for FileId %s by an earlier run", deleted, table, file_id)
        else:
            self.log.info("No earlier rows in %s for FileId %s", table, file_id)

    def _insert_and_call(self, session, spec, columns, rows, args, procs) -> tuple[int, list[str]]:
        self._clear_previous_rows(session, spec.staging_table, str(args["file_id"]))
        inserted = self._bulk_insert(session, spec.staging_table, columns, rows)
        self.log.info("Inserted %d row(s) into %s", inserted, spec.staging_table)
        return inserted, [self._call(session, proc, args, inserted, spec) for proc in procs]

    def _bulk_insert(self, session, table: str, columns, rows: list) -> int:
        """Insert on this connection, or split a big file over extra connections that are free right now."""
        wanted = min(self.insert_connections, len(rows) // self.parallel_insert_min_rows + 1)
        if self.db is None or wanted < 2:
            return session.bulk_insert(table, columns, rows, self.chunk_size)

        with contextlib.ExitStack() as stack:
            extra = []
            for _ in range(wanted - 1):
                s = stack.enter_context(self.db.try_session())
                if s is None:  # no free connection: use what we have, never wait (no deadlock)
                    break
                extra.append(s)
            sessions = [session] + extra
            if len(sessions) == 1:
                self.log.info("No free DB connection for a parallel insert; inserting on one connection")
                return session.bulk_insert(table, columns, rows, self.chunk_size)

            size = -(-len(rows) // len(sessions))  # ceiling division
            slices = [rows[i:i + size] for i in range(0, len(rows), size)]
            self.log.info("Inserting %d row(s) into %s over %d connection(s) (%d rows each)", len(rows), table,
                          len(slices), size)
            started, context = time.monotonic(), current_context()

            def insert(pair):
                s, part = pair
                with log_context(**context):
                    return s.bulk_insert(table, columns, part, self.chunk_size)

            with ThreadPoolExecutor(max_workers=len(slices), thread_name_prefix="insert") as pool:
                total = sum(pool.map(insert, zip(sessions, slices)))
            self.log.info("Parallel insert into %s: %d row(s) in %.1fs", table, total, time.monotonic() - started)
            return total

    def _call(self, session, proc: ProcCall, args: dict, inserted: int, spec: LoadSpec) -> str:
        in_values = [args[a] for a in proc.in_args]
        try:
            outs = session.call_proc(proc.name, in_values, proc.n_out)
        except Exception as exc:
            # A MySQL error raised while the procedure runs (e.g. 1136 "Column count doesn't match")
            # comes from the procedure's own SQL, not from this bulk insert.
            self.log.error("Stored procedure %s(%s) failed inside MySQL: %s. The %d row(s) just inserted "
                           "into %s for FileId %s are still there.", proc.name,
                           ", ".join(map(str, in_values)), exc, inserted, spec.staging_table, args["file_id"])
            raise LoadError(f"Error inside stored procedure {proc.name}: {exc}") from exc
        message = str(outs[0] or "")
        status = str(outs[1] or "") if proc.has_status and len(outs) > 1 else ""
        self.log.info("%s -> message=%r%s", proc.name, message, f", status={status!r}" if status else "")
        if not _proc_succeeded(message, status, proc.has_status):
            self.log.error("%s reported a failure: %s", proc.name, message or status or "no message returned")
            raise LoadError(f"Error in {proc.name}: {message or status or 'no message returned'}")
        return f"{proc.name}: {message}"

    def _read_mapped(self, cleaned_csv: Path, spec: LoadSpec) -> tuple[list[str], list[tuple]]:
        with cleaned_csv.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.reader(fh)
            header = [h.strip() for h in next(reader)]
            self.log.debug("%s header: %s", cleaned_csv.name, header)
            index = {name.lower(): i for i, name in enumerate(header)}
            missing = [c for c, _ in spec.columns if c.lower() not in index]
            if missing:
                # The AA bulk insert needed every mapped column; we load NULL and warn instead.
                self.log.warning("%s is missing column(s) %s; loading NULL for them", cleaned_csv.name, missing)
            positions = [index.get(c.lower()) for c, _ in spec.columns]
            rows = [tuple(r[p] if p is not None and p < len(r) else None for p in positions) for r in reader]
        return [t for _, t in spec.columns], rows


def _proc_succeeded(message: str, status: str, has_status: bool) -> bool:
    if has_status and status:
        return status.strip().upper() != "ERROR"
    return "success" in message.lower()


def spec_for_daisy_category(category: str) -> LoadSpec | None:
    return DAISY_SPECS.get((category or "").strip().lower())


def columns_of(spec: LoadSpec) -> Sequence[str]:
    return [c for c, _ in spec.columns]
