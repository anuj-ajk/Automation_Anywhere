"""
ST_InsertDataIntoDB - Python port of the Automation Anywhere task bot
  Bots/Environmental Social and Governance/Streamlined Energy & Carbon Reporting/
  Tasks/SubTasks/ST_InsertDataIntoDB

What it does (same order as the bot):
  1. Works out the category from the CSV file name (the text before "_ESG").
  2. Cleans the CSV: fills blanks with "NA", flattens line breaks inside cells, removes
     double quotes, and appends a FileId column. The result is written to
     <input folder>\\Cleaned_<category>.csv
  3. Stops without loading if the CSV has no data rows.
  4. Connects to the ESG MySQL database and batch-inserts the cleaned CSV into the
     category's staging table (1000 rows per batch).
  5. Calls the category's stored procedure(s) and checks the p_Message they return.

Usage:
  python st_insert_data_into_db.py --csv <normalized csv> --input-folder <folder> \\
         --file-id 77 --report-id 5 [--config config.json] [--dry-run]

Database settings: read from --config (the bot's dConfig JSON, using the ProcessConfig
keys ESGDBServerName / DatabaseName / MySQL_Port), overridden by the environment
variables ESG_DB_HOST / ESG_DB_NAME / ESG_DB_PORT.
Credentials (they replace the "ESGDB MySQL" Credential Vault entry): set the environment
variables ESG_DB_USER and ESG_DB_PASSWORD. Credentials are never stored in this file.
"""

from __future__ import annotations

import argparse
import csv
import getpass
import json
import logging
import os
import re
import sys
import traceback
from dataclasses import dataclass, field
from pathlib import Path

log = logging.getLogger("ST_InsertDataIntoDB")

# Bot status values. The bot reads these from the global values gStatus_Success /
# gStatus_TechnicalException, which were not in the export. Change them to match.
STATUS_SUCCESS = "Success"
STATUS_TECHNICAL_EXCEPTION = "Technical Exception"
STATUS_NO_DATA = "No Data"

BATCH_SIZE = 1000


class BotException(Exception):
    """Equivalent of the AA ErrorHandler 'BotException'."""


# ----------------------------------------------------------------------------------
# Category configuration (taken from the bot's If / Else If branches)
# ----------------------------------------------------------------------------------

@dataclass
class ProcCall:
    name: str
    # Input parameter names, in order. Values come from the run context.
    inputs: list[str]
    # Output parameter names, in order. p_Message is always first.
    outputs: list[str] = field(default_factory=lambda: ["p_Message"])
    # The SP's p_Message must contain this text (case-insensitive) or the run fails.
    success_text: str = "success"


@dataclass
class Category:
    table: str
    procs: list[ProcCall]


CATEGORIES: dict[str, Category] = {
    "portfolio": Category("Staging_Portfolio", [
        ProcCall("LoadPortfolioData", ["p_FileId", "p_ReportId"]),
        ProcCall("UpdatePortfolioDerivedFields", ["p_ReportId"]),
    ]),
    "landlord": Category("Staging_Landlord", [
        ProcCall("LoadLandlordData", ["p_FileId", "p_ReportId"]),
    ]),
    "transport": Category("Staging_Transport", [
        ProcCall("LoadTransportData", ["p_FileId", "p_ReportId"]),
    ]),
    "other_fuels": Category("Staging_Other_Fuels", [
        ProcCall("LoadOtherFuelData", ["p_FileId", "p_ReportId"]),
    ]),
    "metrics": Category("Staging_Metrics", [
        ProcCall("LoadMetricInputData", ["p_FileId", "p_ReportId"]),
    ]),
    "energy_efficiency_narrative": Category("Staging_Energy_Efficiency_Narrative", [
        ProcCall("LoadEnergyEfficiencyNarrativeData", ["p_FileId", "p_ReportId"]),
    ]),
    "invoice": Category("Staging_Invoices", [
        ProcCall("LoadInvoiceData", ["p_FileId", "p_ReportId"], success_text="successful"),
        ProcCall("CalculateInvoiceData", ["p_ReportId", "p_CreatedBy"],
                 outputs=["p_Message", "p_Status"], success_text="successful"),
    ]),
    "half_hourly_data": Category("Staging_HHData", [
        ProcCall("LoadHHData", ["p_FileId", "p_ReportId", "p_CreatedBy"]),
    ]),
}

# CSV column -> table column. A plain string means both names are the same.
COLUMN_MAPPINGS: dict[str, list[str | tuple[str, str]]] = {
    # Portfolio
    "Staging_Portfolio": [
        "Supply_ID", "Site_Code", "Site_Name", "Street_Address_Line_1",
        "Street_Address_Line_2", "Building_Type", "Town_or_City", "Post_Code", "Country",
        "Subsidiary_Company", "Utility_Type", "Utility_Supplier_Landlord",
        "MPAN_MPRN_or_Reference", "MPAN_Top_Line",
        ("Supplier_name", "Current_Supplier_name"),
        ("Supplier_Contract_Start_Date", "Contract_Start_Date_Current_Supplier"),
        ("Supplier_Contract_End_Date", "Contract_End_Date_Current_Supplier"),
        "Date_Supply_Held_From", "Date_Supply_Held_To", "Normalization_Confidence",
        "Reason_for_Confidence", "Error_Code", "Error_Field", "Error_Value",
        "Error_Message", "FileId",
    ],
    # landlord
    "Staging_Landlord": [
        "Supply_ID", "Site_name", "MPAN_MPRN_Meter_name", "Fuel_Type",
        "Metered_supply_service_charge", "Date_From", "Date_To", "kWh", "Cost_inc_VAT",
        "Cost_exc_VAT", "Cost_Unit", "Floor_area", "Floor_area_Unit",
        "Reason_for_Confidence", "Error_Code", "Error_Field", "Error_Value",
        "Error_Message", "FileId",
    ],
    # transport
    "Staging_Transport": [
        "Site_Name_or_Reference", "Transport_Category_Emission_Source", "Vehicle", "Fuel",
        "Measurement_Unit_Type_1", "Units_1", "Measurement_Unit_1",
        "Measurement_Unit_Type_2", "Units_2", "Measurement_Unit_2",
        "Measurement_Unit_Type_3", "Units_3", "Measurement_Unit_3",
        "Measurement_Unit_Type_4", "Units_4", "Measurement_Unit_4", "Vehicle_Class",
        "Date_From", "Date_To", "Non_UK_Data", "Subsidiary_Data", "Job_Type",
        "Vehicle_Registration", "Normalization_Confidence", "Reason_for_Confidence",
        "Error_Code", "Error_Field", "Error_Value", "Error_Message", "FileId",
    ],
    # other_fuels
    "Staging_Other_Fuels": [
        "Site_Name_or_Reference", "Emission_Source", "Fuel", "Measurement_Unit_Type",
        "Date_From", "Date_To", "Units", "Measurement_Unit", "Non_UK_Data",
        "Subsidiary_Data", "Job_Type", "Normalization_Confidence", "Reason_for_Confidence",
        "Error_Code", "Error_Field", "Error_Value", "Error_Message", "FileId",
    ],
    # metrics
    "Staging_Metrics": [
        "Company_Level_Metric_Type", "Units", "Date_From", "Date_To", "Measurement_Unit",
        "Normalization_Confidence", "Reason_for_Confidence", "Error_Code", "Error_Field",
        "Error_Value", "Error_Message", "FileId",
    ],
    # Energy_Efficiency_Narrative
    "Staging_Energy_Efficiency_Narrative": [
        "Reporting_Year", "Project_Title", "Brief_Description",
        "Impact_on_Business_Operations", "Initiative_Type", "Normalization_Confidence",
        "Reason_for_Confidence", "Error_Code", "Error_Field", "Error_Value",
        "Error_Message", "FileId",
    ],
    # Invoice
    "Staging_Invoices": [
        "Name", ("Address1", "Address_1"), ("Address2", "Address_2"), "Town",
        ("PostCode", "Post_Code"), "Utility", ("MeasurementUnit", "Measurement_Unit"),
        ("ReferenceNumber", "Reference_Number"), "Supplier",
        ("MPANMPRWaterSPID", "M1_MPAN_MPR_Water_SPID"), ("TopLine", "Top_Line"),
        ("EstimatedInvoice", "Estimated_Invoice"), "Units", "Cost", "VAT", "CCL",
        ("InvoiceDate", "Date"), ("PreviousInvoiceDate", "Previous_Date"),
        ("InvoiceTaxDate", "Invoice_Tax_Date"), ("InvoiceType", "Invoice_Type"),
        "Normalization_Confidence", "Reason_for_Confidence", "Error_Code", "Error_Field",
        "Error_Value", "Error_Message", "FileId",
    ],
    # half_hourly_data
    "Staging_HHData": [
        ("MPANCore1", "M1_MPAN_MPR_Water_SPID"), "Utility", "Date", "HH_0030", "HH_0100",
        "HH_0130", "HH_0200", "HH_0230", "HH_0300", "HH_0330", "HH_0400", "HH_0430",
        "HH_0500", "HH_0530", "HH_0600", "HH_0630", "HH_0700", "HH_0730", "HH_0800",
        "HH_0830", "HH_0900", "HH_0930", "HH_1000", "HH_1030", "HH_1100", "HH_1130",
        "HH_1200", "HH_1230", "HH_1300", "HH_1330", "HH_1400", "HH_1430", "HH_1500",
        "HH_1530", "HH_1600", "HH_1630", "HH_1700", "HH_1730", "HH_1800", "HH_1830",
        "HH_1900", "HH_1930", "HH_2000", "HH_2030", "HH_2100", "HH_2130", "HH_2200",
        "HH_2230", "HH_2300", "HH_2330", "HH_0000", "Normalization_Confidence",
        "Reason_for_Confidence", "Error_Code", "Error_Field", "Error_Value",
        "Error_Message", "FileId",
    ],
}


# ----------------------------------------------------------------------------------
# Step: Clean CSV and add a column FileID
# (fill_csv_blanks is the Python function embedded in the bot, kept unchanged)
# ----------------------------------------------------------------------------------

def fill_csv_blanks(input_dict: dict) -> str:
    """
    Fill blank values in a CSV file with "NA" string.

    Args:
        input_dict (dict): Input parameters containing:
            - input_file (str): Path to the input CSV file
            - output_file (str, optional): Path to save the output CSV. Defaults to adding '_filled' suffix
            - fill_value (str, optional): Value to fill blanks with. Defaults to "NA"
            - newline_replacement (str, optional): Text used to replace newline characters in cells. Defaults to a space
            - remove_quotes (bool, optional): Remove literal double quote characters from cell values. Defaults to True

    Returns:
        str: Result as a JSON string with status and statistics
    """
    try:
        input_file = input_dict.get("input_file")
        output_file = input_dict.get("output_file")
        fill_value = input_dict.get("fill_value", "NA")
        newline_replacement = input_dict.get("newline_replacement", " ")
        remove_quotes = input_dict.get("remove_quotes", True)
        file_id = input_dict.get("file_id")

        if not input_file:
            raise ValueError("input_file parameter is required")

        input_path = Path(input_file)
        if not input_path.exists():
            raise FileNotFoundError(f"File not found: {input_file}")

        if not output_file:
            output_file = str(input_path.stem) + "_filled.csv"

        rows_processed = 0
        blanks_filled = 0
        newlines_cleaned = 0
        quotes_cleaned = 0

        with open(input_file, 'r', encoding='utf-8-sig', newline='') as infile:
            reader = csv.reader(infile, strict=True)
            rows = list(reader)

            for row in rows:
                for i, cell in enumerate(row):
                    normalized_cell = re.sub(
                        r'[^\S\r\n]*\r\n|[^\S\r\n]*\r|[^\S\r\n]*\n',
                        lambda match: newline_replacement,
                        cell
                    )
                    if normalized_cell != cell:
                        newlines_cleaned += len(re.findall(r'\r\n|\r|\n', cell))
                        row[i] = normalized_cell
                    if remove_quotes and '"' in row[i]:
                        quote_count = row[i].count('"')
                        row[i] = row[i].replace('"', '')
                        quotes_cleaned += quote_count
                    if row[i].strip() == "":
                        row[i] = fill_value
                        blanks_filled += 1
                rows_processed += 1

            if rows:
                rows[0].append("FileId")
                for row in rows[1:]:
                    row.append(file_id)

        with open(output_file, 'w', encoding='utf-8', newline='') as outfile:
            writer = csv.writer(outfile)
            writer.writerows(rows)

        return json.dumps({
            "status": "success",
            "result": {
                "output_file": output_file,
                "rows_processed": rows_processed,
                "blanks_filled": blanks_filled,
                "fill_value": fill_value,
                "newlines_cleaned": newlines_cleaned,
                "quotes_cleaned": quotes_cleaned
            }
        })

    except Exception as e:
        return json.dumps({
            "status": "error",
            "error": str(e)
        })


def get_category(csv_path: str) -> str:
    """Text before the first '_ESG' in the file name, e.g.
    'Energy_Efficiency_Narrative_ESG_Normalized_Data_20261104_120032.csv'
    -> 'Energy_Efficiency_Narrative'. Empty if '_ESG' is not found (case-sensitive,
    as in the bot)."""
    name = Path(csv_path).name
    idx = name.find("_ESG")
    return name[:idx].strip() if idx >= 0 else ""


# ----------------------------------------------------------------------------------
# Database
# ----------------------------------------------------------------------------------

def connect(host: str, port: int, database: str, user: str, password: str):
    import mysql.connector  # imported here so --dry-run works without the driver

    return mysql.connector.connect(
        host=host, port=port, database=database, user=user, password=password,
        autocommit=False, connection_timeout=60,
    )


def batch_insert(conn, database: str, table: str, csv_path: str,
                 mappings: list[str | tuple[str, str]]) -> int:
    """Insert the cleaned CSV (header on row 1, data from row 2) into the table."""
    pairs = [(m, m) if isinstance(m, str) else m for m in mappings]
    csv_cols = [c for c, _ in pairs]
    table_cols = ", ".join(f"`{t}`" for _, t in pairs)
    placeholders = ", ".join(["%s"] * len(pairs))
    sql = f"INSERT INTO `{database}`.`{table}` ({table_cols}) VALUES ({placeholders})"

    inserted = 0
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        missing = [c for c in csv_cols if c not in (reader.fieldnames or [])]
        if missing:
            raise BotException(f"CSV {csv_path} is missing mapped column(s): {missing}")

        cur = conn.cursor()
        try:
            batch = []
            for row in reader:
                batch.append(tuple(row[c] for c in csv_cols))
                if len(batch) >= BATCH_SIZE:
                    cur.executemany(sql, batch)
                    inserted += len(batch)
                    batch.clear()
            if batch:
                cur.executemany(sql, batch)
                inserted += len(batch)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
    return inserted


def call_proc(conn, database: str, proc: ProcCall, values: dict[str, str]) -> dict[str, str]:
    """Call a stored procedure with VARCHAR IN params and return its OUT params."""
    args = [values[p] for p in proc.inputs] + [None] * len(proc.outputs)
    cur = conn.cursor()
    try:
        # Plain name: the connection's default database is already `database`, and
        # mysql-connector derives OUT-param session variable names from this string.
        result = cur.callproc(proc.name, args)
        # Consume any result sets the procedure returns so the connection stays usable.
        for _ in cur.stored_results():
            pass
        conn.commit()
    finally:
        cur.close()
    out_values = result[len(proc.inputs):]
    return {name: ("" if v is None else str(v)) for name, v in zip(proc.outputs, out_values)}


# ----------------------------------------------------------------------------------
# Error handler (replaces the "Reusable/Framework/ErrorHandler" bot)
# ----------------------------------------------------------------------------------

def notify_error(config: dict, message: str) -> None:
    """The bot runs Reusable/Framework/ErrorHandler here (screenshot + email
    notification). That bot was not in the export, so this only logs the error.
    Add your email or alerting call here."""
    log.error("ErrorHandler notification: %s", message)


# ----------------------------------------------------------------------------------
# Main flow
# ----------------------------------------------------------------------------------

def run(csv_path: str, input_folder: str, file_id: str, report_id: str,
        config: dict | None = None, dry_run: bool = False) -> dict:
    config = config or {}
    process_cfg = config.get("ProcessConfig", {}) or {}
    ref = f"File Id: {file_id}"

    outputs = {"sBotStatus": "", "sBotComments": "", "sErrorMessage": "",
               "nErrorLineNumber": 0, "sErrorType": "", "nInsertedRecords": 0}
    conn = None
    sp_message = ""

    try:
        # ---- INITIAL ASSIGNMENTS / config --------------------------------------
        host = os.environ.get("ESG_DB_HOST") or process_cfg.get("ESGDBServerName", "")
        database = os.environ.get("ESG_DB_NAME") or process_cfg.get("DatabaseName", "")
        port = int(os.environ.get("ESG_DB_PORT") or process_cfg.get("MySQL_Port") or 3306)
        log.debug("Variables are initialized")

        # ---- Clean CSV and add a column FileID ---------------------------------
        file_name = Path(csv_path).name
        category = get_category(csv_path)
        log.debug("Category detected from file name '%s': '%s' (%s)", file_name, category, ref)
        if category == "":
            log.error("Could not determine the category from file name %s (%s)", file_name, ref)
            raise BotException(f"Could not determine the category from file name {file_name}")

        cleaned_path = str(Path(input_folder) / f"Cleaned_{category}.csv")
        log.debug("Cleaning CSV %s and adding File Id (%s)", csv_path, ref)
        result = json.loads(fill_csv_blanks({
            "input_file": csv_path,
            "output_file": cleaned_path,
            "fill_value": "NA",
            "file_id": file_id,
        }))
        if result.get("status", "").lower() != "success":
            msg = result.get("error", "")
            log.error("Python execution failed with error message %s (%s)", msg, ref)
            raise BotException(f"Python execution failed with error message {msg}")

        rows_processed = int(result["result"]["rows_processed"])
        log.debug("CSV cleaned. Rows processed: %s (%s)", rows_processed, ref)
        log.debug("Cleaned the file and stored at %s (%s)", cleaned_path, ref)

        if rows_processed <= 1:  # header row only
            msg = (f"CSV {csv_path} has no data rows (rows processed: {rows_processed}). "
                   "Stopping the task without loading")
            log.info(msg)
            outputs.update(sBotStatus=STATUS_NO_DATA, sBotComments=msg)
            return outputs

        # ---- IMPLEMENTATION ----------------------------------------------------
        cat = CATEGORIES.get(category.lower())
        if cat is None:
            log.error("Unrecognized Category %s (%s)", category, ref)
            raise BotException(f"Unrecognized Category {category}")
        mappings = COLUMN_MAPPINGS[cat.table]

        if dry_run:
            log.info("[dry-run] Would insert %s into %s.%s with %d mapped columns, then call %s",
                     cleaned_path, database or "<db>", cat.table, len(mappings),
                     ", ".join(p.name for p in cat.procs))
            outputs.update(sBotStatus="Dry Run",
                           sBotComments=f"Cleaned file at {cleaned_path}; nothing loaded")
            return outputs

        user = os.environ.get("ESG_DB_USER")
        password = os.environ.get("ESG_DB_PASSWORD")
        if not (host and database and user and password):
            raise BotException("Database settings missing: need host and database name "
                               "(config or ESG_DB_HOST/ESG_DB_NAME) and ESG_DB_USER/"
                               "ESG_DB_PASSWORD environment variables")

        conn = connect(host, port, database, user, password)
        log.debug("Connected to %s successful and starting to bulk insert (%s)", database, ref)

        inserted = batch_insert(conn, database, cat.table, cleaned_path, mappings)
        outputs["nInsertedRecords"] = inserted
        log.debug("Inserted %d record(s) into the staging table for %s (%s)", inserted, category, ref)

        values = {"p_FileId": str(file_id), "p_ReportId": str(report_id),
                  "p_CreatedBy": getpass.getuser()}
        for proc in cat.procs:
            out = call_proc(conn, database, proc, values)
            sp_message = out.get("p_Message", "")
            log.debug("Stored procedure %s finished. Message: %s (%s)", proc.name, sp_message, ref)
            if proc.success_text.lower() not in sp_message.lower():
                log.error("Error in %s SP: %s (%s)", proc.name, sp_message, ref)
                raise BotException(f"Error in {proc.name} SP: {sp_message}")

        log.debug("Ran Bulk insert for %s and Load SP Message: %s (%s)", category, sp_message, ref)

        # ---- CLEANUP & FINALIZE ------------------------------------------------
        log.info("Load Data SP Message %s", sp_message)
        outputs["sBotStatus"] = STATUS_SUCCESS
        log.info("Data load completed for %s. Bot status: %s (%s)", category, STATUS_SUCCESS, ref)
        return outputs

    except Exception as e:
        tb = traceback.extract_tb(e.__traceback__)
        line_no = tb[-1].lineno if tb else 0
        error_message = str(e)
        outputs["nErrorLineNumber"] = line_no
        outputs["sBotStatus"] = outputs["sErrorType"] or STATUS_TECHNICAL_EXCEPTION
        # The bot doubles single quotes (for later use in SQL); kept for compatibility.
        outputs["sErrorMessage"] = error_message.replace("'", " ''")
        outputs["sBotComments"] = outputs["sErrorMessage"]
        log.error("Error Message: %s\nError Line Number: %s (%s)", error_message, line_no, ref)
        if not isinstance(e, BotException):
            log.debug("Traceback:\n%s", traceback.format_exc())
        notify_error(config, error_message)
        return outputs

    finally:
        log.debug("Finally block: DB connected: %s (%s)", conn is not None, ref)
        if conn is not None:
            try:
                conn.close()
            except Exception:  # pragma: no cover
                log.warning("Could not close the database connection cleanly")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Clean an ESG normalized CSV and load it into MySQL.")
    p.add_argument("--csv", required=True, help="Path to the normalized CSV (ioCSVFilePath)")
    p.add_argument("--input-folder", required=True,
                   help="Folder for the cleaned CSV (ioInputFolderPath)")
    p.add_argument("--file-id", required=True, help="ioFileId")
    p.add_argument("--report-id", required=True, help="ioReportId")
    p.add_argument("--config", help="JSON file with the bot's dConfig (uses ProcessConfig keys)")
    p.add_argument("--dry-run", action="store_true",
                   help="Clean the CSV and validate the mapping, but do not touch the database")
    p.add_argument("-v", "--verbose", action="store_true", help="Show TRACE-level logging")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(message)s")

    config = {}
    if args.config:
        config = json.loads(Path(args.config).read_text(encoding="utf-8"))

    outputs = run(args.csv, args.input_folder, args.file_id, args.report_id,
                  config=config, dry_run=args.dry_run)
    print(json.dumps(outputs, indent=2))
    return 0 if outputs["sBotStatus"] in (STATUS_SUCCESS, STATUS_NO_DATA, "Dry Run") else 1


if __name__ == "__main__":
    sys.exit(main())
