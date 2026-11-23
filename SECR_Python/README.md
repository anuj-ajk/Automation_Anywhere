# SECR Normalizer – Python port

Python version of the Automation Anywhere process **MT Normalizer Uploaded Data**
(`AAPackage/.../Streamlined Energy & Carbon Reporting/Tasks`), including
`MainTask_Normalize_RawData`, `MainTask_Retrive_Data_Daisy_EM` and all their sub tasks.
It processes **up to 10 batches at the same time**, and each batch can have any number of files.

## Setup

```powershell
cd SECR_Python
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy config.example.yaml config.yaml   # then edit config.yaml
```

You also need the Microsoft ODBC Driver for SQL Server for the Energy Manager / NAV connections.

**Work folder:** set `run.work_dir` to a short **local** folder, for example `%TEMP%\SECR_work` or `C:\SECR_work`.
Environment variables are expanded.

* A work folder on a network share (`\\server\...`) is slower.
* It also pushes downloaded files over the Windows 260-character path limit, which shows up as
  `[Errno 2] No such file or directory` during the S3 download.
* Local file names are already shortened (`b<batch>_<time>\f<file>\src\<first 40 characters of the name>.xlsx`).
  A path that is still too long fails with a clear "over the Windows limit" message.
* The log warns when `work_dir` is on a network share.

### Secrets (environment variables only)

| Variable | Replaces (AA Credential Vault) |
|---|---|
| `SECR_DB_USER`, `SECR_DB_PASSWORD` | ESG Locker / ESGDB MySQL |
| `SECR_S3_BUCKET`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | ESG Locker / AWS_S3 (region is in `config.yaml`) |
| `SECR_ODIN_API_KEY`, `SECR_ODIN_API_SECRET` | ESG Locker / AA ESG EKB Credential |
| `SECR_EM_CONNECTION_STRING` | global value `@sql_JupiterConnection` (ODBC string) |
| `SECR_NAV_CONNECTION_STRING` | ESG Locker / NAV_ESGDB_ConnectionString (ODBC string) |

## Running

```powershell
python -m secr --config config.yaml run --batch-ids 40 41 42 43
python -m secr --config config.yaml run --batch-file batches.txt
python -m secr --config config.yaml run --pending            # uses run.pending_batches_query
python -m secr --config config.yaml run --batch-ids 40 --max-batches 1 --max-files 1 --debug
python -m secr --config config.yaml run --batch-ids 40 --force       # also redo files already COMPLETED
```

Exit code: `0` all files OK, `2` some files failed (batch still completed), `1` at least one batch failed.
A summary table is printed and written to the log file in `run.log_dir`.

## Calling it from Automation Anywhere

Use **`aa_entry.py`**, function **`run_batches`**, with a Dictionary argument.

| AA action | Settings |
|---|---|
| Python script: **Open** | Python file = `D:\Automation_Anywhere\SECR_Python\aa_entry.py`, session `SECR`, Python version 3 |
| Dictionary: **Put** (×n) | build `dPythonArgs` (keys below) |
| Python script: **Execute function** | session `SECR`, function `run_batches`, argument `$dPythonArgs$`, output to `sPythonOutput` |
| Python script: **Close** | session `SECR` |
| JSON: Start session / Get node value | read `$.status`, `$.message`, `$.batches[*]` from `sPythonOutput` |

Dictionary keys (all values may be text):

| Key | Required | Example | Meaning |
|---|---|---|---|
| `batch_ids` | yes | `40,41,42` | batch ids, separated by comma, semicolon or space (a list works too) |
| `config_path` | no | `D:\Automation_Anywhere\SECR_Python\config.yaml` | default: `config.yaml` next to `aa_entry.py` |
| `max_batches` | no | `10` | batches processed at the same time |
| `max_files` | no | `4` | files of one batch processed at the same time |
| `debug` | no | `true` | verbose log |
| `force` | no | `true` | process every file again, also those already `COMPLETED` (see "Running a batch again") |
| `secrets` | no | dictionary | `SECR_*` / `AWS_*` values, if the bot takes them from the Credential Vault instead of the machine's environment variables |

The function never throws; it always returns JSON:

```json
{"status": "PARTIAL", "exit_code": 2, "message": "<summary table>", "log_file": "...\\logs\\SECR_....log",
 "batches": [{"batch_id": "40", "state": "COMPLETED_WITH_FILE_ERRORS", "files": 3, "files_ok": 2,
              "seconds": 512.4, "error": "",
              "file_results": [{"file_id": "77", "file_name": "data.xlsx", "flow": "normalize",
                                "status": "FAILED", "detail": "..."}]}]}
```

`status` is `SUCCESS` (every file OK), `PARTIAL` (some files failed, batches completed) or `FAILED`
(a batch failed, or bad input / config — see `message`). In the bot, throw an error when
`status = FAILED` so the work item is marked failed.

Notes:
* The bot runner needs Python 3.10+ with `requirements.txt` installed **in the interpreter AA uses**.
* The call blocks until every batch is finished (AI polling can take ~10 min per file), so give the
  AA work item / bot a long enough timeout.
* Run one `run_batches` call per bot run; it already processes the batches in parallel.

## Running a batch again

* **Only unfinished files are processed.** When a batch runs again, files already in `COMPLETED` or
  `DATA_NOT_AVAILABLE` are skipped. They still show in the result with `detail = "skipped: already COMPLETED"`.
  All other files are processed again:
  * `FAILED`, `PARTIAL_FAILURE` and `FAILED_WHILE_NORMALIZATION`;
  * files left in `PARSING` / `NORMALIZING` by a run that stopped;
  * new files.

  If every file is already done, the batch state is `ALREADY_COMPLETED` and nothing is loaded.
  Use `--force` (CLI) or `force: true` (AA) to process every file again.
* **No duplicate staging rows.** Before a file is loaded into a staging table, the rows that an earlier run
  left in that table for the same `FileId` are deleted. This happens once per file and table in a run, so
  two sheets of one file that load into the same table do not delete each other's rows.
* **Settings** (all optional), under `run:` in `config.yaml`:
  * `skip_completed_files: true`
  * `completed_statuses: [COMPLETED, DATA_NOT_AVAILABLE]`
  * `clear_staging_before_load: true`
* **Batch status:**
  * every file OK → `COMPLETE`;
  * some files failed → `PARTIAL_FAILURE` (setting `batch_status_on_file_errors`).

  If `PARTIAL_FAILURE` is not in `RefBatchStatus`, the batch gets `COMPLETE` and a warning is logged.
  Set `batch_status_on_file_errors: COMPLETE` to keep the bot's behaviour. A batch status code is always looked
  up first, so an unknown code never sets `BatchStatusId` to NULL.
* **Note on `PARTIAL_FAILURE`:** the whole file is processed again, including the sheets that already loaded.
  Whether this creates duplicates in the final tables depends on the `Load*` procedures (whether they replace
  or append the file's rows).

## Logging

Each run writes one log file to `run.log_dir` (its path is in the AA output as `log_file`), and the same lines go to the console.
Every line shows the run, the thread, the module and the batch/file it belongs to:

```
2026-11-23 16:29:24,525 INFO    [run ad5f69933743] [b41-file_1] [secr.normalize] [batch 41][file 3] Step 1/4 download from S3 - started
```

* **Filter one batch or one file:** search for `[batch 41]` or `[file 3]`. This also finds the DB, S3, Odin and CSV lines for that file.
* **Find slow steps:** each step logs `- started` and `- done in Ns` (or `- FAILED after Ns: error`). The steps are: read batch, Normalize steps 1–4, Daisy export/load, each stored procedure and each bulk insert.
* **Watch progress:** the batch pool logs `Progress: x/y batch(es) done`. While the AI is working, a `Still waiting for ...` line is logged every 10 polls.
* **Settings and secrets:** the effective settings are logged at the start of the run. Secrets are listed only as `set` / `NOT SET`, never with their values. Signed download links are logged without their query string.
* **Debug level** (`--debug` / `debug: true`) adds:
  * every SQL statement, with its time and row count;
  * every Odin HTTP call and every poll;
  * DB locks and connection use;
  * CSV headers and clean-up counts.

  SQL that takes more than 5 s is logged as a warning at any level.

## How the parallel processing works

```
run_batches  ── ThreadPoolExecutor(max_parallel_batches = 10)
   └─ BatchProcessor.process(batch)          one thread per running batch
        ├─ MySQL GET_LOCK('secr_batch_<id>')  same batch never runs twice (any process/machine)
        ├─ read all files of the batch        0..N rows, uploaded + 'Source: Daisy' mixed
        └─ ThreadPoolExecutor(max_parallel_files_per_batch = 4)
              ├─ NormalizeFlow.run_file   S3 → split → Odin submit+poll → download CSV → load
              │    └─ ThreadPoolExecutor(odin.max_parallel_sheets = 5)   per file
              │         submit all sheets │ poll all chats each round │ download all CSVs
              │         (then the CSVs are loaded one after the other)
              └─ DaisyFlow.run_file       Energy Manager / NAV → CSV → load

All Odin calls and CSV downloads share one cap: odin.max_parallel_requests = 20
```

* **Sheets in a file:** the sheets of one file are submitted to the AI in parallel (`odin.max_parallel_sheets`).
  Each poll round checks every waiting chat at once and then waits 40 s, so 6 sheets take about as long as 1.
  The normalized CSVs are downloaded in parallel and then loaded in order, because the staging lock and the
  "clear earlier rows" step need the loads to run in order.
* **Bulk insert:** a file with at least `parallel_insert_min_rows` (20,000) rows per connection is split over up to
  `insert_connections` (4) connections that insert at the same time.
  * Example: 115,269 HH rows go over 4 connections instead of 115 sequential chunks on one.
  * Extra connections are taken only if they are free at that moment. A worker never waits for more connections,
    so workers cannot deadlock each other. If none are free, it inserts on one connection as before.
* **Stored procedures:** the staging lock now covers only the clean-up, the insert and the `Load*` procedure.
  * Report-level procedures (`UpdatePortfolioDerivedFields`, `CalculateInvoiceData`) do not read the staging
    table. They run after the staging lock is released, under a lock per report and procedure
    (`secr_report_<id>_<proc>`).
  * So another batch can load `Staging_Invoices` while `CalculateInvoiceData` (about 2 minutes) runs.
  * One procedure call itself cannot be split; it runs as fast as MySQL runs it.
* **API cap:** `odin.max_parallel_requests` limits the Odin calls and CSV downloads in flight across **all**
  files and batches. Lower it if Odin returns HTTP 429 (rate limit).

* **Batches**: at most `max_parallel_batches` (default 10) run at once; when one finishes the next starts.
* **Files in a batch**: any number. They are queued into the batch's own pool, so a batch with 1 file
  and a batch with 50 files both work; at most `max_parallel_files_per_batch` of them run at once.
* **Per-file pipeline**: the bot ran each stage for every file before the next stage and polled the AI
  one chat at a time (up to ~10 min each). Here each file moves through all stages on its own, and all
  sheets of a file are polled together, so one slow AI answer no longer blocks the rest of the batch.
* **Normalize and Daisy** files of the same batch run side by side (the bot ran them one after the other).
* **Shared staging tables**: `Staging_*` tables and the `Load*` procedures are shared by all batches, so
  by default only one load per staging table runs at a time (MySQL named lock, works across processes).
  If the procedures only touch the rows of their own `FileId`, set `serialize_staging_loads: false`
  for more speed.
* **Limits**: total threads ≈ `batches × files_per_batch` (40 by default); DB connections are capped by
  `max_db_connections`. Lower `max_parallel_files_per_batch` if the Odin API rate-limits you.

## Module map (bot → Python)

| AA bot | Python |
|---|---|
| MT Normalizer Uploaded Data | `secr/__main__.py`, `orchestrator.py`, `batch.py` |
| MainTask_Normalize_RawData | `normalize.py` |
| AWS S3 File Management | `storage.py` |
| ST_SplitSheetsAndCreateIndividualExcel | `excel_split.py` |
| ST_GetAIResponse_ParallelExecution | `ai_client.py` + `NormalizeFlow._submit/_poll` |
| inline `download_file` script | `downloader.py` |
| ST_InsertDataIntoDB, `fill_csv_blanks` | `loader.py`, `csv_clean.py` |
| MainTask_Retrive_Data_Daisy_EM | `daisy.py` |
| ST_Retrieve_and_Load_{Portfolio,Invoices,HH}Data | `source_systems.py` + `loader.py` (`DAISY_SPECS`) |
| Framework ErrorHandler / SendEmailNotification | `notifier.py` + logging |

Status codes written to `UploadedFile` and `FileUploadBatch` are the same as in the bots
(`PARSING`, `PARSED`, `NORMALIZING`, `NORMALIZATION_COMPLETED`, `FAILED_WHILE_NORMALIZATION`,
`PARTIAL_FAILURE`, `COMPLETED`, `FAILED`, `DATA_NOT_AVAILABLE`, batch `COMPLETE`).

## Behaviour changes compared with the bot

| Area | Bot | Python |
|---|---|---|
| Empty batch / batch with only one kind of file | `[0]` row read without a check → main task fails | Handled; empty batch is reported as `NO_FILES` |
| Daisy errors | `throw` disabled → master reports *Completed* | File is `FAILED` and the batch is reported `COMPLETED_WITH_FILE_ERRORS` (exit code 2) |
| Data source `Energy Manager` vs `EnergyManager` | UAT/PROD never loaded the query | Matched ignoring spaces and case |
| Invoices in PROD | read `PROD_PORTFOLIO_EM_QUERY.txt` | reads `PROD_INVOICE_EM_QUERY.txt` (file must be created) |
| Unknown Daisy category | silently skipped | file set to `FAILED` |
| Batch `COMPLETE` | set before the Daisy sync ran | set after all files of the batch finished |
| Batch with failed files | `COMPLETE` | `PARTIAL_FAILURE` if that code exists in `RefBatchStatus`, else `COMPLETE` |
| SQL | values pasted into SQL, quotes escaped by hand | parameterised queries everywhere |
| AI answered without any CSV link | counted as success | counted as failed for that sheet |
| A sheet failed in AI but the rest loaded | final status `COMPLETED` | final status `PARTIAL_FAILURE` |
| CSV missing a mapped column | AA bulk insert fails | column loaded as NULL, warning logged |
| Category of a normalized CSV | parsed from the file name | taken from the Odin response key |
| Framework steps (kill apps, archive, folder upload/download) | AA framework bots | not needed: each batch gets its own temp folder that is deleted afterwards |

## Not verified yet

The tests in `tests/` run the whole pipeline with fake DB / S3 / Odin / source systems
(`python -m pytest tests`). They prove the parallel scheduling, the per-file status flow and the CSV
and Excel code, but **nothing has been run against the real ESG DB, S3, Odin, Energy Manager or NAV**.
Before production, run a few known batches with `--max-batches 1 --debug` in DEV and check:

* the stored procedures' OUT parameters come back as expected through PyMySQL `callproc`;
* the Odin `json_response` shape and the CSV link download;
* the UAT/PROD query files in `Docs/Config` (they are currently empty) and `PROD_INVOICE_EM_QUERY.txt`;
* whether the `Load*` procedures are safe to run concurrently (then disable `serialize_staging_loads`).
