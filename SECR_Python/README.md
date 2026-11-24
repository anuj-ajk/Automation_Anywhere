# SECR Normalizer – Python port

Python version of the Automation Anywhere process **MT Normalizer Uploaded Data**
(`AAPackage/.../Streamlined Energy & Carbon Reporting/Tasks`), including
`MainTask_Normalize_RawData`, `MainTask_Retrive_Data_Daisy_EM` and all their sub tasks.
It processes **up to 10 batches at the same time**, and each batch can have any number of files.

For the architecture, data flow, status model, concurrency design and risks, see
**`docs/SECR_Python_Design_Document.docx`** (the older design document of the AA bots is `../SECR_Normalizer_Design.md`).

**What a batch is:** a user uploads files for an SECR report; they are stored as one `FileUploadBatch` with one
`UploadedFile` row per file. Two kinds of file rows exist and both are handled:

* **Uploaded files** (Excel / CSV in S3) → split by sheet → normalized by the Odin AI agent into one CSV per
  category → bulk-loaded into `Staging_*` tables → `Load*` stored procedures (`normalize.py`).
* **`Source: Daisy` rows** (no file) → data for the client and report period is pulled from Energy Manager or NAV →
  CSV → same staging load (`daisy.py`).

## Setup

```powershell
cd SECR_Python
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
# config.yaml is in the folder already: edit it for your environment (it never holds secrets)
```

You also need the Microsoft ODBC Driver for SQL Server for the Energy Manager / NAV connections.

Tests (no real DB / S3 / Odin needed, 51 tests): `python -m pytest tests`

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

Where the values come from:

* **Machine / process environment variables** always work (CLI and AA).
* **`.env` file** next to `aa_entry.py` (git-ignored): it is loaded by `aa_entry.py` only (`python-dotenv`), **not** by
  `python -m secr`. For the command line, set the variables in the shell first. Values already set in the
  environment are not overwritten by `.env`.
* **AA `secrets` dictionary** (see below): values passed in are written to the process environment for that call.
  Only keys starting with `SECR_` or `AWS_` are accepted.

Only the sections of `config.yaml` that you need must be complete: the Normalize flow needs S3 + Odin settings,
the Daisy flow needs the source settings. If one of them cannot be built, it is disabled with a warning and the
other still works (a batch that contains files for the disabled flow then fails).

## Running

```powershell
python -m secr --config config.yaml run --batch-ids 40 41 42 43
python -m secr --config config.yaml run --batch-file batches.txt
python -m secr --config config.yaml run --pending            # uses run.pending_batches_query
python -m secr --config config.yaml run --batch-ids 40 --max-batches 1 --max-files 1 --debug
python -m secr --config config.yaml run --batch-ids 40 --force       # also redo files already COMPLETED
python -m secr --config config.yaml run --batch-ids 40 --work-dir C:\SECR_work --log-dir D:\logs
```

Options of `run`: `--batch-ids` / `--batch-file` / `--pending` (one of them is required), `--max-batches`,
`--max-files`, `--work-dir`, `--log-dir`, `--force`; global: `--config`, `--debug`.

Exit code: `0` all files OK, `2` some files failed (batch still completed), `1` at least one batch failed.
A summary table is printed and written to the log file in `run.log_dir`.

Batch states in the summary / AA output:

| State | Meaning |
|---|---|
| `SUCCESS` | every file finished `COMPLETED` or `DATA_NOT_AVAILABLE` |
| `COMPLETED_WITH_FILE_ERRORS` | batch ran to the end, at least one file failed (exit code 2) |
| `FAILED` | the batch itself crashed (exit code 1); its files still in progress are set to failed |
| `NO_FILES` | no valid SECR file rows for the batch (not an error; status put back if the batch was claimed) |
| `ALREADY_COMPLETED` | rerun and every file was already done; nothing loaded, batch set `COMPLETE` |
| `SKIPPED` | another process holds the batch (DB lock) or it is `IN_PROGRESS` elsewhere |
| `LOCKED` | its `FileUploadBatch` row is locked by another DB session (MySQL 1205, e.g. an uncommitted Workbench transaction): skipped completely, nothing read or loaded, status unchanged (exit code 2). `COMMIT`/`ROLLBACK` that session and run again |

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
| `work_dir` | no | `%TEMP%\SECR_work` | temp folder for this call (overrides `run.work_dir`). `%VARS%` are expanded; a relative path is taken from the config file's folder |
| `log_dir` | no | `D:\...\SECR_Python\logs` | log folder for this call (overrides `run.log_dir`) |
| `pending` | no | `true` | process the batches returned by `run.pending_batches_query` (then `batch_ids` is optional; ids given there are processed too). `batch_ids = pending` does the same. No pending batch → `status SUCCESS`, message "No pending batches" |
| `pending_limit` | no | `20` | take at most this many batches in total in one call |
| `poll_seconds` | no | `60` | pending mode: look for new batches at least this often while batches run |
| `max_runtime_minutes` | no | `120` | pending mode: stop taking new batches after this many minutes (running ones finish) |
| `force` | no | `true` | process every file again, also those already `COMPLETED` (see "Running a batch again"), and take a batch even if it is `IN_PROGRESS` |
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

## Pending mode: claiming batches and refilling free slots

Start it with `pending: true` from AA, or `run --pending` on the command line.

1. **Free slots are refilled.** Up to `max_parallel_batches` (10) batches run at once. When a batch finishes, and at
   least every `pending_poll_seconds` (60) while batches run, new pending batches are looked for. For example,
   with 2 running, up to 8 more are taken.
   * The run ends when nothing is running and nothing is pending.
   * It also stops taking new batches after `pending_max_runtime_minutes` (AA: `max_runtime_minutes`) or
     `pending_limit` batches in total. The batches already running always finish.
   * A batch that has already run in this call is not taken again in the same call.
2. **Each batch is claimed atomically before it starts.** The claim is a compare-and-set:
   ```sql
   UPDATE FileUploadBatch SET BatchStatusId = <IN_PROGRESS>
   WHERE BatchId = ? AND BatchStatusId = <the status just read>
   ```
   Only one session can change the row. A second system, or a second bot, gets 0 rows and moves on to the next
   batch. The status the batch had is kept and put back if the batch fails or has no files.
3. **Stored procedures** (`sql/ClaimBatches.sql`, not installed automatically):
   * `ClaimPendingBatches(limit, codes, in_progress_code, exclude_ids, claimed_by)` claims up to `limit`
     batches and returns them as a result set (`BatchId`, `PreviousCode`). SECR calls it when
     `claim_procedure: ClaimPendingBatches` is set, so all claims happen in one database call.
   * `ClaimBatch(batch_id, codes, in_progress_code, claimed_by, OUT previous_code, OUT claimed)` claims one
     specific batch. SECR calls it when `claim_batch_procedure: ClaimBatch` is set (see "Explicit batch ids"
     below). In pending mode it is used when `claim_procedure` is empty: the batches found by
     `pending_batches_query` are then claimed one by one through it. Other systems that take one batch at a
     time can call it too.
   * `ReleaseStaleClaims(in_progress_code, stale_minutes, fallback_code)` (see "Stale claims" below). SECR calls
     it when `release_stale_procedure: ReleaseStaleClaims` is set; otherwise Python runs the same release.

   `codes` and `exclude_ids` are comma-separated lists, for example `'PROCESSING,PARTIAL_FAILURE'` and `'52,53'`.
   `claimed_by` is written to `ClaimedBy`, for example `'host:1234:abc'`. The procedures write the claim
   columns, so run `sql/AddClaimColumns.sql` before installing them.

   Other systems that pick up batches should use these procedures, so they cannot take a batch SECR has already
   claimed. With neither `claim_procedure` nor `claim_batch_procedure`, Python runs the same compare-and-set
   itself.

**Explicit batch ids** (`batch_ids` / `--batch-ids`), with `claim_batch_procedure: ClaimBatch` set:

* A batch in one of the `claimable_statuses` is claimed through `ClaimBatch`, like in pending mode.
* A batch that is already `IN_PROGRESS` belongs to another system or run. It is skipped (`SKIPPED`) and its
  status is left alone. Stale claims are released at start-up first, so a batch left behind by a process that
  died is not blocked. With `force` (`--force` / `force: true`) the batch is taken anyway; if it then fails,
  it goes back to the first `claimable_statuses` entry.
* Any other status (for example `COMPLETE` when a batch is rerun on purpose) is not claimable. The batch is set
  to `IN_PROGRESS` directly and processed, as before.
* If the procedure call itself fails (for example it is not installed), the error is logged and the status is
  set directly.

Without `claim_batch_procedure`, explicit batch ids are set to `IN_PROGRESS` whatever their status.

**Requirements:**

* `IN_PROGRESS` must exist in `RefBatchStatus`. Without it, batches cannot be claimed, and only the per-batch DB
  lock protects them (that lock works between SECR processes only).
* `claimable_statuses` must match the statuses in `pending_batches_query`.

### Stale claims (`ClaimedAt` / `ClaimedBy`)

If the process is killed (the machine restarts, or the bot is stopped), its batches would stay in `IN_PROGRESS`
for ever. Run `sql/AddClaimColumns.sql` once, then `sql/ClaimBatches.sql`, and this is handled:

* **On claim:** `ClaimedAt = NOW()` and `ClaimedBy = <host>:<pid>:<run id>` are written, and
  `ClaimPreviousStatusId` keeps the status to go back to.
* **While the batch runs:** a heartbeat refreshes `ClaimedAt` every `claim_heartbeat_seconds` (60). A long batch
  (AI, `CalculateInvoiceData`) therefore never looks stale.
* **When the batch ends or is put back:** the three columns are emptied.
* **Before claiming new batches,** pending mode releases stale ones. These are `IN_PROGRESS` batches whose
  `ClaimedAt` is older than `stale_claim_minutes` (30).
  * They go back to `ClaimPreviousStatusId`, or to the first `claimable_statuses` entry if that is unknown.
  * The age is checked again in the same `UPDATE`, so a heartbeat that arrives at that moment keeps the batch.
  * Each release is logged as a warning, with the old owner.
  * With `release_stale_procedure: ReleaseStaleClaims` this is done by one `CALL ReleaseStaleClaims('IN_PROGRESS',
    30, 'PROCESSING')`; with `""` Python runs the same statements. Stale claims are released at the start of
    every run and, in pending mode, before each claim.

The code checks for the columns at start-up. Without them it works as before, and logs a warning that stale
claims cannot be detected. Keep `stale_claim_minutes` well above the heartbeat interval: at least 3 heartbeats
must be missed before a batch counts as stale.

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
* **Batch picked up:** as soon as a batch is taken (after its `secr_batch_<id>` lock), it is set to
  `IN_PROGRESS` (setting `batch_status_in_progress`), so it no longer looks pending.
  * If that code is not in `RefBatchStatus`, the status is left unchanged and the log lists the codes that exist.
  * If the batch then fails, or has no files, its previous status (e.g. `PROCESSING`) is put back, so it is picked
    up again.
* **Batch status:**
  * every file OK → `COMPLETE`;
  * some files failed → `PARTIAL_FAILURE` (setting `batch_status_on_file_errors`).

  If `PARTIAL_FAILURE` is not in `RefBatchStatus`, the batch gets `COMPLETE` and a warning is logged.
  Set `batch_status_on_file_errors: COMPLETE` to keep the bot's behaviour. A batch status code is always looked
  up first, so an unknown code never sets `BatchStatusId` to NULL.
* **Only the failed sheets of a file are retried** (needs `sql/AddSheetResultsColumn.sql`). After every
  Normalize run, `UploadedFile.SheetResults` holds a JSON list of the file's sheets: `COMPLETED`, or `FAILED`
  with the `stage` (`split`, `submit`, `ai`, `download`, `load`) and the `reason`, plus the number of attempts
  and the result per category. When the file runs again (it is not `COMPLETED`, same `FileVersion`):
  * sheets that are `COMPLETED` are not split, sent to the AI agent or loaded again;
  * inside a retried sheet, a category that already loaded is not loaded twice;
  * staging tables that completed sheets of this file loaded are **not** cleared before the retry, so their
    rows stay. (If the failed sheet itself had inserted rows into such a table before its procedure failed,
    those rows stay too; the log says "Earlier rows ... are kept".)
  * the file becomes `COMPLETED` once every sheet is `COMPLETED`.

  Every sheet is processed again when: `force` is set; the `FileVersion` changed (a new upload); the column is
  empty or does not exist (then the log warns once and the old behaviour applies). `SET SheetResults = NULL` for
  a file to redo all of its sheets; `sql/ResetBatchesForTest.sql` does this. Signed tokens (`?token=...`) are
  removed from the stored reasons and from `StatusDescription`. A sheet that could not be split now also keeps
  the file out of `COMPLETED`, so a rerun retries it.

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
        └─ ThreadPoolExecutor(max_parallel_files_per_batch)   code default 4, config.yaml sets 5
              ├─ NormalizeFlow.run_file   S3 → split → Odin submit+poll → download CSV → load
              │    └─ ThreadPoolExecutor(odin.max_parallel_sheets)   per file; code default 5, config.yaml sets 6
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
* **Limits**: total threads ≈ `batches × files_per_batch` (40 by default in code, 50 with the shipped
  `config.yaml`). DB connections have two limits, and both make a worker wait rather than fail:
  - **This process**: at most `max_db_connections` (20) open at once, so other systems keep room in the
    database. It must be at least `batches + 1` (checked at start-up; `aa_entry`/CLI overrides raise it to
    that minimum if needed). Past it, a worker waits until one of our connections closes ("All 20 DB
    connections allowed for this process are in use; waiting for a free one").
  - **The database**: when MySQL refuses a new connection (1040 "Too many connections", or a per-user limit
    1203 / 1226), the worker tries again every `connect_retry_seconds`, and at once when one of ours closes,
    until the database accepts it ("ESG DB refused a new connection …; waiting for the database to have room").

  Extra bulk-insert connections and heartbeats never wait: they are skipped if either limit is reached. The
  DB's `max_connections` and current connection count are logged at start-up.
  Lower `max_parallel_files_per_batch` if the Odin API rate-limits you.

## Configuration reference (`config.yaml`)

| Section | Key | Default (code) | Meaning |
|---|---|---|---|
| `run` | `max_parallel_batches` / `max_parallel_files_per_batch` | 10 / 4 | batches at once / files of one batch at once |
| `run` | `max_db_connections` | 20 | max connections this process opens (min `max_parallel_batches + 1`; past it workers wait) |
| `run` | `work_dir`, `log_dir` | `work`, `logs` | local temp folder (deleted per batch) and log folder; `%VARS%` expanded, relative = from the config file's folder |
| `run` | `keep_work_files` | false | keep downloaded / split / CSV files for debugging |
| `run` | `created_by` | OS user | passed to `LoadHHData` / `CalculateInvoiceData` |
| `run` | `serialize_staging_loads`, `staging_lock_timeout_seconds` | true, 900 | one load per staging table at a time (MySQL `GET_LOCK`) |
| `run` | `insert_chunk_size`, `insert_connections`, `parallel_insert_min_rows` | 1000, 4, 20000 | bulk insert tuning |
| `run` | `skip_completed_files`, `completed_statuses`, `clear_staging_before_load` | true, `[COMPLETED, DATA_NOT_AVAILABLE]`, true | rerun behaviour |
| `run` | `batch_status_in_progress`, `batch_status_on_file_errors` | `IN_PROGRESS`, `PARTIAL_FAILURE` | batch status codes written to `RefBatchStatus` |
| `run` | `claimable_statuses`, `pending_batches_query` | `[PROCESSING, PARTIAL_FAILURE]`, none | which batches pending mode takes |
| `run` | `claim_procedure`, `claim_batch_procedure`, `release_stale_procedure` | empty | stored procedure names; empty = Python runs the same SQL (the shipped `config.yaml` sets all three) |
| `run` | `pending_poll_seconds`, `pending_max_runtime_minutes` | 60, 0 | pending-mode polling / stop taking new batches (0 = no limit) |
| `run` | `claim_heartbeat_seconds`, `stale_claim_minutes` | 60, 30 | claim tracking (0 minutes = never release) |
| `esg_db` | `host`, `port`, `database`, `connect_timeout` | –, 3306, `esgdb`, 30 | user / password come from the environment |
| `esg_db` | `connect_retry_seconds` | 5 | DB full (too many connections): wait and retry this often until it accepts |
| `s3` | `region` | `eu-west-2` | bucket from `SECR_S3_BUCKET` |
| `s3` | `download_attempts`, `retry_backoff_seconds` | 3, 5 | retries network errors / throttling / 5xx; waits 5s, 10s, ... (missing key / access denied fail at once) |
| `odin` | `base_uri`, `project_id`, `agent_id`, `message` | – | Odin REST API and agent |
| `odin` | `submit_timeout_seconds`, `request_timeout_seconds` | 300, 120 | upload timeout (a timeout counts as accepted) / other calls |
| `odin` | `poll_interval_seconds`, `max_poll_attempts` | 40, 15 | about 10 minutes per file |
| `odin` | `max_parallel_sheets`, `max_parallel_requests` | 5, 20 | sheets of one file at once / Odin calls + CSV downloads in flight overall |
| `odin` | `submit_attempts`, `submit_retry_backoff_seconds` | 3, 10 | create chat + upload retried in a **new** chat on connection errors, timeouts, HTTP 408/429/5xx; other 4xx fail at once |
| `sources` | `environment`, `query_dir`, `query_timeout_seconds` | `DEV`, `.`, 600 | `<ENV>_<TYPE>_EM_QUERY.txt` is read from `query_dir` |
| `notify` | `enabled`, `smtp_host`, `smtp_port`, `use_tls`, `from_address`, `to_addresses`, `cc_addresses` | disabled | start / end / batch-failure e-mails through an SMTP relay (no login); a failed mail never fails a batch |

## Module map (bot → Python)

| AA bot | Python |
|---|---|
| MT Normalizer Uploaded Data | `secr/__main__.py`, `orchestrator.py`, `batch.py`, `runner.py` (wiring), `aa_entry.py` |
| MainTask_Normalize_RawData | `normalize.py` |
| AWS S3 File Management | `storage.py` |
| ST_SplitSheetsAndCreateIndividualExcel | `excel_split.py` |
| ST_GetAIResponse_ParallelExecution | `ai_client.py` + `NormalizeFlow._submit/_poll` |
| inline `download_file` script | `downloader.py` |
| ST_InsertDataIntoDB, `fill_csv_blanks` | `loader.py`, `csv_clean.py` |
| MainTask_Retrive_Data_Daisy_EM | `daisy.py` |
| ST_Retrieve_and_Load_{Portfolio,Invoices,HH}Data | `source_systems.py` + `loader.py` (`DAISY_SPECS`) |
| Framework ErrorHandler / SendEmailNotification | `notifier.py` + logging |
| ReadMainConfig / ReadUserConfig / Credential Vault | `config.py` (YAML + environment variables) |
| (DB access inside every bot) | `db.py` (PyMySQL sessions, locks, claims), `models.py`, `status.py`, `paths.py`, `logging_setup.py` |

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
| AI returns `NA` for a category | — | only real `http(s)` links are used. `NA` / `N/A` / `null` / empty values are left out: not downloaded, not counted as failures, not added to `CategoryRecognized` |
| A sheet failed in AI but the rest loaded | final status `COMPLETED` | final status `PARTIAL_FAILURE` |
| CSV missing a mapped column | AA bulk insert fails | column loaded as NULL, warning logged |
| Category of a normalized CSV | parsed from the file name | taken from the Odin response key |
| Framework steps (kill apps, archive, folder upload/download) | AA framework bots | not needed: each batch gets its own temp folder that is deleted afterwards |

## Verification status

The tests in `tests/` (`python -m pytest tests`, 51 tests) run the whole pipeline with fake DB / S3 / Odin /
source systems. They prove the parallel scheduling, the per-file status flow, claiming, and the CSV and Excel code.

The logs in `logs/` show **runs against the real DEV environment** on 2026-11-23: batches 40 and 52 (S3 download,
Odin normalization, loads of portfolio / landlord / transport / other fuels, NAV invoice export with
`LoadInvoiceData` + `CalculateInvoiceData`), plus reruns, an empty batch (13) and an already-completed batch.
Some of those runs ended with file errors (state `COMPLETED_WITH_FILE_ERRORS`); read the logs before relying on
a category. Still to confirm in DEV / UAT before production:

* Energy Manager extraction and Half Hourly (`LoadHHData`) loads, and large files over the parallel insert;
* several batches at the same time, and pending mode with the installed `ClaimBatches.sql` procedures;
* the UAT / PROD query files: only `queries/DEV_*_EM_QUERY.txt` exist. `UAT_*` / `PROD_*` files (including
  `PROD_INVOICE_EM_QUERY.txt`) must be created before running with `sources.environment` set to UAT / PROD;
* whether the `Load*` procedures are safe to run concurrently (then set `serialize_staging_loads: false`).
