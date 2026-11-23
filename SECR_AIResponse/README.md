# SECR AI Response – Python port of `ST_GetAIResponse_ParallelExecution`

This is a standalone replacement for the AA sub task **ST_GetAIResponse_ParallelExecution**
(`AAPackage/.../Streamlined Energy & Carbon Reporting/Tasks/SubTasks`).

It sends every split sheet to the Odin AI agent, waits for the normalized CSV links, and writes the
`UploadedFile` status and `CategoryRecognized` columns the same way the bot does.

It does not need the `SECR_Python` folder. You can call it from the existing `MainTask_Normalize_RawData`
in place of the `runTask` of the sub task.

## Why it is faster

| Bot | Python |
|---|---|
| Creates a chat and sends the file one sheet after another | Up to `max_parallel` (10) sheets are submitted at the same time |
| Polls **one** chat until it answers (15 × 40 s), then moves to the next chat | Each round polls **all** waiting chats in parallel, then waits 40 s |
| 5 sheets × up to 10 min each ≈ up to 50 min | 5 sheets ≈ up to 10 min in total |

## Setup

```powershell
cd SECR_AIResponse
pip install -r requirements.txt
```

Install the requirements into the Python interpreter that Automation Anywhere uses (Python 3.10 or newer).

Secrets come from environment variables, or from the `secrets` dictionary in the AA call:

| Variable | Replaces (AA Credential Vault) |
|---|---|
| `SECR_ODIN_API_KEY`, `SECR_ODIN_API_SECRET` | ESG Locker / AA ESG EKB Credential |
| `SECR_DB_USER`, `SECR_DB_PASSWORD` | ESG Locker / ESGDB MySQL (only needed when `update_db` is true) |

## Calling it from Automation Anywhere

In `MainTask_Normalize_RawData`, replace the `runTask` of `ST_GetAIResponse_ParallelExecution` with these actions:

| AA action | Settings |
|---|---|
| Data table: **Write to file** | `tSplittedFileTable` → e.g. `<work>\sheets.csv` (columns `FileID,SplittedFilePath`) |
| Python script: **Open** | Python file `D:\Automation_Anywhere\SECR_AIResponse\aa_entry.py`, session `AIResp` |
| Dictionary: **Put** | Build `dAIArgs` (keys below). You can copy the `dProcessConfig` keys into it as they are |
| Python script: **Execute function** | Session `AIResp`, function `get_ai_responses`, argument `$dAIArgs$`, output `sAIOutput` |
| Python script: **Close** | Session `AIResp` |
| CSV/TXT: **Read** | `<work>\links.csv` → `tNormalizedCSVFileLinkTable` |
| JSON: read `$.status` | If it is `FAILED`, throw an error or log it |

### Dictionary keys

All values may be text.

| Key | Required | Meaning |
|---|---|---|
| `input_csv` **or** `files` | yes | The split sheets. `input_csv` is a CSV with `FileID,SplittedFilePath`. `files` is a JSON array `[{"FileID":"77","SplittedFilePath":"C:\\...xlsx"}]` or text `77\|C:\a.xlsx;78\|C:\b.xlsx` |
| `output_csv` | no | CSV written with `FileID,NormalizedCSVLink,FileName,Category`. The first three columns are the bot's `tNormalizedCSVFileLinkTable` |
| `Agent_Project_ID` / `project_id` | yes | From `dProcessConfig` |
| `Agent_ID` / `agent_id` | yes | From `dProcessConfig` |
| `ENV_Base_URI` / `base_uri` | no | Default `https://api.getodin.ai` |
| `ESGDBServerName` / `db_host` | when `update_db` is true | From `dProcessConfig` |
| `DatabaseName` / `db_name` | no | Default `esgdb` |
| `MySQL_Port` / `db_port` | no | Default `3306` |
| `update_db` | no | Default `true`. Writes `NORMALIZING`, then the final status and `CategoryRecognized`, like the bot. `false` only logs them |
| `max_parallel` | no | Sheets handled at the same time (default `10`) |
| `poll_interval_seconds` | no | Default `40` |
| `max_poll_attempts` | no | Default `15` |
| `debug` | no | `true` gives a detailed log |
| `log_dir` | no | Default `SECR_AIResponse\logs` |
| `secrets` | no | Dictionary with the four variables above, if the bot reads them from the Credential Vault |

### Output

The function never throws an error into AA. It always returns JSON:

```json
{"status": "PARTIAL", "message": "5 sheet(s), 6 CSV link(s); 1 file(s) OK, 1 partial, 0 failed",
 "log_file": "...\\logs\\AIResponse_....log", "output_csv": "...\\links.csv", "seconds": 412.3,
 "links": [{"FileID": "77", "NormalizedCSVLink": "https://...", "FileName": "Sheet_Data.xlsx", "Category": "portfolio"}],
 "files": [{"file_id": "77", "status": "NORMALIZATION_COMPLETED", "sheets": 2, "success": 3, "failed": 0, "description": ""}]}
```

`status` is one of:

* `SUCCESS`: every file is `NORMALIZATION_COMPLETED`.
* `PARTIAL`: some sheets failed.
* `FAILED`: every file failed, or the input or settings were bad. `message` and `trace` say why.

## Command line

```powershell
python -m ai_response --input-csv sheets.csv --output-csv links.csv --project-id <id> --agent-id <id> --db-host <server>
python -m ai_response --input-csv sheets.csv --project-id <id> --agent-id <id> --no-db --debug
```

Exit code: `0` all files OK, `2` some sheets failed, `1` everything failed or the input was bad.

## Status rules

The status rules are the same as in the bot, per FileID:

* `NORMALIZING` is set as soon as one sheet of the file has been submitted.
* Categories the agent returns as `NA` (also `N/A`, `null`, `none` or empty, in any case) are not links. They are
  left out of the JSON `links`, out of `output_csv` and out of `CategoryRecognized`, and they do not count as
  successes. If all of a sheet's categories are `NA`, the sheet counts as "answered without any CSV link".
* Each CSV link the agent returns counts as one success. Each sheet that failed counts as one failure. A sheet fails when:
  * the chat could not be created or the message could not be sent;
  * the agent did not answer in time (time-out);
  * the agent answered without any CSV link.
* The final status is:
  * no failures → `NORMALIZATION_COMPLETED`;
  * no successes → `FAILED_WHILE_NORMALIZATION`;
  * otherwise → `PARTIAL_FAILURE`.

  The error texts are appended to `StatusDescription`.
* Each recognized category is added to `CategoryRecognized` (Portfolio, Landlord, Transport, Other Fuels, Metrics,
  Energy Efficiency Narrative, Invoice, Half Hourly Data).

## Differences from the bot

| Area | Bot | Python |
|---|---|---|
| Speed | Sheets and polls are sequential | Parallel, see above |
| Agent answered without a CSV link | Counted as neither success nor failure | Counted as a failed sheet |
| HTTP error while polling | The sheet fails at once | Logged, and the poll is retried in the next round |
| Time-out | Calls the ErrorHandler bot (screenshot and e-mail) | Logged as an error. The time-out is in the file's `StatusDescription` and in the JSON |
| SQL | Values pasted into the SQL, quotes escaped by hand | Parameterised queries |
| Secrets | Plain text in the disabled unit-test step | Environment variables only |

## Logging

There is one log file per call, in `log_dir`. Every line shows the run, thread, module and `[file <FileID>][<sheet>]`:

```
2026-11-23 18:20:04,964 INFO [run 0608cfd52341] [poll_2] [ai_response.processor] [file 77][Sheet_Data.xlsx] Answered after poll 3: portfolio, metrics
```

Each step (submit, poll, results) logs when it starts and how long it took. Each poll round lists the sheets that are still waiting.
Secrets are logged only as `set` / `NOT SET`. With `debug` on, every HTTP call and SQL update is also logged.

## Tests

```powershell
python -m pytest tests
```

The tests use a fake Odin client and a fake DB. They cover:

* the status rules;
* parallel submit and polling;
* the different input forms;
* the output CSV and JSON;
* the logging.

**Not verified yet:** nothing has been run against the real Odin API or ESG DB. Before you use it in production, run it once in DEV with
`update_db=false` and `debug=true`.
