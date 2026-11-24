-- =====================================================================================================
-- UploadedFile.SheetResults: per-sheet outcome of the Normalize flow  (run ONCE; safe to run again)
--
-- The SECR Python process writes one JSON document per uploaded file: every sheet that came out of the
-- workbook split, whether it was normalized and loaded, and for a failed sheet the step and the reason.
-- On a rerun (file not COMPLETED, same FileVersion) only the sheets that are not COMPLETED are sent to the
-- AI agent and loaded again; completed sheets are skipped. force = true ignores it and redoes every sheet.
--
-- Example value:
-- {
--   "file_version": "1",
--   "updated_at": "2026-11-24T12:11:14",
--   "sheets": [
--     {"sheet": "PORTFOLIO", "file": "Sheet_PORTFOLIO.xlsx", "status": "COMPLETED", "stage": "", "reason": "",
--      "attempts": 1, "updated_at": "2026-11-24T12:11:12",
--      "categories": [{"category": "portfolio", "status": "COMPLETED", "rows": 9}]},
--     {"sheet": "TRANSPORT", "file": "Sheet_TRANSPORT.xlsx", "status": "FAILED", "stage": "download",
--      "reason": "400 Client Error: Bad Request for url: https://api.getodin.ai/.../Transport.csv",
--      "attempts": 2, "updated_at": "2026-11-24T12:11:13",
--      "categories": [{"category": "transport", "status": "FAILED", "stage": "download", "reason": "..."}]}
--   ]
-- }
--   status = COMPLETED | FAILED
--   stage  = split | submit | ai | download | load   (where a FAILED sheet / category stopped)
--
-- The Python code detects the column and works without it (every sheet is then processed on a rerun).
-- =====================================================================================================

SET @column_exists = (
    SELECT COUNT(*) FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'UploadedFile' AND COLUMN_NAME = 'SheetResults'
);

SET @ddl = IF(@column_exists = 0,
    'ALTER TABLE UploadedFile ADD COLUMN SheetResults JSON NULL',
    'SELECT ''UploadedFile.SheetResults already exists'' AS info');

PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Useful queries (MySQL 8):
--   Every sheet of a batch with its status and, for failures, the step and reason:
--     SELECT UF.FileId, UF.FileName, s.sheet, s.status, s.stage, s.reason, s.attempts
--     FROM UploadedFile UF,
--          JSON_TABLE(UF.SheetResults, '$.sheets[*]' COLUMNS (
--              sheet    VARCHAR(255)  PATH '$.sheet',
--              status   VARCHAR(20)   PATH '$.status',
--              stage    VARCHAR(20)   PATH '$.stage',
--              reason   VARCHAR(2000) PATH '$.reason',
--              attempts INT           PATH '$.attempts')) s
--     WHERE UF.BatchId = 52
--     ORDER BY UF.FileId, s.status, s.sheet;
--
--   Process every sheet of a file again on the next run:
--     UPDATE UploadedFile SET SheetResults = NULL WHERE FileId = 77;

-- Rollback:
--   ALTER TABLE UploadedFile DROP COLUMN SheetResults;
