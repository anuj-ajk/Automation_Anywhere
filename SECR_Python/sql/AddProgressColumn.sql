-- =====================================================================================================
-- UploadedFile.ProgressPercent: how far the SECR Python process is with a file (0-100)  (run ONCE; safe to
-- run again)
--
-- An indicative number for a progress bar, not a measurement. It only goes up during a run and is 100 when
-- the file is finished, whatever the outcome (check ProcessingStatusId for COMPLETED / FAILED / ...).
-- NULL = the file has not been picked up since the column was added.
--
-- Uploaded files (Normalize flow):
--     0  picked up                        30  sheets submitted to the AI agent
--     5  downloading from S3              30-70  rises as each sheet's AI answer arrives
--    10  downloaded                       75  normalized CSVs downloaded
--    20  split into sheets                75-99  rises as each CSV is loaded (staging + procedures)
--                                        100  finished
-- Daisy files (Energy Manager / NAV):
--     0  picked up    10  source system queried    60  data exported    100  loaded / no data / failed
--
-- The Python code detects the column and works without it.
-- =====================================================================================================

SET @column_exists = (
    SELECT COUNT(*) FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'UploadedFile' AND COLUMN_NAME = 'ProgressPercent'
);

SET @ddl = IF(@column_exists = 0,
    'ALTER TABLE UploadedFile ADD COLUMN ProgressPercent TINYINT UNSIGNED NULL',
    'SELECT ''UploadedFile.ProgressPercent already exists'' AS info');

PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Progress of a batch (average of its files):
--   SELECT BatchId, ROUND(AVG(COALESCE(ProgressPercent, 0))) AS BatchProgressPercent
--   FROM UploadedFile WHERE BatchId = 52 AND IsDeleted = 0 AND IsValid = 1 GROUP BY BatchId;

-- Rollback:
--   ALTER TABLE UploadedFile DROP COLUMN ProgressPercent;
