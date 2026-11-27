-- =====================================================================================================
-- Reset batches (and their files) so the SECR Python process can run them again - for TESTING only.
--
-- After a run, FileUploadBatch is COMPLETE / PARTIAL_FAILURE / IN_PROGRESS and its UploadedFile rows are
-- COMPLETED / FAILED / DATA_NOT_AVAILABLE ..., so a second run would skip them. This script puts them back
-- to the state they had before the first run:
--   FileUploadBatch  BatchStatusId      -> @batch_status (PROCESSING: picked up by pending mode again)
--                    ClaimedAt/By/Prev  -> NULL (only if sql/AddClaimColumns.sql was run, see step 3)
--                    AttemptCount       -> 0    (only if sql/AddAttemptCountColumn.sql was run, see step 3)
--   UploadedFile     ProcessingStatusId -> @file_status, StatusDescription / CategoryRecognized -> NULL
--   Staging_* rows   for these files    -> deleted when @clear_staging = 1
--                    (the Python run also deletes them before each load, this just leaves the tables clean)
--
-- Usage: set @batch_ids and run the WHOLE script (it ends with COMMIT).
--   The changes run in one transaction that the script commits itself. Do not stop it half way: an
--   uncommitted transaction keeps these rows locked, and every SECR run then fails its status updates with
--   "Lock wait timeout exceeded" (1205). If a run stopped early, execute ROLLBACK; in the same tab, or close it.
--   To try it without changing anything, replace the COMMIT at the end with ROLLBACK.
-- =====================================================================================================

SET @batch_ids     = '40,41';        -- comma separated BatchIds, no spaces
SET @batch_status  = 'PROCESSING';   -- RefBatchStatus.Code to put the batches back to
SET @file_status   = 'PARSING';      -- RefFileProcessingStatus.Code to put the files back to
SET @clear_staging = 1;              -- 1 = delete the files' rows from the Staging_* tables, 0 = keep them

SET @saved_safe_updates = @@SQL_SAFE_UPDATES;
SET SQL_SAFE_UPDATES = 0;            -- MySQL Workbench safe mode refuses FIND_IN_SET in a WHERE

START TRANSACTION;

-- 0. Unknown status codes would set the ids to NULL: stop here if either SELECT returns no row.
SELECT BatchStatusId          AS BatchStatusId FROM RefBatchStatus          WHERE Code = @batch_status;
SELECT FileProcessingStatusId AS FileStatusId  FROM RefFileProcessingStatus WHERE Code = @file_status;

SET @batch_status_id = (SELECT BatchStatusId          FROM RefBatchStatus          WHERE Code = @batch_status);
SET @file_status_id  = (SELECT FileProcessingStatusId FROM RefFileProcessingStatus WHERE Code = @file_status);

-- 1. Staging rows written by the previous run
DELETE FROM Staging_Portfolio                  WHERE @clear_staging = 1 AND FileId IN (SELECT FileId FROM UploadedFile WHERE FIND_IN_SET(BatchId, @batch_ids));
DELETE FROM Staging_Landlord                   WHERE @clear_staging = 1 AND FileId IN (SELECT FileId FROM UploadedFile WHERE FIND_IN_SET(BatchId, @batch_ids));
DELETE FROM Staging_Transport                  WHERE @clear_staging = 1 AND FileId IN (SELECT FileId FROM UploadedFile WHERE FIND_IN_SET(BatchId, @batch_ids));
DELETE FROM Staging_Other_Fuels                WHERE @clear_staging = 1 AND FileId IN (SELECT FileId FROM UploadedFile WHERE FIND_IN_SET(BatchId, @batch_ids));
DELETE FROM Staging_Metrics                    WHERE @clear_staging = 1 AND FileId IN (SELECT FileId FROM UploadedFile WHERE FIND_IN_SET(BatchId, @batch_ids));
DELETE FROM Staging_Energy_Efficiency_Narrative WHERE @clear_staging = 1 AND FileId IN (SELECT FileId FROM UploadedFile WHERE FIND_IN_SET(BatchId, @batch_ids));
DELETE FROM Staging_Invoices                   WHERE @clear_staging = 1 AND FileId IN (SELECT FileId FROM UploadedFile WHERE FIND_IN_SET(BatchId, @batch_ids));
DELETE FROM Staging_HHData                     WHERE @clear_staging = 1 AND FileId IN (SELECT FileId FROM UploadedFile WHERE FIND_IN_SET(BatchId, @batch_ids));

-- 2. Files back to their starting status
UPDATE UploadedFile
SET    ProcessingStatusId = @file_status_id,
       StatusDescription  = NULL,
       CategoryRecognized = NULL
WHERE  FIND_IN_SET(BatchId, @batch_ids)
  AND  @file_status_id IS NOT NULL;

-- 3. Batches back to pending
UPDATE FileUploadBatch
SET    BatchStatusId = @batch_status_id
WHERE  FIND_IN_SET(BatchId, @batch_ids)
  AND  @batch_status_id IS NOT NULL;

-- Only if sql/AddSheetResultsColumn.sql was run (otherwise remove it): forget which sheets completed, so the
-- next run processes every sheet again instead of only the failed ones.
UPDATE UploadedFile
SET    SheetResults = NULL
WHERE  FIND_IN_SET(BatchId, @batch_ids);

-- Only if sql/AddProgressColumn.sql was run (otherwise remove it): no progress until the next run picks it up.
UPDATE UploadedFile
SET    ProgressPercent = NULL
WHERE  FIND_IN_SET(BatchId, @batch_ids);

-- Only if sql/AddClaimColumns.sql was run (otherwise this statement fails: remove it, nothing else changes):
UPDATE FileUploadBatch
SET    ClaimedAt = NULL, ClaimedBy = NULL, ClaimPreviousStatusId = NULL
WHERE  FIND_IN_SET(BatchId, @batch_ids);

-- Only if sql/AddAttemptCountColumn.sql was run (otherwise remove it): the batch gets all its retries again.
UPDATE FileUploadBatch
SET    AttemptCount = 0
WHERE  FIND_IN_SET(BatchId, @batch_ids);

-- 4. Commit: releases the row locks, so the SECR process can update these batches and files again
COMMIT;

SET SQL_SAFE_UPDATES = @saved_safe_updates;

-- 5. Result (committed)
SELECT FB.BatchId, B.Code AS BatchStatus, UF.FileId, UF.FileName, PS.Code AS FileStatus
FROM   FileUploadBatch FB
LEFT JOIN RefBatchStatus B           ON B.BatchStatusId = FB.BatchStatusId
LEFT JOIN UploadedFile UF            ON UF.BatchId = FB.BatchId
LEFT JOIN RefFileProcessingStatus PS ON PS.FileProcessingStatusId = UF.ProcessingStatusId
WHERE  FIND_IN_SET(FB.BatchId, @batch_ids)
ORDER BY FB.BatchId, UF.FileId;

-- Still getting "Lock wait timeout exceeded" in the SECR log? Look for a transaction left open:
--   SELECT trx_id, trx_started, trx_mysql_thread_id, trx_rows_locked FROM information_schema.innodb_trx;
--   KILL <trx_mysql_thread_id>;   -- ends (rolls back) that session's open transaction
