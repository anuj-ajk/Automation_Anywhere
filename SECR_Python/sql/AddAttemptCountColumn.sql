-- =====================================================================================================
-- Retry limit on FileUploadBatch (run ONCE)
--
--   AttemptCount  how many SECR runs have started processing this batch's files. The first run makes it 1,
--                 every retry (the batch was PARTIAL_FAILURE, or a run failed / died) adds 1.
--                 With run.max_batch_retries = 3 a batch gets at most 4 runs (1 + 3 retries). When the
--                 last one does not complete, the batch is set to run.batch_status_retries_exhausted
--                 (FAILED) and pending mode no longer takes it.
--
-- Rerunning a batch on purpose with force / --force sets AttemptCount back to 0 first.
-- The SECR Python code detects the column at start-up and works without it (no retry limit then).
--
-- Prerequisite: the exhausted status exists in RefBatchStatus, e.g.
--     SELECT * FROM RefBatchStatus WHERE Code = 'FAILED';
-- If it does not, add it (columns as in your RefBatchStatus), or set run.batch_status_retries_exhausted to an
-- existing code that is NOT in run.claimable_statuses. Without it, a batch over the limit is still skipped
-- (nothing processed) but stays PARTIAL_FAILURE, and an error is logged on every run.
-- =====================================================================================================

ALTER TABLE FileUploadBatch
    ADD COLUMN AttemptCount INT NOT NULL DEFAULT 0;

-- Rollback:
--   ALTER TABLE FileUploadBatch DROP COLUMN AttemptCount;
