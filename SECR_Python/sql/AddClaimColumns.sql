-- =====================================================================================================
-- Claim tracking on FileUploadBatch (run ONCE, before installing sql/ClaimBatches.sql)
--
--   ClaimedAt              when the batch was claimed; the owner refreshes it while the batch runs
--                          (heartbeat). An IN_PROGRESS batch whose ClaimedAt is too old was left behind
--                          by a process that died (machine restart, bot stopped) and can be released.
--   ClaimedBy              who owns the claim: "<host>:<pid>:<run id>"
--   ClaimPreviousStatusId  the status the batch had before the claim, restored when it is released
--
-- All three are NULL when the batch is not claimed. The SECR Python code detects the columns at start-up
-- and works without them (no stale-claim recovery then).
-- =====================================================================================================

ALTER TABLE FileUploadBatch
    ADD COLUMN ClaimedAt             DATETIME     NULL,
    ADD COLUMN ClaimedBy             VARCHAR(100) NULL,
    ADD COLUMN ClaimPreviousStatusId INT          NULL;

-- Finds stale claims quickly (status + age).
CREATE INDEX IX_FileUploadBatch_Claim ON FileUploadBatch (BatchStatusId, ClaimedAt);

-- Rollback:
--   DROP INDEX IX_FileUploadBatch_Claim ON FileUploadBatch;
--   ALTER TABLE FileUploadBatch DROP COLUMN ClaimedAt, DROP COLUMN ClaimedBy, DROP COLUMN ClaimPreviousStatusId;
