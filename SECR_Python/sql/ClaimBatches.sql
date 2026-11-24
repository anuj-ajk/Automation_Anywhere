-- =====================================================================================================
-- Atomic batch claiming for the SECR normalizer (and any other system that picks up FileUploadBatch rows)
--
-- Run sql/AddClaimColumns.sql first (ClaimedAt, ClaimedBy, ClaimPreviousStatusId).
--
-- Why: reading "pending" batches and then setting them to IN_PROGRESS are two steps. Two systems that
-- read at the same moment could both take the same batch. These procedures make the hand-over atomic with
-- a compare-and-set:
--
--     UPDATE FileUploadBatch SET BatchStatusId = <IN_PROGRESS>, ClaimedAt = NOW(), ClaimedBy = <owner>, ...
--     WHERE BatchId = <id> AND BatchStatusId = <the status we just read>
--
-- MySQL applies one UPDATE per row at a time, so only ONE session can change the row away from the
-- status it read; every other session gets ROW_COUNT() = 0 and simply moves on to the next batch.
-- No SKIP LOCKED needed, so this works on MySQL 5.7 and 8.x.
--
-- Stale claims: the owner refreshes ClaimedAt while the batch runs (heartbeat). ReleaseStaleClaims puts
-- IN_PROGRESS batches whose ClaimedAt is older than N minutes back to their previous status, so a batch
-- left behind by a process that died is picked up again.
--
-- Every system that takes batches should claim them through these procedures (or the same UPDATE),
-- otherwise it can still take a batch another system already owns.
--
-- Prerequisite: the in-progress code exists in RefBatchStatus, e.g.
--     INSERT INTO RefBatchStatus (Code, ...) VALUES ('IN_PROGRESS', ...);
-- =====================================================================================================

DROP PROCEDURE IF EXISTS ClaimBatch;
DELIMITER $$
-- Claim ONE batch. p_Claimed = 1 when this caller now owns it (status set to p_InProgressCode),
-- p_PreviousCode = the status it had, to put back if the batch cannot be processed.
CREATE PROCEDURE ClaimBatch(
    IN  p_BatchId         INT,
    IN  p_ClaimableCodes  VARCHAR(500),   -- comma list, e.g. 'PROCESSING,PARTIAL_FAILURE'
    IN  p_InProgressCode  VARCHAR(50),    -- e.g. 'IN_PROGRESS'
    IN  p_ClaimedBy       VARCHAR(100),   -- owner, e.g. 'host:pid:run'
    OUT p_PreviousCode    VARCHAR(50),
    OUT p_Claimed         TINYINT
)
BEGIN
    DECLARE v_InProgressId INT DEFAULT NULL;
    DECLARE v_CurrentId    INT DEFAULT NULL;

    SET p_Claimed = 0, p_PreviousCode = NULL;

    SELECT BatchStatusId INTO v_InProgressId FROM RefBatchStatus WHERE Code = p_InProgressCode LIMIT 1;
    IF v_InProgressId IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'ClaimBatch: in-progress status code not found in RefBatchStatus';
    END IF;

    SELECT FB.BatchStatusId, B.Code INTO v_CurrentId, p_PreviousCode
    FROM FileUploadBatch FB
    LEFT JOIN RefBatchStatus B ON B.BatchStatusId = FB.BatchStatusId
    WHERE FB.BatchId = p_BatchId;

    IF p_PreviousCode IS NOT NULL AND FIND_IN_SET(p_PreviousCode, p_ClaimableCodes) > 0 THEN
        -- compare-and-set: only succeeds if nobody changed the status since we read it
        UPDATE FileUploadBatch
        SET ClaimPreviousStatusId = v_CurrentId,
            BatchStatusId = v_InProgressId,
            ClaimedAt = NOW(),
            ClaimedBy = p_ClaimedBy
        WHERE BatchId = p_BatchId AND BatchStatusId = v_CurrentId;
        SET p_Claimed = IF(ROW_COUNT() = 1, 1, 0);
    END IF;
END$$
DELIMITER ;


DROP PROCEDURE IF EXISTS ClaimPendingBatches;
DELIMITER $$
-- Claim up to p_Limit pending batches (oldest BatchId first) and return them as a result set:
--     BatchId, PreviousCode
-- Batches listed in p_ExcludeIds (comma list) are skipped, e.g. the ones this run already processed.
CREATE PROCEDURE ClaimPendingBatches(
    IN p_Limit           INT,
    IN p_ClaimableCodes  VARCHAR(500),    -- comma list, e.g. 'PROCESSING,PARTIAL_FAILURE'
    IN p_InProgressCode  VARCHAR(50),     -- e.g. 'IN_PROGRESS'
    IN p_ExcludeIds      TEXT,            -- comma list of BatchIds to skip ('' = none)
    IN p_ClaimedBy       VARCHAR(100)     -- owner, e.g. 'host:pid:run'
)
BEGIN
    DECLARE v_InProgressId INT DEFAULT NULL;
    DECLARE v_BatchId      INT;
    DECLARE v_StatusId     INT;
    DECLARE v_Code         VARCHAR(50);
    DECLARE v_Claimed      INT DEFAULT 0;
    DECLARE v_Attempts     INT DEFAULT 0;
    DECLARE v_LastId       INT DEFAULT 0;

    SELECT BatchStatusId INTO v_InProgressId FROM RefBatchStatus WHERE Code = p_InProgressCode LIMIT 1;
    IF v_InProgressId IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'ClaimPendingBatches: in-progress status code not found in RefBatchStatus';
    END IF;

    DROP TEMPORARY TABLE IF EXISTS tmp_ClaimedBatches;
    CREATE TEMPORARY TABLE tmp_ClaimedBatches (BatchId INT PRIMARY KEY, PreviousCode VARCHAR(50));

    claim_loop: WHILE v_Claimed < p_Limit AND v_Attempts < p_Limit * 20 DO
        SET v_Attempts = v_Attempts + 1, v_BatchId = NULL;

        -- next candidate after the last one we looked at (keeps the loop moving forward)
        SELECT FB.BatchId, FB.BatchStatusId, B.Code INTO v_BatchId, v_StatusId, v_Code
        FROM FileUploadBatch FB
        JOIN RefBatchStatus B ON B.BatchStatusId = FB.BatchStatusId
        WHERE FIND_IN_SET(B.Code, p_ClaimableCodes) > 0
          AND FB.BatchId > v_LastId
          AND (p_ExcludeIds IS NULL OR p_ExcludeIds = '' OR FIND_IN_SET(FB.BatchId, p_ExcludeIds) = 0)
        ORDER BY FB.BatchId
        LIMIT 1;

        IF v_BatchId IS NULL THEN
            LEAVE claim_loop;                      -- nothing (more) pending
        END IF;
        SET v_LastId = v_BatchId;

        -- compare-and-set: 0 rows means another system took it first -> just try the next one
        UPDATE FileUploadBatch
        SET ClaimPreviousStatusId = v_StatusId,
            BatchStatusId = v_InProgressId,
            ClaimedAt = NOW(),
            ClaimedBy = p_ClaimedBy
        WHERE BatchId = v_BatchId AND BatchStatusId = v_StatusId;

        IF ROW_COUNT() = 1 THEN
            INSERT INTO tmp_ClaimedBatches (BatchId, PreviousCode) VALUES (v_BatchId, v_Code);
            SET v_Claimed = v_Claimed + 1;
        END IF;
    END WHILE;

    SELECT BatchId, PreviousCode FROM tmp_ClaimedBatches ORDER BY BatchId;
    DROP TEMPORARY TABLE IF EXISTS tmp_ClaimedBatches;
END$$
DELIMITER ;


DROP PROCEDURE IF EXISTS ReleaseStaleClaims;
DELIMITER $$
-- Put IN_PROGRESS batches whose claim is older than p_StaleMinutes back to their previous status
-- (or p_FallbackCode when that is unknown). Returns the released batches: BatchId, ClaimedBy, ClaimedAt.
CREATE PROCEDURE ReleaseStaleClaims(
    IN p_InProgressCode VARCHAR(50),      -- e.g. 'IN_PROGRESS'
    IN p_StaleMinutes   INT,              -- e.g. 30 (must be well above the heartbeat interval)
    IN p_FallbackCode   VARCHAR(50)       -- e.g. 'PROCESSING'
)
BEGIN
    DECLARE v_InProgressId INT DEFAULT NULL;
    DECLARE v_FallbackId   INT DEFAULT NULL;

    SELECT BatchStatusId INTO v_InProgressId FROM RefBatchStatus WHERE Code = p_InProgressCode LIMIT 1;
    SELECT BatchStatusId INTO v_FallbackId   FROM RefBatchStatus WHERE Code = p_FallbackCode   LIMIT 1;
    IF v_InProgressId IS NULL OR v_FallbackId IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'ReleaseStaleClaims: status code not found in RefBatchStatus';
    END IF;

    DROP TEMPORARY TABLE IF EXISTS tmp_StaleBatches;
    CREATE TEMPORARY TABLE tmp_StaleBatches AS
        SELECT BatchId, ClaimedBy, ClaimedAt
        FROM FileUploadBatch
        WHERE BatchStatusId = v_InProgressId
          AND ClaimedAt IS NOT NULL
          AND ClaimedAt < NOW() - INTERVAL p_StaleMinutes MINUTE;

    -- the ClaimedAt condition is repeated: a heartbeat that arrived meanwhile keeps the batch claimed
    UPDATE FileUploadBatch FB
    JOIN tmp_StaleBatches T ON T.BatchId = FB.BatchId
    SET FB.BatchStatusId = COALESCE(FB.ClaimPreviousStatusId, v_FallbackId),
        FB.ClaimedAt = NULL, FB.ClaimedBy = NULL, FB.ClaimPreviousStatusId = NULL
    WHERE FB.BatchStatusId = v_InProgressId
      AND FB.ClaimedAt < NOW() - INTERVAL p_StaleMinutes MINUTE;

    SELECT BatchId, ClaimedBy, ClaimedAt FROM tmp_StaleBatches ORDER BY BatchId;
    DROP TEMPORARY TABLE IF EXISTS tmp_StaleBatches;
END$$
DELIMITER ;

-- Examples:
--   CALL ReleaseStaleClaims('IN_PROGRESS', 30, 'PROCESSING');
--   CALL ClaimBatch(52, 'PROCESSING,PARTIAL_FAILURE', 'IN_PROGRESS', 'host:1234:abc', @prev, @ok); SELECT @prev, @ok;
--   CALL ClaimPendingBatches(8, 'PROCESSING,PARTIAL_FAILURE', 'IN_PROGRESS', '52,53', 'host:1234:abc');
