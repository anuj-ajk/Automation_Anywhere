-- =====================================================================================================
-- Index on Staging_HHData(FileId)  (run ONCE; safe to run again - it does nothing if the index exists)
--
-- Every HH load first runs  DELETE FROM Staging_HHData WHERE FileId = ?  (clear_staging_before_load) and
-- LoadHHData reads the table by FileId. Without an index that is a full scan of a large table: the log of
-- 24 Nov 2026 shows the DELETE taking 9.7s while finding 0 rows, inside the staging lock, so every other
-- HH load waits for it.
--
-- ALGORITHM=INPLACE, LOCK=NONE: built online, inserts / loads can keep running while it is created.
-- =====================================================================================================

SET @index_exists = (
    SELECT COUNT(*) FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME   = 'Staging_HHData'
      AND INDEX_NAME   = 'IX_Staging_HHData_FileId'
);

SET @ddl = IF(@index_exists = 0,
    'CREATE INDEX IX_Staging_HHData_FileId ON Staging_HHData (FileId) ALGORITHM=INPLACE LOCK=NONE',
    'SELECT ''IX_Staging_HHData_FileId already exists'' AS info');

PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Check: the DELETE / SELECT by FileId should now use the index (key = IX_Staging_HHData_FileId)
SHOW INDEX FROM Staging_HHData WHERE Key_name = 'IX_Staging_HHData_FileId';
EXPLAIN SELECT 1 FROM Staging_HHData WHERE FileId = 86;

-- Rollback:
--   DROP INDEX IX_Staging_HHData_FileId ON Staging_HHData;
