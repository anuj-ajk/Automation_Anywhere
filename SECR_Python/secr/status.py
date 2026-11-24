"""Status codes used in ESG DB (RefFileProcessingStatus.Code / RefBatchStatus.Code)."""

# UploadedFile.ProcessingStatusId
PARSING = "PARSING"
PARSED = "PARSED"
NORMALIZING = "NORMALIZING"
NORMALIZATION_COMPLETED = "NORMALIZATION_COMPLETED"
FAILED_WHILE_NORMALIZATION = "FAILED_WHILE_NORMALIZATION"
PARTIAL_FAILURE = "PARTIAL_FAILURE"
COMPLETED = "COMPLETED"
FAILED = "FAILED"
DATA_NOT_AVAILABLE = "DATA_NOT_AVAILABLE"

# FileUploadBatch.BatchStatusId
BATCH_COMPLETE = "COMPLETE"
BATCH_IN_PROGRESS = "IN_PROGRESS"  # set as soon as a batch is picked up (if it exists)
BATCH_PARTIAL_FAILURE = "PARTIAL_FAILURE"  # used when some files failed, if it exists in RefBatchStatus

# File outcomes that count as "fine" for the batch summary.
OK_FILE_STATUSES = {COMPLETED, DATA_NOT_AVAILABLE}

# Files already in one of these statuses are skipped when a batch is run again (run.skip_completed_files).
DEFAULT_COMPLETED_STATUSES = (COMPLETED, DATA_NOT_AVAILABLE)

DAISY_FILE_NAME = "Source: Daisy"


def roll_up(success: int, failed: int, *, all_ok: str, none_ok: str) -> str:
    """Same rule the AA bots used: 0 failures -> all_ok, 0 successes -> none_ok, else partial."""
    if failed == 0:
        return all_ok
    if success == 0:
        return none_ok
    return PARTIAL_FAILURE
