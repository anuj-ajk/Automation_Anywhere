"""AWS S3 download (replaces the 'ESG Common Utility / AWS S3 File Management' bot)."""
from __future__ import annotations

import time
from pathlib import Path
from urllib.parse import unquote, urlparse

from .config import S3Config
from .logging_setup import module_logger

LOG = module_logger(__name__)


class S3Storage:
    def __init__(self, cfg: S3Config):
        import boto3  # boto3 clients are thread-safe; one client is shared by all workers

        if not cfg.bucket:
            raise ValueError("S3 bucket is not configured (SECR_S3_BUCKET)")
        self.bucket = cfg.bucket
        # Credentials: standard AWS env vars / profile / instance role.
        self._client = boto3.client("s3", region_name=cfg.region)
        LOG.info("S3 storage ready: bucket %s, region %s", self.bucket, cfg.region)

    def _split(self, storage_path: str) -> tuple[str, str]:
        """Accept 's3://bucket/key', 'https://bucket.s3...amazonaws.com/key' or a plain key."""
        storage_path = storage_path.strip()
        if storage_path.startswith("s3://"):
            p = urlparse(storage_path)
            return p.netloc, unquote(p.path.lstrip("/"))
        if storage_path.startswith(("http://", "https://")):
            p = urlparse(storage_path)
            bucket = p.netloc.split(".s3")[0] if ".s3" in p.netloc else self.bucket
            return bucket, unquote(p.path.lstrip("/"))
        return self.bucket, storage_path.lstrip("/")

    def download(self, storage_path: str, destination: Path) -> Path:
        bucket, key = self._split(storage_path)
        LOG.info("Downloading s3://%s/%s", bucket, key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        started = time.monotonic()
        try:
            self._client.download_file(bucket, key, str(destination))
        except Exception as exc:
            LOG.error("S3 download of s3://%s/%s failed: %s", bucket, key, exc)
            raise
        if not destination.exists() or destination.stat().st_size == 0:
            raise IOError(f"Downloaded file is empty: s3://{bucket}/{key}")
        LOG.info("Downloaded s3://%s/%s -> %s (%d bytes, %.1fs)", bucket, key, destination.name,
                 destination.stat().st_size, time.monotonic() - started)
        return destination
