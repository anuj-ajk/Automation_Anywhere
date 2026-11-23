"""AWS S3 download (replaces the 'ESG Common Utility / AWS S3 File Management' bot)."""
from __future__ import annotations

from pathlib import Path
from urllib.parse import unquote, urlparse

from .config import S3Config


class S3Storage:
    def __init__(self, cfg: S3Config):
        import boto3  # boto3 clients are thread-safe; one client is shared by all workers

        if not cfg.bucket:
            raise ValueError("S3 bucket is not configured (SECR_S3_BUCKET)")
        self.bucket = cfg.bucket
        # Credentials: standard AWS env vars / profile / instance role.
        self._client = boto3.client("s3", region_name=cfg.region)

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
        destination.parent.mkdir(parents=True, exist_ok=True)
        self._client.download_file(bucket, key, str(destination))
        if not destination.exists() or destination.stat().st_size == 0:
            raise IOError(f"Downloaded file is empty: s3://{bucket}/{key}")
        return destination
