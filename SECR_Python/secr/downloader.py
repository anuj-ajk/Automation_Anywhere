"""Download a normalized CSV link (port of the inline ``download_file`` Python script)."""
from __future__ import annotations

import os
import time
from pathlib import Path
from urllib.parse import unquote, urlparse

from .logging_setup import module_logger

LOG = module_logger(__name__)


def filename_from_url(url: str) -> str:
    name = os.path.basename(urlparse(url).path)
    return unquote(name) if name else "downloaded_file.csv"


def download_link(link: str, destination: Path, timeout: int = 300, chunk_size: int = 65536) -> Path:
    import requests

    if not link:
        raise ValueError("link is required")
    destination.parent.mkdir(parents=True, exist_ok=True)
    parsed = urlparse(link)
    safe_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"  # no query string: may hold a signed token
    LOG.debug("Downloading %s", safe_url)
    started = time.monotonic()
    size = 0
    with requests.get(link, stream=True, timeout=timeout) as resp:
        if resp.status_code >= 400:
            LOG.error("Download of %s failed: HTTP %s", safe_url, resp.status_code)
        resp.raise_for_status()
        with destination.open("wb") as fh:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if chunk:
                    fh.write(chunk)
                    size += len(chunk)
    LOG.info("Downloaded %s (%d bytes, %.1fs)", destination.name, size, time.monotonic() - started)
    if size == 0:
        LOG.warning("Downloaded file %s is empty", destination.name)
    return destination
