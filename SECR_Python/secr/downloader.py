"""Download a normalized CSV link (port of the inline ``download_file`` Python script)."""
from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import unquote, urlparse


def filename_from_url(url: str) -> str:
    name = os.path.basename(urlparse(url).path)
    return unquote(name) if name else "downloaded_file.csv"


def download_link(link: str, destination: Path, timeout: int = 300, chunk_size: int = 65536) -> Path:
    import requests

    if not link:
        raise ValueError("link is required")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(link, stream=True, timeout=timeout) as resp:
        resp.raise_for_status()
        with destination.open("wb") as fh:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if chunk:
                    fh.write(chunk)
    return destination
