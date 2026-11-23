"""Test helper: replace the HTTP download of normalized CSVs with a local file copy."""
import shutil
from pathlib import Path

import secr.normalize as normalize


FAKE_HOST = "https://fake.test/"


def fake_url(local_path) -> str:
    """A real-looking https link (NA filtering only accepts http(s) URLs) that points at a local test file."""
    return FAKE_HOST + Path(local_path).as_posix()


def _copy(link, destination: Path, timeout=300):
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(link[len(FAKE_HOST):] if link.startswith(FAKE_HOST) else link, destination)
    return destination


normalize.download_link = _copy
normalize.filename_from_url = lambda url: Path(url).name
