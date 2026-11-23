"""Test helper: replace the HTTP download of normalized CSVs with a local file copy."""
import shutil
from pathlib import Path

import secr.normalize as normalize


def _copy(link, destination: Path, timeout=300):
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(link, destination)
    return destination


normalize.download_link = _copy
normalize.filename_from_url = lambda url: Path(url).name
