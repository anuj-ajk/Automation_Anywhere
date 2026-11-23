"""Local file names that stay under the Windows 260-character path limit.

The work folder can be deep (e.g. on \\\\server\\share\\...), and uploaded file names can be long. Without
long-path support, Windows rejects anything over 259 characters with a misleading
"[Errno 2] No such file or directory".
"""
from __future__ import annotations

import os
import re
from pathlib import Path

WINDOWS_MAX_PATH = 259
_UNSAFE = re.compile(r'[<>:"/\\|?*\x00-\x1f]+')


def short_name(name: str, max_stem: int = 40) -> str:
    """Keep the extension, shorten the rest: 'Inspired ESG_SECR_DSF_..._161448552.xlsx' -> 40 chars + '.xlsx'."""
    p = Path(name or "file")
    stem = _UNSAFE.sub("_", p.stem).strip(" .") or "file"
    return f"{stem[:max_stem].rstrip(' .')}{p.suffix.lower()}"


def check_path_length(path: Path, extra: int = 0) -> None:
    """Raise a clear error when ``path`` (+ ``extra`` characters, e.g. a temp suffix) is too long for Windows."""
    if os.name != "nt":
        return
    full = str(Path(path).absolute())
    if len(full) + extra > WINDOWS_MAX_PATH:
        raise OSError(
            f"Local path is {len(full) + extra} characters, over the Windows limit of {WINDOWS_MAX_PATH}: {full}. "
            "Set run.work_dir in config.yaml to a short local folder (e.g. C:\\SECR_work)."
        )
