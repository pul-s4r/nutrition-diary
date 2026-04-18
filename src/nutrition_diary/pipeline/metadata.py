from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from PIL import Image, ExifTags


def extract_exif(path: Path) -> dict[str, Any]:
    img = Image.open(path)
    exif = getattr(img, "getexif", lambda: None)()
    if not exif:
        return {}
    out: dict[str, Any] = {}
    tag_map = {v: k for k, v in ExifTags.TAGS.items()}
    for key_name in ["DateTimeOriginal", "Orientation", "Make", "Model"]:
        tag = tag_map.get(key_name)
        if tag is None:
            continue
        if tag in exif:
            out[key_name] = exif.get(tag)
    return out


def parse_datetime_original(value: str | None) -> str | None:
    if not value:
        return None
    # EXIF is usually "YYYY:MM:DD HH:MM:SS"
    try:
        dt = datetime.strptime(value, "%Y:%m:%d %H:%M:%S")
        return dt.isoformat()
    except Exception:  # noqa: BLE001
        return None

