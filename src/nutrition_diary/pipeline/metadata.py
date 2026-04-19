from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from PIL import Image, ExifTags

# Exif sub-IFD (DateTimeOriginal, etc.); Pillow 8.2+ often stores these here, not in the root IFD.
_EXIF_IFD = 0x8769


def extract_exif(path: Path) -> dict[str, Any]:
    tag_map = {v: k for k, v in ExifTags.TAGS.items()}
    wanted = ("DateTimeOriginal", "Orientation", "Make", "Model")
    out: dict[str, Any] = {}

    def _fill_from(mapping: Any) -> None:
        if not mapping:
            return
        for key_name in wanted:
            if key_name in out:
                continue
            tag = tag_map.get(key_name)
            if tag is None:
                continue
            try:
                if tag not in mapping:  # type: ignore[operator]
                    continue
                val = mapping[tag]  # type: ignore[index]
            except (KeyError, TypeError):
                continue
            if val is not None:
                out[key_name] = val

    with Image.open(path) as img:
        exif = getattr(img, "getexif", lambda: None)()
        if not exif:
            return {}
        _fill_from(exif)
        get_ifd = getattr(exif, "get_ifd", None)
        if callable(get_ifd):
            try:
                _fill_from(get_ifd(_EXIF_IFD))
            except (KeyError, ValueError, TypeError, OSError):
                pass
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
