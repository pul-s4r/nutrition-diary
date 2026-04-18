from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterator

from nutrition_diary.sources.base import PhotoSource


class LocalPhotoSource(PhotoSource):
    name = "local"

    def list_photos(self, root: Path, *, since_date: str | None = None) -> Iterator[Path]:
        root = root.expanduser().resolve()
        if not root.exists():
            return iter(())

        since_dt = None
        if since_date is not None:
            since_dt = datetime.strptime(since_date, "%Y-%m-%d")

        exts = {".jpg", ".jpeg", ".png", ".heic", ".webp"}
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            if path.suffix.lower() not in exts:
                continue
            if since_dt is not None:
                mtime = datetime.fromtimestamp(path.stat().st_mtime)
                if mtime.date() < since_dt.date():
                    continue
            yield path

