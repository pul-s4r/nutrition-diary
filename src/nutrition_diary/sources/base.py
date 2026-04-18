from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Protocol


@dataclass(frozen=True)
class PhotoRef:
    source_adapter: str
    source_ref: str  # adapter-specific identifier (path, drive file id, s3 key)


@dataclass(frozen=True)
class PhotoMetadata:
    photo_hash: str
    taken_at: str | None = None  # ISO8601
    tz: str | None = None
    gps_lat: float | None = None
    gps_lon: float | None = None
    orientation: int | None = None
    make: str | None = None
    model: str | None = None


class PhotoSource(Protocol):
    name: str

    def list_photos(self, root: Path, *, since_date: str | None = None) -> Iterator[Path]: ...

