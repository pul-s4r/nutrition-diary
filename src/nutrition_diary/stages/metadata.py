from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from nutrition_diary.pipeline.metadata import extract_exif, parse_datetime_original
from nutrition_diary.stages.base import Stage, StageContext, StageScope


@dataclass(frozen=True)
class MetadataStage(Stage):
    name: str = "metadata"

    def select_work(self, ctx: StageContext, scope: StageScope) -> Iterable[str]:
        if scope.photo_hashes:
            yield from scope.photo_hashes
            return

        if scope.since_date:
            rows = ctx.db.execute(
                """
                SELECT p.photo_hash
                FROM photos p
                LEFT JOIN photo_metadata m ON m.photo_hash = p.photo_hash
                WHERE substr(p.discovered_at, 1, 0) IS NULL OR 1=1
                """,
            ).fetchall()
            # v1: the scope filter is applied in CLI by selecting photos by mtime; keep simple here.
            for r in rows:
                yield str(r["photo_hash"])
            return

        rows = ctx.db.execute("SELECT photo_hash FROM photos").fetchall()
        for r in rows:
            yield str(r["photo_hash"])

    def run_one(self, ctx: StageContext, item_key: str) -> dict | None:
        row = ctx.db.execute(
            "SELECT local_blob_path FROM photos WHERE photo_hash=?",
            (item_key,),
        ).fetchone()
        if row is None:
            raise KeyError(f"photo_hash not found: {item_key}")
        path = Path(str(row["local_blob_path"]))
        if not path.exists():
            raise FileNotFoundError(str(path))

        exif = {}
        try:
            exif = extract_exif(path)
        except Exception:  # noqa: BLE001
            exif = {}

        taken_at = parse_datetime_original(exif.get("DateTimeOriginal"))
        orientation = exif.get("Orientation")
        make = exif.get("Make")
        model = exif.get("Model")

        # fallback: file mtime
        if taken_at is None:
            taken_at = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(path.stat().st_mtime))

        ctx.db.execute(
            """
            INSERT INTO photo_metadata(photo_hash, taken_at, tz, gps_lat, gps_lon, orientation, make, model)
            VALUES (?, ?, NULL, NULL, NULL, ?, ?, ?)
            ON CONFLICT(photo_hash) DO UPDATE SET
              taken_at=excluded.taken_at,
              orientation=excluded.orientation,
              make=excluded.make,
              model=excluded.model
            """,
            (item_key, taken_at, orientation, make, model),
        )
        return {
            "photo_hash": item_key,
            "taken_at": taken_at,
            "orientation": orientation,
            "make": make,
            "model": model,
        }
