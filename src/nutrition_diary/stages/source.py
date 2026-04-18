from __future__ import annotations

import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from nutrition_diary.sources.base import PhotoSource
from nutrition_diary.stages.base import Stage, StageContext, StageScope
from nutrition_diary.util import sha256_file


@dataclass(frozen=True)
class SourceStage(Stage):
    name: str = "source"
    source: PhotoSource = None  # type: ignore[assignment]
    root: Path = None  # type: ignore[assignment]

    def select_work(self, ctx: StageContext, scope: StageScope) -> Iterable[str]:
        since = scope.since_date
        for path in self.source.list_photos(self.root, since_date=since):
            photo_hash = sha256_file(path)
            yield photo_hash

    def run_one(self, ctx: StageContext, item_key: str) -> dict | None:
        # item_key is photo_hash; locate file by rescanning (cheap enough for v1)
        for path in self.source.list_photos(self.root, since_date=None):
            if sha256_file(path) != item_key:
                continue

            blobs_dir = ctx.settings.data_dir / "blobs"
            blobs_dir.mkdir(parents=True, exist_ok=True)
            blob_path = blobs_dir / f"{item_key}{path.suffix.lower()}"
            if not blob_path.exists():
                shutil.copy2(path, blob_path)

            now = int(time.time())
            ctx.db.execute(
                """
                INSERT INTO photos(photo_hash, source_adapter, source_ref, local_blob_path, discovered_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(photo_hash) DO UPDATE SET
                  source_ref=excluded.source_ref,
                  local_blob_path=excluded.local_blob_path
                """,
                (item_key, self.source.name, str(path), str(blob_path), now),
            )
            return {
                "photo_hash": item_key,
                "source_adapter": self.source.name,
                "source_ref": str(path),
                "local_blob_path": str(blob_path),
            }

        raise FileNotFoundError(f"Could not locate photo bytes for hash={item_key}")

