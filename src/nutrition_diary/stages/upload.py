from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Iterable

from nutrition_diary.schema.entry import DiaryEntry, FoodItem
from nutrition_diary.stages.base import Stage, StageContext, StageScope
from nutrition_diary.uploaders.base import DiaryUploader
from nutrition_diary.uploaders.csv_export import CsvExportUploader
from nutrition_diary.uploaders.myfitnesspal import MyFitnessPalUploader


@dataclass(frozen=True)
class UploadStage(Stage):
    name: str = "upload"
    target: str = "csv"
    uploader: DiaryUploader | None = None

    def _get_uploader(self, ctx: StageContext) -> DiaryUploader:
        if self.uploader is not None:
            return self.uploader
        if self.target == "csv":
            return CsvExportUploader()
        if self.target == "mfp":
            return MyFitnessPalUploader(settings=ctx.settings)
        raise RuntimeError(f"Unsupported upload target: {self.target}")

    def select_work(self, ctx: StageContext, scope: StageScope) -> Iterable[str]:
        if scope.entry_id:
            yield scope.entry_id
            return
        rows = ctx.db.execute(
            "SELECT entry_id FROM upload_queue WHERE target=? AND status='pending'",
            (self.target,),
        ).fetchall()
        for r in rows:
            yield str(r["entry_id"])

    def run_one(self, ctx: StageContext, item_key: str) -> dict | None:
        row = ctx.db.execute(
            "SELECT entry_id, date, meal_type, items_json, overall_confidence FROM diary_entries WHERE entry_id=?",
            (item_key,),
        ).fetchone()
        if row is None:
            raise KeyError(f"entry not found: {item_key}")

        items = [FoodItem(**d) for d in json.loads(str(row["items_json"]))]
        entry = DiaryEntry(
            entry_id=str(row["entry_id"]),
            date=str(row["date"]),
            meal_type=str(row["meal_type"]),
            items=items,
            source_photos=[],
            overall_confidence=float(row["overall_confidence"] or 0.0),
        )

        uploader = self._get_uploader(ctx)
        uploader.authenticate()
        res = uploader.submit_entry(entry)

        now = int(time.time())
        if res.success:
            ctx.db.execute(
                """
                UPDATE upload_queue
                SET status='success', attempts=attempts+1, last_error=NULL, updated_at=?
                WHERE entry_id=? AND target=?
                """,
                (now, item_key, self.target),
            )
        else:
            ctx.db.execute(
                """
                UPDATE upload_queue
                SET status='failed', attempts=attempts+1, last_error=?, updated_at=?
                WHERE entry_id=? AND target=?
                """,
                (res.error, now, item_key, self.target),
            )
        return {"entry_id": item_key, "target": self.target, "success": res.success, "external_id": res.external_id}

