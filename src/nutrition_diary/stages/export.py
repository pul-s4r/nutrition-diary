from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Iterable

from nutrition_diary.export.writer import append_jsonl, existing_entry_ids
from nutrition_diary.stages.base import Stage, StageContext, StageScope


@dataclass(frozen=True)
class ExportStage(Stage):
    name: str = "export"
    target: str = "csv"

    def select_work(self, ctx: StageContext, scope: StageScope) -> Iterable[str]:
        if scope.entry_id:
            yield scope.entry_id
            return
        rows = ctx.db.execute(
            "SELECT entry_id FROM diary_entries WHERE approved=1",
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

        date = str(row["date"])
        export_path = ctx.settings.exports_dir / f"{date}.jsonl"
        already = existing_entry_ids(export_path)
        payload = {
            "entry_id": str(row["entry_id"]),
            "date": date,
            "meal_type": str(row["meal_type"]),
            "items": json.loads(str(row["items_json"])),
            "overall_confidence": row["overall_confidence"],
        }
        if payload["entry_id"] not in already:
            append_jsonl(export_path, payload)

        now = int(time.time())
        if ctx.force:
            ctx.db.execute(
                """
                INSERT INTO upload_queue(entry_id, target, status, attempts, last_error, updated_at)
                VALUES (?, ?, 'pending', 0, NULL, ?)
                ON CONFLICT(entry_id, target) DO UPDATE SET
                  status='pending',
                  last_error=NULL,
                  updated_at=excluded.updated_at
                """,
                (item_key, self.target, now),
            )
        else:
            ctx.db.execute(
                """
                INSERT INTO upload_queue(entry_id, target, status, attempts, last_error, updated_at)
                VALUES (?, ?, 'pending', 0, NULL, ?)
                ON CONFLICT(entry_id, target) DO UPDATE SET
                  updated_at=excluded.updated_at
                """,
                (item_key, self.target, now),
            )
        return {"export_path": str(export_path), "queued_target": self.target, **payload}
