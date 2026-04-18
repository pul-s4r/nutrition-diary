from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Iterable

from nutrition_diary.schema.entry import DiaryEntry, FoodItem
from nutrition_diary.stages.base import Stage, StageContext, StageScope


@dataclass(frozen=True)
class AssembleStage(Stage):
    name: str = "assemble"

    def select_work(self, ctx: StageContext, scope: StageScope) -> Iterable[str]:
        if scope.cluster_id:
            yield scope.cluster_id
            return
        rows = ctx.db.execute("SELECT cluster_id FROM meal_clusters").fetchall()
        for r in rows:
            yield str(r["cluster_id"])

    def run_one(self, ctx: StageContext, item_key: str) -> dict | None:
        cluster = ctx.db.execute(
            "SELECT date, meal_type FROM meal_clusters WHERE cluster_id=?",
            (item_key,),
        ).fetchone()
        if cluster is None:
            raise KeyError(f"cluster not found: {item_key}")

        photos = ctx.db.execute(
            """
            SELECT p.photo_hash, p.source_ref, l.identification_json, l.confidence, g.source, g.fdc_id,
                   g.matched_name, g.match_conf
            FROM meal_photos mp
            JOIN photos p ON p.photo_hash=mp.photo_hash
            LEFT JOIN llm_results l ON l.photo_hash=p.photo_hash
            LEFT JOIN grounding_results g ON g.photo_hash=p.photo_hash
            WHERE mp.cluster_id=?
            ORDER BY p.photo_hash
            """,
            (item_key,),
        ).fetchall()

        items: list[FoodItem] = []
        source_photos: list[str] = []
        entry_conf = 1.0

        for r in photos:
            source_photos.append(str(r["source_ref"]))
            ident_json = r["identification_json"]
            if ident_json is None:
                # Not food or not recognized; skip item but lower confidence.
                entry_conf = min(entry_conf, 0.0)
                continue

            ident = json.loads(str(ident_json))
            llm_conf = float(r["confidence"] or 0.0)
            match_conf = r["match_conf"]
            if match_conf is not None:
                overall = min(llm_conf, float(match_conf))
            else:
                overall = llm_conf

            item = FoodItem(
                name=str(ident["name"]),
                serving_size_g=float(ident["serving_size_g"]),
                serving_unit=str(ident.get("serving_unit", "g")),
                serving_description=str(ident.get("serving_description", "")),
                llm_confidence=llm_conf,
                normalized_food_id=None if r["fdc_id"] is None else str(r["fdc_id"]),
                grounding_source=None if r["source"] is None else str(r["source"]),
                grounding_match_confidence=None if match_conf is None else float(match_conf),
                overall_confidence=overall,
            )
            entry_conf = min(entry_conf, overall)
            items.append(item)

        entry = DiaryEntry(
            entry_id=item_key,
            date=str(cluster["date"]),
            meal_type=str(cluster["meal_type"]),
            items=items,
            source_photos=source_photos,
            overall_confidence=0.0 if entry_conf == 1.0 and not items else float(entry_conf),
        )

        ctx.db.execute(
            """
            INSERT INTO diary_entries(entry_id, date, meal_type, items_json, overall_confidence, source_cluster_id, approved)
            VALUES (?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(entry_id) DO UPDATE SET
              date=excluded.date,
              meal_type=excluded.meal_type,
              items_json=excluded.items_json,
              overall_confidence=excluded.overall_confidence,
              source_cluster_id=excluded.source_cluster_id
            """,
            (
                entry.entry_id,
                entry.date,
                entry.meal_type,
                json.dumps([i.to_dict() for i in entry.items], sort_keys=True),
                entry.overall_confidence,
                item_key,
            ),
        )
        return entry.to_dict()

