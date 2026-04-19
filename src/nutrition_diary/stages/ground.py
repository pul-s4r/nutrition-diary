from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Iterable

from nutrition_diary.grounding.base import GroundingResult, NutritionGrounder
from nutrition_diary.grounding.usda import UsdaGrounder
from nutrition_diary.stages.base import Stage, StageContext, StageScope


def _per_100g_dict(gr: GroundingResult) -> dict:
    return {
        "calories_per_100g": gr.calories_per_100g,
        "fat_per_100g": gr.fat_per_100g,
        "carbs_per_100g": gr.carbs_per_100g,
        "protein_per_100g": gr.protein_per_100g,
        "fiber_per_100g": gr.fiber_per_100g,
        "sugar_per_100g": gr.sugar_per_100g,
        "sodium_per_100g": gr.sodium_per_100g,
    }


@dataclass(frozen=True)
class GroundStage(Stage):
    name: str = "ground"
    grounder: NutritionGrounder | None = None

    def select_work(self, ctx: StageContext, scope: StageScope) -> Iterable[str]:
        if scope.photo_hashes:
            yield from scope.photo_hashes
            return
        rows = ctx.db.execute("SELECT photo_hash FROM llm_results").fetchall()
        for r in rows:
            yield str(r["photo_hash"])

    def _write_empty(self, ctx: StageContext, item_key: str, now: int) -> None:
        ctx.db.execute(
            """
            INSERT INTO grounding_results(photo_hash, source, fdc_id, matched_name, match_conf,
                                             per_100g_json, scaled_json, created_at)
            VALUES (?, NULL, NULL, NULL, NULL, NULL, NULL, ?)
            ON CONFLICT(photo_hash) DO UPDATE SET
              source=NULL,
              fdc_id=NULL,
              matched_name=NULL,
              match_conf=NULL,
              per_100g_json=NULL,
              scaled_json=NULL,
              created_at=excluded.created_at
            """,
            (item_key, now),
        )

    def run_one(self, ctx: StageContext, item_key: str) -> dict | None:
        row = ctx.db.execute(
            "SELECT identification_json FROM llm_results WHERE photo_hash=?",
            (item_key,),
        ).fetchone()
        if row is None:
            raise KeyError(f"llm_results not found for photo_hash={item_key}")

        ident_json = row["identification_json"]
        now = int(time.time())

        if ident_json is None:
            self._write_empty(ctx, item_key, now)
            return {"photo_hash": item_key, "grounded": False, "reason": "not_food"}

        ident = json.loads(str(ident_json))
        food_name = str(ident["name"])
        serving_size_g = float(ident["serving_size_g"])

        if ctx.settings.usda_api_key is None:
            self._write_empty(ctx, item_key, now)
            return {"photo_hash": item_key, "grounded": False, "reason": "no_usda_key"}

        grounder = self.grounder or UsdaGrounder(ctx.settings, ctx.db)
        gr = grounder.ground(food_name, serving_size_g)
        if gr is None:
            self._write_empty(ctx, item_key, now)
            return {"photo_hash": item_key, "grounded": False, "reason": "no_match"}

        per_100 = _per_100g_dict(gr)
        scaled = UsdaGrounder.scaled_payload(gr, serving_size_g)
        ctx.db.execute(
            """
            INSERT INTO grounding_results(photo_hash, source, fdc_id, matched_name, match_conf,
                                             per_100g_json, scaled_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(photo_hash) DO UPDATE SET
              source=excluded.source,
              fdc_id=excluded.fdc_id,
              matched_name=excluded.matched_name,
              match_conf=excluded.match_conf,
              per_100g_json=excluded.per_100g_json,
              scaled_json=excluded.scaled_json,
              created_at=excluded.created_at
            """,
            (
                item_key,
                gr.source,
                gr.normalized_food_id,
                gr.matched_name,
                gr.match_confidence,
                json.dumps(per_100, sort_keys=True),
                json.dumps(scaled, sort_keys=True),
                now,
            ),
        )
        return {
            "photo_hash": item_key,
            "grounded": True,
            "fdc_id": gr.normalized_food_id,
            "match_confidence": gr.match_confidence,
        }
