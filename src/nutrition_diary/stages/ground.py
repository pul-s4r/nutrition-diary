from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Iterable

from nutrition_diary.grounding.base import NutritionGrounder
from nutrition_diary.stages.base import Stage, StageContext, StageScope


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
            ctx.db.execute(
                """
                INSERT INTO grounding_results(photo_hash, source, fdc_id, matched_name, match_conf,
                                             per_100g_json, scaled_json, created_at)
                VALUES (?, NULL, NULL, NULL, NULL, NULL, NULL, ?)
                ON CONFLICT(photo_hash) DO UPDATE SET created_at=excluded.created_at
                """,
                (item_key, now),
            )
            return {"photo_hash": item_key, "grounded": False, "reason": "not_food"}

        ident = json.loads(str(ident_json))
        food_name = str(ident["name"])
        serving_size_g = float(ident["serving_size_g"])

        if ctx.settings.usda_api_key is None:
            ctx.db.execute(
                """
                INSERT INTO grounding_results(photo_hash, source, fdc_id, matched_name, match_conf,
                                             per_100g_json, scaled_json, created_at)
                VALUES (?, NULL, NULL, NULL, NULL, NULL, NULL, ?)
                ON CONFLICT(photo_hash) DO UPDATE SET created_at=excluded.created_at
                """,
                (item_key, now),
            )
            return {"photo_hash": item_key, "grounded": False, "reason": "no_usda_key"}

        raise RuntimeError(
            "USDA grounding is not yet wired to live API in this implementation. "
            "Set ND_USDA_API_KEY to enable later, or use mock mode."
        )

