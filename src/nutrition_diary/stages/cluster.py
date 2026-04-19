from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable

from nutrition_diary.pipeline.clustering import (
    MealWindows,
    deterministic_cluster_id,
    meal_type_for,
    parse_taken_at,
)
from nutrition_diary.stages.base import Stage, StageContext, StageScope


@dataclass
class ClusterStage(Stage):
    name: str = "cluster"
    _clusters: dict[str, dict] = field(default_factory=dict, init=False)

    def select_work(self, ctx: StageContext, scope: StageScope) -> Iterable[str]:
        windows = MealWindows(
            breakfast_start=ctx.settings.breakfast_start,
            breakfast_end=ctx.settings.breakfast_end,
            lunch_start=ctx.settings.lunch_start,
            lunch_end=ctx.settings.lunch_end,
            dinner_start=ctx.settings.dinner_start,
            dinner_end=ctx.settings.dinner_end,
        )

        rows = ctx.db.execute(
            """
            SELECT m.photo_hash, m.taken_at
            FROM photo_metadata m
            """
        ).fetchall()

        by_date_meal: dict[tuple[str, str], list[tuple[str, datetime]]] = {}
        for r in rows:
            taken_raw = r["taken_at"]
            if taken_raw is None:
                continue
            dt = parse_taken_at(str(taken_raw))
            date = dt.date().isoformat()
            mt = meal_type_for(dt.time(), windows)
            by_date_meal.setdefault((date, mt), []).append((str(r["photo_hash"]), dt))

        for (date, mt), pairs in by_date_meal.items():
            photo_hashes = [p[0] for p in pairs]
            dts = [p[1] for p in pairs]
            earliest = min(dts)
            latest = max(dts)
            cid = deterministic_cluster_id(date, mt, photo_hashes)
            self._clusters[cid] = {
                "date": date,
                "meal_type": mt,
                "photo_hashes": photo_hashes,
                "earliest_taken_at": earliest.isoformat(timespec="seconds"),
                "latest_taken_at": latest.isoformat(timespec="seconds"),
            }
            yield cid

    def run_one(self, ctx: StageContext, item_key: str) -> dict | None:
        cluster = self._clusters.get(item_key)
        if cluster is None:
            raise KeyError(f"cluster not computed for id={item_key}")

        ctx.db.execute(
            """
            INSERT INTO meal_clusters(
              cluster_id, date, meal_type, earliest_taken_at, latest_taken_at
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(cluster_id) DO UPDATE SET
              date=excluded.date,
              meal_type=excluded.meal_type,
              earliest_taken_at=excluded.earliest_taken_at,
              latest_taken_at=excluded.latest_taken_at
            """,
            (
                item_key,
                cluster["date"],
                cluster["meal_type"],
                cluster["earliest_taken_at"],
                cluster["latest_taken_at"],
            ),
        )

        ctx.db.execute("DELETE FROM meal_photos WHERE cluster_id=?", (item_key,))
        for ph in cluster["photo_hashes"]:
            ctx.db.execute(
                "INSERT OR IGNORE INTO meal_photos(cluster_id, photo_hash) VALUES (?, ?)",
                (item_key, ph),
            )

        return {"cluster_id": item_key, **cluster}
