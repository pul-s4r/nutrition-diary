from __future__ import annotations

import json
import time

from nutrition_diary.db import connect, migrate
from nutrition_diary.grounding.cache import UsdaCache


def test_positive_hit_within_ttl(tmp_path) -> None:
    db_path = tmp_path / "n.db"
    conn = connect(db_path)
    migrate(conn)
    c = UsdaCache(conn)
    now = int(time.time())
    c.put_positive(
        "chicken",
        fdc_id="123",
        matched_name="Chicken, roasted",
        match_confidence=0.9,
        data_type="Foundation",
        nutrients_json=json.dumps({"calories_per_100g": 200.0, "fat_per_100g": 10.0, "carbs_per_100g": 0.0, "protein_per_100g": 25.0, "fiber_per_100g": 0.0, "sugar_per_100g": 0.0, "sodium_per_100g": 50.0}),
        now_ts=now,
        ttl_seconds=86400 * 90,
    )
    conn.commit()
    row = c.get_row("chicken", now + 100)
    assert row is not None
    assert c.is_negative_hit(row) is False


def test_expired_treated_as_miss(tmp_path) -> None:
    db_path = tmp_path / "n.db"
    conn = connect(db_path)
    migrate(conn)
    c = UsdaCache(conn)
    now = int(time.time())
    c.put_positive(
        "rice",
        fdc_id="9",
        matched_name="Rice",
        match_confidence=0.8,
        data_type="SR Legacy",
        nutrients_json=json.dumps(
            {
                "calories_per_100g": 130.0,
                "fat_per_100g": 0.3,
                "carbs_per_100g": 28.0,
                "protein_per_100g": 2.7,
                "fiber_per_100g": 0.4,
                "sugar_per_100g": 0.1,
                "sodium_per_100g": 1.0,
            }
        ),
        now_ts=now - 100 * 86400,
        ttl_seconds=90 * 86400,
    )
    conn.commit()
    assert c.get_row("rice", now) is None


def test_negative_cache_written(tmp_path) -> None:
    db_path = tmp_path / "n.db"
    conn = connect(db_path)
    migrate(conn)
    c = UsdaCache(conn)
    now = int(time.time())
    c.put_miss("weird_food_xyz", now_ts=now, ttl_seconds=7 * 86400)
    conn.commit()
    row = c.get_row("weird_food_xyz", now + 1)
    assert row is not None
    assert c.is_negative_hit(row) is True
