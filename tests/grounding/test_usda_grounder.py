from __future__ import annotations

import json
from unittest.mock import patch

from nutrition_diary.config import Settings
from nutrition_diary.db import connect, migrate
from nutrition_diary.grounding.usda import UsdaGrounder


def _detail_payload() -> dict:
    return {
        "foodNutrients": [
            {"nutrientId": 1008, "amount": 200.0},
            {"nutrientId": 1003, "amount": 25.0},
            {"nutrientId": 1004, "amount": 10.0},
            {"nutrientId": 1005, "amount": 0.0},
            {"nutrientId": 1079, "amount": 0.0},
            {"nutrientId": 2000, "amount": 0.0},
            {"nutrientId": 1093, "amount": 50.0},
        ]
    }


@patch("nutrition_diary.grounding.usda._get_json")
@patch("nutrition_diary.grounding.usda._post_json")
def test_happy_path_scales(mock_post, mock_get, tmp_path) -> None:
    mock_post.return_value = {
        "foods": [
            {
                "fdcId": 999,
                "description": "Chicken breast",
                "dataType": "Foundation",
            }
        ]
    }
    mock_get.return_value = _detail_payload()

    db_path = tmp_path / "n.db"
    conn = connect(db_path)
    migrate(conn)
    settings = Settings(usda_api_key="test-key", usda_cache_ttl_days=90)
    g = UsdaGrounder(settings, conn)
    res = g.ground("chicken", 200.0)
    assert res is not None
    assert res.normalized_food_id == "999"
    assert res.match_confidence >= 0.65
    scaled = UsdaGrounder.scaled_payload(res, 200.0)
    assert abs(scaled["calories"] - 400.0) < 1e-6
    conn.commit()


@patch("nutrition_diary.grounding.usda._post_json")
def test_below_threshold_caches_negative(mock_post, tmp_path) -> None:
    mock_post.return_value = {
        "foods": [
            {
                "fdcId": 1,
                "description": "ZZZZZZ entirely different food name",
                "dataType": "Branded",
            }
        ]
    }

    db_path = tmp_path / "n.db"
    conn = connect(db_path)
    migrate(conn)
    settings = Settings(usda_api_key="key", usda_match_threshold=0.65)
    g = UsdaGrounder(settings, conn)
    assert g.ground("chicken", 100.0) is None
    conn.commit()
