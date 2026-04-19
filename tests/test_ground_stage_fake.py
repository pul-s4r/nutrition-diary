from __future__ import annotations

import json

from nutrition_diary.config import Settings
from nutrition_diary.db import connect, migrate
from nutrition_diary.grounding.base import GroundingResult
from nutrition_diary.stages import GroundStage, StageContext, StageScope, run_stage


class _FakeGrounder:
    def ground(self, food_name: str, serving_size_g: float) -> GroundingResult | None:
        return GroundingResult(
            normalized_food_id="fdc-1",
            source="usda_fdc",
            matched_name="Test food",
            match_confidence=0.88,
            calories_per_100g=100.0,
            fat_per_100g=1.0,
            carbs_per_100g=10.0,
            protein_per_100g=5.0,
            fiber_per_100g=0.5,
            sugar_per_100g=1.0,
            sodium_per_100g=20.0,
        )


def test_ground_stage_writes_results(tmp_path) -> None:
    settings = Settings(data_dir=tmp_path / "d", db_path=tmp_path / "d" / "n.db", usda_api_key="x")
    conn = connect(settings.db_path)
    migrate(conn)
    ph = "a" * 64
    conn.execute(
        """
        INSERT INTO photos(photo_hash, source_adapter, source_ref, local_blob_path, discovered_at)
        VALUES (?, 'local', 'x', 'y', 1)
        """,
        (ph,),
    )
    conn.execute(
        """
        INSERT INTO llm_results(photo_hash, model_id, raw_json, identification_json, confidence, created_at)
        VALUES (?, 'm', '{}', ?, 0.9, 1)
        """,
        (ph, json.dumps({"name": "oatmeal", "serving_size_g": 100.0, "serving_unit": "g", "serving_description": "bowl", "confidence": 0.9})),
    )
    conn.commit()

    ctx = StageContext(db=conn, settings=settings)
    run_stage(GroundStage(grounder=_FakeGrounder()), ctx, StageScope(photo_hashes=[ph]))
    row = conn.execute("SELECT source, fdc_id, scaled_json FROM grounding_results WHERE photo_hash=?", (ph,)).fetchone()
    assert row is not None
    assert str(row["source"]) == "usda_fdc"
    assert str(row["fdc_id"]) == "fdc-1"
    scaled = json.loads(str(row["scaled_json"]))
    assert abs(float(scaled["calories"]) - 100.0) < 1e-6
