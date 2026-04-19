from __future__ import annotations

import io
from pathlib import Path

from PIL import Image

from nutrition_diary.config import Settings
from nutrition_diary.db import connect, migrate
from nutrition_diary.recognition.base import LLMFoodAnalysisResult, LLMFoodIdentification
from nutrition_diary.stages import (
    RecognizeStage,
    StageContext,
    StageScope,
    run_stage,
)


class _FakeBedrockLike:
    last_input_tokens = 100
    last_output_tokens = 20
    last_model_id = "us.test-model-v1:0"

    def analyze(self, image_bytes: bytes, *, context: dict) -> LLMFoodAnalysisResult:
        return LLMFoodAnalysisResult(
            identification=LLMFoodIdentification(
                name="test meal",
                serving_size_g=300.0,
                serving_unit="g",
                serving_description="1 plate",
                confidence=0.9,
            ),
            meal_confidence=0.9,
        )


def test_recognize_stage_persists_tokens_and_cost(tmp_path: Path) -> None:
    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    img_path = photos_dir / "meal.jpg"
    Image.new("RGB", (64, 64), (10, 200, 10)).save(img_path, format="JPEG")

    settings = Settings(
        data_dir=tmp_path / "data",
        db_path=tmp_path / "data" / "nutrition.db",
        exports_dir=tmp_path / "data" / "exports",
        stages_dump_dir=tmp_path / "data" / "stages",
        recognizer="bedrock",
        bedrock_input_price_per_mtok=3.0,
        bedrock_output_price_per_mtok=15.0,
    )
    db = connect(settings.db_path)
    migrate(db)

    ph = "deadbeef" * 8
    blob = tmp_path / "data" / "blobs" / f"{ph}.jpg"
    blob.parent.mkdir(parents=True, exist_ok=True)
    blob.write_bytes(img_path.read_bytes())
    db.execute(
        """
        INSERT INTO photos(photo_hash, source_adapter, source_ref, local_blob_path, discovered_at)
        VALUES (?, 'local', ?, ?, 1)
        """,
        (ph, str(img_path), str(blob)),
    )
    db.execute(
        "INSERT INTO photo_metadata(photo_hash, taken_at) VALUES (?, ?)",
        (ph, "2026-04-19T12:00:00"),
    )
    db.commit()

    ctx = StageContext(db=db, settings=settings)
    run_stage(RecognizeStage(recognizer=_FakeBedrockLike()), ctx, StageScope(photo_hashes=[ph]))

    row = db.execute("SELECT model_id, tokens_in, tokens_out, cost_est FROM llm_results WHERE photo_hash=?", (ph,)).fetchone()
    assert row is not None
    assert str(row["model_id"]) == "us.test-model-v1:0"
    assert int(row["tokens_in"]) == 100
    assert int(row["tokens_out"]) == 20
    expected = (100 * 3.0 + 20 * 15.0) / 1_000_000.0
    assert abs(float(row["cost_est"]) - expected) < 1e-9
