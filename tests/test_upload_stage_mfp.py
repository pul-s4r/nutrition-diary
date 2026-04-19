from __future__ import annotations

from dataclasses import dataclass

from nutrition_diary.config import Settings
from nutrition_diary.db import connect, migrate
from nutrition_diary.schema.entry import DiaryEntry
from nutrition_diary.uploaders.base import SubmitResult
from nutrition_diary.stages import StageContext, StageScope, UploadStage, run_stage


@dataclass
class _FakeMFP:
    name: str = "mfp"

    def authenticate(self) -> None:
        return

    def submit_entry(self, entry: DiaryEntry) -> SubmitResult:
        return SubmitResult(success=True, external_id="fake://ok")


def test_upload_stage_mfp_success_updates_queue(tmp_path) -> None:
    settings = Settings(data_dir=tmp_path / "d", db_path=tmp_path / "d" / "n.db")
    db = connect(settings.db_path)
    migrate(db)
    db.execute(
        """
        INSERT INTO meal_clusters(cluster_id, date, meal_type, earliest_taken_at, latest_taken_at)
        VALUES ('c1', '2026-04-19', 'Dinner', '2026-04-19T18:00:00', '2026-04-19T18:05:00')
        """
    )
    db.execute(
        """
        INSERT INTO diary_entries(entry_id, date, meal_type, items_json, overall_confidence, source_cluster_id, approved)
        VALUES (?, '2026-04-19', 'Dinner', ?, 0.9, 'c1', 1)
        """,
        (
            "ent1",
            __import__("json").dumps(
                [
                    {
                        "name": "Rice",
                        "serving_size_g": 150.0,
                        "serving_unit": "g",
                        "serving_description": "",
                        "llm_confidence": 0.8,
                        "calories": 200.0,
                        "fat_g": 1.0,
                        "carbs_g": 40.0,
                        "protein_g": 4.0,
                    }
                ]
            ),
        ),
    )
    now = 1
    db.execute(
        """
        INSERT INTO upload_queue(entry_id, target, status, attempts, last_error, updated_at)
        VALUES (?, 'mfp', 'pending', 0, NULL, ?)
        """,
        ("ent1", now),
    )
    db.commit()

    ctx = StageContext(db=db, settings=settings)
    run_stage(UploadStage(target="mfp", uploader=_FakeMFP()), ctx, StageScope(entry_id="ent1"))
    row = db.execute(
        "SELECT status, last_error FROM upload_queue WHERE entry_id=? AND target='mfp'", ("ent1",)
    ).fetchone()
    assert str(row["status"]) == "success"
    assert row["last_error"] is None
