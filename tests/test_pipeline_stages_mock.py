from __future__ import annotations

from pathlib import Path

from PIL import Image

from nutrition_diary.config import Settings
from nutrition_diary.db import connect, migrate
from nutrition_diary.sources.local import LocalPhotoSource
from nutrition_diary.stages import (
    AssembleStage,
    ClusterStage,
    ExportStage,
    GroundStage,
    MetadataStage,
    RecognizeStage,
    SourceStage,
    StageContext,
    StageScope,
    UploadStage,
    run_stage,
)


def test_end_to_end_mock(tmp_path: Path) -> None:
    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    img_path = photos_dir / "meal.jpg"
    Image.new("RGB", (32, 32), (200, 10, 10)).save(img_path, format="JPEG")

    settings = Settings(
        data_dir=tmp_path / "data",
        db_path=tmp_path / "data" / "nutrition.db",
        exports_dir=tmp_path / "data" / "exports",
        stages_dump_dir=tmp_path / "data" / "stages",
        recognizer="mock",
    )

    db = connect(settings.db_path)
    migrate(db)
    ctx = StageContext(db=db, settings=settings, dump_dir=settings.stages_dump_dir)
    scope = StageScope()

    run_stage(SourceStage(source=LocalPhotoSource(), root=photos_dir), ctx, scope)
    run_stage(MetadataStage(), ctx, scope)
    run_stage(ClusterStage(), ctx, scope)
    run_stage(RecognizeStage(), ctx, scope)
    run_stage(GroundStage(), ctx, scope)
    run_stage(AssembleStage(), ctx, scope)
    run_stage(ExportStage(target="csv"), ctx, scope)

    export_files = list(settings.exports_dir.glob("*.jsonl"))
    assert export_files, "expected at least one export JSONL"

    # Upload drains pending queue
    run_stage(UploadStage(target="csv"), ctx, scope)
    pending = db.execute("SELECT COUNT(*) AS n FROM upload_queue WHERE status='pending'").fetchone()
    assert int(pending["n"]) == 0

    # Re-export with --force resets upload_queue to pending so upload can run again
    ctx_force = StageContext(db=db, settings=settings, dump_dir=None, force=True, dry_run=False)
    run_stage(ExportStage(target="csv"), ctx_force, scope)
    pending_after = db.execute(
        "SELECT COUNT(*) AS n FROM upload_queue WHERE status='pending' AND target='csv'"
    ).fetchone()
    assert int(pending_after["n"]) >= 1

