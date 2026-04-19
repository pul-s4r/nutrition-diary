from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from nutrition_diary.config import Settings
from nutrition_diary.schema.entry import DiaryEntry, FoodItem
from nutrition_diary.uploaders.myfitnesspal import MyFitnessPalUploader


def test_csv_mode_writes_file(tmp_path: Path) -> None:
    settings = Settings(mfp_mode="csv", mfp_csv_dir=tmp_path / "out")
    up = MyFitnessPalUploader(settings=settings)
    up.authenticate()
    entry = DiaryEntry(
        entry_id="abc",
        date="2026-04-19",
        meal_type="Dinner",
        items=[
            FoodItem(
                name="Soup",
                serving_size_g=300.0,
                serving_unit="g",
                serving_description="bowl",
                llm_confidence=0.8,
                calories=150.0,
                fat_g=5.0,
                carbs_g=20.0,
                protein_g=6.0,
            )
        ],
        source_photos=[],
    )
    res = up.submit_entry(entry)
    assert res.success
    p = Path(res.external_id or "")
    assert p.exists()
    text = p.read_text(encoding="utf-8")
    assert "nd_entry_id" in text.splitlines()[0]
    assert "abc" in text


def test_appends_without_duplicate_header(tmp_path: Path) -> None:
    settings = Settings(mfp_mode="csv", mfp_csv_dir=tmp_path / "out")
    up = MyFitnessPalUploader(settings=settings)
    entry = DiaryEntry(
        entry_id="e1",
        date="2026-04-19",
        meal_type="Breakfast",
        items=[
            FoodItem(
                name="Toast",
                serving_size_g=60.0,
                serving_unit="g",
                serving_description="",
                llm_confidence=0.7,
                calories=180.0,
                fat_g=4.0,
                carbs_g=22.0,
                protein_g=5.0,
            )
        ],
        source_photos=[],
    )
    assert up.submit_entry(entry).success
    assert up.submit_entry(entry).success
    lines = (tmp_path / "out" / "2026-04-19.csv").read_text(encoding="utf-8").splitlines()
    assert sum(1 for ln in lines if ln.startswith("Date,")) == 1


@patch("nutrition_diary.uploaders.myfitnesspal._mfp_client_factory")
def test_live_mode_uses_verified_suffix(mock_factory: MagicMock, tmp_path: Path) -> None:
    mock_factory.return_value = MagicMock(return_value=MagicMock())
    settings = Settings(mfp_mode="live", mfp_csv_dir=tmp_path / "out")
    up = MyFitnessPalUploader(settings=settings)
    up.authenticate()
    entry = DiaryEntry(
        entry_id="live1",
        date="2026-05-01",
        meal_type="Lunch",
        items=[
            FoodItem(
                name="Salad",
                serving_size_g=200.0,
                serving_unit="g",
                serving_description="",
                llm_confidence=0.9,
                calories=120.0,
                fat_g=4.0,
                carbs_g=10.0,
                protein_g=5.0,
            )
        ],
        source_photos=[],
    )
    res = up.submit_entry(entry)
    assert res.success
    assert "2026-05-01_verified.csv" in (res.external_id or "")
