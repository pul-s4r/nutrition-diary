from __future__ import annotations

import pytest

from nutrition_diary.schema.entry import DiaryEntry, FoodItem
from nutrition_diary.uploaders.mfp_mapper import (
    MFP_CSV_HEADER,
    diary_entry_to_csv_rows,
    meal_type_for_mfp,
    validate_entry_macros,
)


def test_meal_type_snack_maps_to_snacks() -> None:
    assert meal_type_for_mfp("Snack") == "Snacks"
    assert meal_type_for_mfp("Dinner") == "Dinner"


def test_validate_missing_macros_raises() -> None:
    entry = DiaryEntry(
        entry_id="e1",
        date="2026-04-19",
        meal_type="Lunch",
        items=[
            FoodItem(
                name="x",
                serving_size_g=100.0,
                serving_unit="g",
                serving_description="",
                llm_confidence=0.5,
                calories=None,
            )
        ],
        source_photos=[],
    )
    with pytest.raises(ValueError, match="missing calories"):
        validate_entry_macros(entry)


def test_diary_entry_to_csv_rows() -> None:
    entry = DiaryEntry(
        entry_id="e1",
        date="2026-04-19",
        meal_type="Snack",
        items=[
            FoodItem(
                name="Oats",
                serving_size_g=50.0,
                serving_unit="g",
                serving_description="bowl",
                llm_confidence=0.9,
                calories=200.0,
                fat_g=4.0,
                carbs_g=30.0,
                protein_g=8.0,
                fiber_g=5.0,
                sugar_g=1.0,
                sodium_mg=10.0,
            )
        ],
        source_photos=[],
    )
    rows = diary_entry_to_csv_rows(entry)
    assert len(rows) == 1
    assert rows[0][0] == "2026-04-19"
    assert rows[0][1] == "Snacks"
    assert rows[0][2] == "Oats"
    assert rows[0][-1] == "e1"
    assert len(MFP_CSV_HEADER) == len(rows[0])
