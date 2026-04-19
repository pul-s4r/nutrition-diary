from __future__ import annotations

from nutrition_diary.schema.entry import DiaryEntry, FoodItem

MFP_CSV_HEADER: list[str] = [
    "Date",
    "Meal",
    "Food",
    "Calories",
    "Fat (g)",
    "Carbohydrates (g)",
    "Protein (g)",
    "Fiber (g)",
    "Sodium (mg)",
    "Sugars (g)",
    "Serving",
    "nd_entry_id",
]


def meal_type_for_mfp(meal_type: str) -> str:
    m = meal_type.strip()
    if m == "Snack":
        return "Snacks"
    return m


def validate_entry_macros(entry: DiaryEntry) -> None:
    for idx, item in enumerate(entry.items):
        if item.calories is None:
            raise ValueError(
                f"Item {idx} ({item.name!r}) is missing calories; complete grounding before MFP upload."
            )


def diary_entry_to_csv_rows(entry: DiaryEntry) -> list[list[str]]:
    """One CSV row per FoodItem, suitable for manual Premium CSV import."""
    validate_entry_macros(entry)
    meal = meal_type_for_mfp(entry.meal_type)
    rows: list[list[str]] = []
    for item in entry.items:
        rows.append(
            [
                entry.date,
                meal,
                item.name,
                _fmt(item.calories),
                _fmt(item.fat_g),
                _fmt(item.carbs_g),
                _fmt(item.protein_g),
                _fmt(item.fiber_g),
                _fmt(item.sodium_mg),
                _fmt(item.sugar_g),
                f"{item.serving_size_g:g} {item.serving_unit}".strip(),
                entry.entry_id,
            ]
        )
    return rows


def _fmt(val: float | None) -> str:
    if val is None:
        return ""
    return f"{val:g}"
