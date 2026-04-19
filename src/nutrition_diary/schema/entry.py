from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class FoodItem:
    # From LLM
    name: str
    serving_size_g: float
    serving_unit: str
    serving_description: str
    llm_confidence: float
    estimation_method: str = "llm"

    # From grounding (nullable)
    normalized_food_id: str | None = None
    grounding_source: str | None = None
    grounding_match_confidence: float | None = None

    calories: float | None = None
    fat_g: float | None = None
    carbs_g: float | None = None
    protein_g: float | None = None
    fiber_g: float | None = None
    sugar_g: float | None = None
    sodium_mg: float | None = None

    overall_confidence: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DiaryEntry:
    entry_id: str
    date: str  # YYYY-MM-DD
    meal_type: str
    items: list[FoodItem]
    source_photos: list[str]
    notes: str = ""
    overall_confidence: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "entry_id": self.entry_id,
            "date": self.date,
            "meal_type": self.meal_type,
            "items": [i.to_dict() for i in self.items],
            "source_photos": self.source_photos,
            "notes": self.notes,
            "overall_confidence": self.overall_confidence,
        }
