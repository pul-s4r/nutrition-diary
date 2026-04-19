from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Protocol


@dataclass(frozen=True)
class GroundingResult:
    normalized_food_id: str
    source: str
    matched_name: str
    match_confidence: float
    calories_per_100g: float
    fat_per_100g: float
    carbs_per_100g: float
    protein_per_100g: float
    fiber_per_100g: float
    sugar_per_100g: float
    sodium_per_100g: float

    def to_dict(self) -> dict:
        return asdict(self)


class NutritionGrounder(Protocol):
    def ground(self, food_name: str, serving_size_g: float) -> GroundingResult | None: ...
