from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Protocol


@dataclass(frozen=True)
class LLMFoodIdentification:
    name: str
    serving_size_g: float
    serving_unit: str
    serving_description: str
    confidence: float

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class LLMFoodAnalysisResult:
    identification: LLMFoodIdentification | None
    meal_confidence: float

    def to_dict(self) -> dict:
        return {
            "identification": None
            if self.identification is None
            else self.identification.to_dict(),
            "meal_confidence": self.meal_confidence,
        }


class FoodRecognizer(Protocol):
    def analyze(self, image_bytes: bytes, *, context: dict) -> LLMFoodAnalysisResult: ...
