from __future__ import annotations

import hashlib

from nutrition_diary.recognition.base import LLMFoodAnalysisResult, LLMFoodIdentification


class MockRecognizer:
    """
    Deterministic recognizer for offline development/testing.
    Produces a stable composite name based on image bytes hash prefix.
    """

    def analyze(self, image_bytes: bytes, *, context: dict) -> LLMFoodAnalysisResult:
        h = hashlib.sha256(image_bytes).hexdigest()[:8]
        ident = LLMFoodIdentification(
            name=f"composite meal ({h})",
            serving_size_g=450.0,
            serving_unit="g",
            serving_description="1 dinner plate",
            confidence=0.5,
        )
        return LLMFoodAnalysisResult(identification=ident, meal_confidence=ident.confidence)

