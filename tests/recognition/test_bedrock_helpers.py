from __future__ import annotations

import pytest
from pydantic import ValidationError

from nutrition_diary.config import Settings
from nutrition_diary.recognition.bedrock import (
    BedrockRecognizer,
    StructuredOutputParseError,
    _AnalysisModel,
    _to_result,
)


def test_to_result_preserves_explicit_zero_meal_confidence() -> None:
    m = _AnalysisModel.model_validate(
        {
            "identification": {
                "name": "plate",
                "serving_size_g": 400.0,
                "confidence": 0.9,
            },
            "meal_confidence": 0.0,
        }
    )
    assert _to_result(m).meal_confidence == 0.0


def test_to_result_omitted_meal_confidence_falls_back_to_ident_confidence() -> None:
    m = _AnalysisModel.model_validate(
        {
            "identification": {
                "name": "plate",
                "serving_size_g": 400.0,
                "confidence": 0.72,
            },
        }
    )
    assert m.meal_confidence is None
    assert _to_result(m).meal_confidence == 0.72


def test_parse_invoke_result_none_parsed_raises_structured_output_parse_error() -> None:
    r = BedrockRecognizer(Settings())
    with pytest.raises(StructuredOutputParseError):
        r._parse_invoke_result(
            {"parsed": None, "parsing_error": ValueError("bad json"), "raw": None}
        )


def test_parse_invoke_result_re_raises_pydantic_validation_error() -> None:
    r = BedrockRecognizer(Settings())
    ve = ValidationError.from_exception_data(
        "X",
        [{"type": "missing", "loc": ("f",), "msg": "required", "input": {}}],
    )
    with pytest.raises(ValidationError):
        r._parse_invoke_result({"parsed": None, "parsing_error": ve, "raw": None})
