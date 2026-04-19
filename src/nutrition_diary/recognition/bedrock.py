from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel, Field, ValidationError
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from nutrition_diary.config import Settings
from nutrition_diary.recognition.base import (
    LLMFoodAnalysisResult,
    LLMFoodIdentification,
)
from nutrition_diary.recognition.preprocess import preprocess_to_b64_jpeg
from nutrition_diary.recognition.prompts import (
    FOOD_ANALYSIS_SIMPLIFIED_SYSTEM,
    FOOD_ANALYSIS_SYSTEM_PROMPT,
    FOOD_ANALYSIS_USER_TEMPLATE,
)

logger = logging.getLogger(__name__)


class _IdentModel(BaseModel):
    name: str
    serving_size_g: float
    serving_unit: str = "g"
    serving_description: str = ""
    confidence: float = Field(ge=0.0, le=1.0)


class _AnalysisModel(BaseModel):
    identification: _IdentModel | None = None
    meal_confidence: float = Field(ge=0.0, le=1.0, default=0.0)


def _retryable(exc: BaseException) -> bool:
    try:
        from botocore.exceptions import ClientError

        if isinstance(exc, ClientError):
            code = exc.response.get("Error", {}).get("Code", "")
            return code in (
                "ThrottlingException",
                "TooManyRequestsException",
                "ServiceUnavailableException",
            )
    except Exception:  # noqa: BLE001
        pass
    return isinstance(exc, TimeoutError)


def _extract_usage(raw: Any) -> tuple[int | None, int | None]:
    if raw is None:
        return None, None
    meta = getattr(raw, "response_metadata", None) or {}
    usage = meta.get("usage") or {}
    tin = usage.get("input_tokens")
    if tin is None:
        tin = usage.get("inputTokens")
    tout = usage.get("output_tokens")
    if tout is None:
        tout = usage.get("outputTokens")
    return (
        int(tin) if tin is not None else None,
        int(tout) if tout is not None else None,
    )


def _to_result(parsed: _AnalysisModel) -> LLMFoodAnalysisResult:
    if parsed.identification is None:
        return LLMFoodAnalysisResult(identification=None, meal_confidence=0.0)
    i = parsed.identification
    ident = LLMFoodIdentification(
        name=i.name,
        serving_size_g=float(i.serving_size_g),
        serving_unit=i.serving_unit or "g",
        serving_description=i.serving_description or "",
        confidence=float(i.confidence),
    )
    meal_conf = float(parsed.meal_confidence) if parsed.meal_confidence else ident.confidence
    return LLMFoodAnalysisResult(identification=ident, meal_confidence=meal_conf)


class BedrockRecognizer:
    """Multimodal Bedrock recognizer (composite plate). Requires `pip install nutrition-diary[aws]`."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self.last_input_tokens: int | None = None
        self.last_output_tokens: int | None = None
        self.last_model_id: str = settings.bedrock_model_id

    def _build_query(self, context: dict) -> str:
        taken = context.get("taken_at")
        meal = context.get("meal_type")
        parts: list[str] = []
        if meal:
            parts.append(f"This is a {meal} photo")
        if taken:
            parts.append(f"taken at {taken} local time")
        if parts:
            return FOOD_ANALYSIS_USER_TEMPLATE.format(meal_context=" ".join(parts) + ". ")
        return FOOD_ANALYSIS_USER_TEMPLATE.format(meal_context="")

    def _make_chain(self, model_id: str, *, simplified_system: bool):
        from langchain_aws import ChatBedrockConverse
        from langchain_core.prompts import ChatPromptTemplate

        system = FOOD_ANALYSIS_SIMPLIFIED_SYSTEM if simplified_system else FOOD_ANALYSIS_SYSTEM_PROMPT
        prompt = ChatPromptTemplate.from_messages(
            [
                ("system", "{system}"),
                (
                    "human",
                    [
                        {
                            "type": "image_url",
                            "image_url": {"url": "data:image/jpeg;base64,{image_b64}"},
                        },
                        {"type": "text", "text": "{query}"},
                    ],
                ),
            ]
        )
        llm = ChatBedrockConverse(model=model_id, region_name=self._settings.bedrock_region)
        try:
            structured = llm.with_structured_output(_AnalysisModel, include_raw=True)
        except TypeError:
            structured = llm.with_structured_output(_AnalysisModel)
        return prompt.partial(system=system) | structured

    @retry(
        retry=retry_if_exception(_retryable),
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=1, max=16),
        reraise=True,
    )
    def _invoke_chain(self, chain: Any, inputs: dict) -> Any:
        return chain.invoke(inputs)

    def _parse_invoke_result(self, out: Any) -> _AnalysisModel:
        if isinstance(out, _AnalysisModel):
            return out
        if isinstance(out, dict) and "parsed" in out:
            raw = out.get("raw")
            tin, tout = _extract_usage(raw)
            self.last_input_tokens = tin
            self.last_output_tokens = tout
            if raw is not None:
                meta = getattr(raw, "response_metadata", None) or {}
                mid = meta.get("model_id") or meta.get("model")
                if mid:
                    self.last_model_id = str(mid)
            parsed = out.get("parsed")
            if isinstance(parsed, _AnalysisModel):
                return parsed
        raise TypeError(f"Unexpected structured output type: {type(out)}")

    def analyze(self, image_bytes: bytes, *, context: dict) -> LLMFoodAnalysisResult:
        self.last_input_tokens = None
        self.last_output_tokens = None
        self.last_model_id = self._settings.bedrock_model_id

        prep = preprocess_to_b64_jpeg(image_bytes)
        query = self._build_query(context)
        base_inputs = {"image_b64": prep.jpeg_b64, "query": query}

        chain = self._make_chain(self._settings.bedrock_model_id, simplified_system=False)
        if self._settings.bedrock_fallback_model_id:
            chain = chain.with_fallbacks(
                [
                    self._make_chain(
                        self._settings.bedrock_fallback_model_id,
                        simplified_system=False,
                    )
                ]
            )

        try:
            out = self._invoke_chain(chain, base_inputs)
            parsed = self._parse_invoke_result(out)
            return _to_result(parsed)
        except ValidationError as e:
            logger.warning("Bedrock structured output validation failed: %s", e)
        except Exception as e:  # noqa: BLE001
            logger.warning("Bedrock chain failed: %s", e)
            return LLMFoodAnalysisResult(identification=None, meal_confidence=0.0)

        try:
            simple = self._make_chain(self._settings.bedrock_model_id, simplified_system=True)
            out = self._invoke_chain(simple, base_inputs)
            parsed = self._parse_invoke_result(out)
            return _to_result(parsed)
        except Exception as e2:  # noqa: BLE001
            logger.warning("Simplified Bedrock prompt failed: %s", e2)
            return LLMFoodAnalysisResult(identification=None, meal_confidence=0.0)
