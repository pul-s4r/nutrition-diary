from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from nutrition_diary.config import Settings
from nutrition_diary.recognition.base import FoodRecognizer
from nutrition_diary.recognition.bedrock import BedrockRecognizer
from nutrition_diary.recognition.mock import MockRecognizer
from nutrition_diary.stages.base import Stage, StageContext, StageScope


def _bedrock_cost_estimate(settings: Settings, tin: int | None, tout: int | None) -> float | None:
    if tin is None or tout is None:
        return None
    return (
        tin * settings.bedrock_input_price_per_mtok + tout * settings.bedrock_output_price_per_mtok
    ) / 1_000_000.0


@dataclass(frozen=True)
class RecognizeStage(Stage):
    name: str = "recognize"
    recognizer: FoodRecognizer | None = None

    def _get_recognizer(self, ctx: StageContext) -> FoodRecognizer:
        if self.recognizer is not None:
            return self.recognizer
        if ctx.settings.recognizer == "mock":
            return MockRecognizer()
        if ctx.settings.recognizer == "bedrock":
            return BedrockRecognizer(ctx.settings)
        raise RuntimeError(
            f"Unsupported recognizer '{ctx.settings.recognizer}'. "
            "Use ND_RECOGNIZER=mock or ND_RECOGNIZER=bedrock (requires nutrition-diary[aws])."
        )

    def select_work(self, ctx: StageContext, scope: StageScope) -> Iterable[str]:
        if scope.photo_hashes:
            yield from scope.photo_hashes
            return
        rows = ctx.db.execute("SELECT photo_hash FROM photos").fetchall()
        for r in rows:
            yield str(r["photo_hash"])

    def run_one(self, ctx: StageContext, item_key: str) -> dict | None:
        row = ctx.db.execute(
            """
            SELECT p.local_blob_path, m.taken_at,
              (SELECT c.meal_type FROM meal_photos mp
               JOIN meal_clusters c ON c.cluster_id = mp.cluster_id
               WHERE mp.photo_hash = p.photo_hash LIMIT 1) AS meal_type
            FROM photos p
            LEFT JOIN photo_metadata m ON m.photo_hash=p.photo_hash
            WHERE p.photo_hash=?
            """,
            (item_key,),
        ).fetchone()
        if row is None:
            raise KeyError(f"photo_hash not found: {item_key}")
        blob_path = Path(str(row["local_blob_path"]))
        image_bytes = blob_path.read_bytes()

        recognizer = self._get_recognizer(ctx)
        started = time.time()
        analysis = recognizer.analyze(
            image_bytes,
            context={
                "taken_at": row["taken_at"],
                "photo_hash": item_key,
                "meal_type": row["meal_type"],
            },
        )
        latency_ms = int((time.time() - started) * 1000)

        raw_json = json.dumps(analysis.to_dict(), sort_keys=True)
        ident = analysis.identification.to_dict() if analysis.identification else None

        model_id = ctx.settings.bedrock_model_id
        tokens_in: int | None = None
        tokens_out: int | None = None
        cost_est: float | None = None
        if hasattr(recognizer, "last_model_id"):
            model_id = str(getattr(recognizer, "last_model_id") or model_id)
        if hasattr(recognizer, "last_input_tokens"):
            tokens_in = getattr(recognizer, "last_input_tokens")
        if hasattr(recognizer, "last_output_tokens"):
            tokens_out = getattr(recognizer, "last_output_tokens")
        if ctx.settings.recognizer == "bedrock":
            cost_est = _bedrock_cost_estimate(ctx.settings, tokens_in, tokens_out)

        now = int(time.time())
        ctx.db.execute(
            """
            INSERT INTO llm_results(
              photo_hash, model_id, raw_json, identification_json, confidence,
              tokens_in, tokens_out, cost_est, latency_ms, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(photo_hash) DO UPDATE SET
              model_id=excluded.model_id,
              raw_json=excluded.raw_json,
              identification_json=excluded.identification_json,
              confidence=excluded.confidence,
              tokens_in=excluded.tokens_in,
              tokens_out=excluded.tokens_out,
              cost_est=excluded.cost_est,
              latency_ms=excluded.latency_ms,
              created_at=excluded.created_at
            """,
            (
                item_key,
                model_id,
                raw_json,
                None if ident is None else json.dumps(ident, sort_keys=True),
                analysis.meal_confidence,
                tokens_in,
                tokens_out,
                cost_est,
                latency_ms,
                now,
            ),
        )
        return {
            "photo_hash": item_key,
            "model_id": model_id,
            "identification": ident,
            "meal_confidence": analysis.meal_confidence,
            "latency_ms": latency_ms,
            "tokens_in": tokens_in,
            "tokens_out": tokens_out,
            "cost_est": cost_est,
        }
