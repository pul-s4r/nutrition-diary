from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from nutrition_diary.recognition.base import FoodRecognizer
from nutrition_diary.recognition.mock import MockRecognizer
from nutrition_diary.stages.base import Stage, StageContext, StageScope


@dataclass(frozen=True)
class RecognizeStage(Stage):
    name: str = "recognize"
    recognizer: FoodRecognizer | None = None

    def _get_recognizer(self, ctx: StageContext) -> FoodRecognizer:
        if self.recognizer is not None:
            return self.recognizer
        if ctx.settings.recognizer == "mock":
            return MockRecognizer()
        raise RuntimeError(
            f"Unsupported recognizer '{ctx.settings.recognizer}'. "
            "For now, set ND_RECOGNIZER=mock."
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
            SELECT p.local_blob_path, m.taken_at
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
            context={"taken_at": row["taken_at"], "photo_hash": item_key},
        )
        latency_ms = int((time.time() - started) * 1000)

        raw_json = json.dumps(analysis.to_dict(), sort_keys=True)
        ident = analysis.identification.to_dict() if analysis.identification else None

        now = int(time.time())
        ctx.db.execute(
            """
            INSERT INTO llm_results(
              photo_hash, model_id, raw_json, identification_json, confidence,
              tokens_in, tokens_out, cost_est, latency_ms, created_at
            )
            VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?)
            ON CONFLICT(photo_hash) DO UPDATE SET
              model_id=excluded.model_id,
              raw_json=excluded.raw_json,
              identification_json=excluded.identification_json,
              confidence=excluded.confidence,
              latency_ms=excluded.latency_ms,
              created_at=excluded.created_at
            """,
            (
                item_key,
                ctx.settings.bedrock_model_id,
                raw_json,
                None if ident is None else json.dumps(ident, sort_keys=True),
                analysis.meal_confidence,
                latency_ms,
                now,
            ),
        )
        return {
            "photo_hash": item_key,
            "model_id": ctx.settings.bedrock_model_id,
            "identification": ident,
            "meal_confidence": analysis.meal_confidence,
            "latency_ms": latency_ms,
        }

