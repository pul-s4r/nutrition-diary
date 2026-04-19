from __future__ import annotations

import json
import sqlite3
from typing import Any


class UsdaCache:
    """Disk-persistent TTL cache backed by the shared `usda_cache` table."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def get_row(self, cache_key: str, now_ts: int) -> dict[str, Any] | None:
        row = self._conn.execute(
            """
            SELECT cache_key, fdc_id, matched_name, match_confidence, data_type, nutrients_json,
                   cached_at, ttl_seconds
            FROM usda_cache
            WHERE cache_key=?
            """,
            (cache_key,),
        ).fetchone()
        if row is None:
            return None
        cached_at = int(row["cached_at"])
        ttl = row["ttl_seconds"]
        if ttl is not None and cached_at + int(ttl) < now_ts:
            return None
        return dict(row)

    def put_positive(
        self,
        cache_key: str,
        *,
        fdc_id: str,
        matched_name: str,
        match_confidence: float,
        data_type: str,
        nutrients_json: str,
        now_ts: int,
        ttl_seconds: int | None,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO usda_cache(
              cache_key, fdc_id, matched_name, match_confidence, data_type, nutrients_json,
              cached_at, ttl_seconds
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(cache_key) DO UPDATE SET
              fdc_id=excluded.fdc_id,
              matched_name=excluded.matched_name,
              match_confidence=excluded.match_confidence,
              data_type=excluded.data_type,
              nutrients_json=excluded.nutrients_json,
              cached_at=excluded.cached_at,
              ttl_seconds=excluded.ttl_seconds
            """,
            (
                cache_key,
                fdc_id,
                matched_name,
                match_confidence,
                data_type,
                nutrients_json,
                now_ts,
                ttl_seconds,
            ),
        )

    def put_miss(self, cache_key: str, *, now_ts: int, ttl_seconds: int) -> None:
        miss_json = json.dumps({"_miss": True}, sort_keys=True)
        self._conn.execute(
            """
            INSERT INTO usda_cache(
              cache_key, fdc_id, matched_name, match_confidence, data_type, nutrients_json,
              cached_at, ttl_seconds
            )
            VALUES (?, NULL, '', 0.0, 'miss', ?, ?, ?)
            ON CONFLICT(cache_key) DO UPDATE SET
              fdc_id=NULL,
              matched_name='',
              match_confidence=0.0,
              data_type='miss',
              nutrients_json=excluded.nutrients_json,
              cached_at=excluded.cached_at,
              ttl_seconds=excluded.ttl_seconds
            """,
            (cache_key, miss_json, now_ts, ttl_seconds),
        )

    def is_negative_hit(self, row: dict[str, Any]) -> bool:
        if str(row.get("data_type") or "") == "miss":
            return True
        try:
            blob = json.loads(str(row["nutrients_json"]))
        except Exception:  # noqa: BLE001
            return False
        return bool(blob.get("_miss"))
