from __future__ import annotations

import difflib
import json
import sqlite3
import time
from collections import defaultdict
from typing import Any

import requests
from ratelimit import limits, sleep_and_retry
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from nutrition_diary.config import Settings
from nutrition_diary.grounding.base import GroundingResult
from nutrition_diary.grounding.cache import UsdaCache
from nutrition_diary.grounding.synonyms import normalize_food_name

_BASE = "https://api.nal.usda.gov/fdc/v1"

_NUTRIENT_IDS = {
    1008: "calories_per_100g",
    208: "calories_per_100g",
    957521: "calories_per_100g",
    1004: "fat_per_100g",
    1005: "carbs_per_100g",
    1003: "protein_per_100g",
    1079: "fiber_per_100g",
    2000: "sugar_per_100g",
    1093: "sodium_per_100g",
}


def _nutrient_amounts(food: dict[str, Any]) -> dict[str, float]:
    acc: dict[str, list[float]] = defaultdict(list)
    for fn in food.get("foodNutrients") or []:
        nid = fn.get("nutrientId")
        if nid is None:
            nutrient = fn.get("nutrient") or {}
            nid = nutrient.get("id")
        if nid is None:
            continue
        try:
            nid_int = int(nid)
        except (TypeError, ValueError):
            continue
        key = _NUTRIENT_IDS.get(nid_int)
        if not key:
            continue
        amt = fn.get("amount")
        if amt is None:
            nutrient = fn.get("nutrient") or {}
            amt = nutrient.get("amount")
        if amt is None:
            continue
        try:
            val = float(amt)
        except (TypeError, ValueError):
            continue
        acc[key].append(val)
    keys = sorted(set(_NUTRIENT_IDS.values()))
    out: dict[str, float] = {}
    for k in keys:
        vals = acc.get(k) or []
        out[k] = max(vals) if vals else 0.0
    return out


def _scaled_macros(res: GroundingResult, serving_size_g: float) -> dict[str, float]:
    f = serving_size_g / 100.0
    return {
        "calories": res.calories_per_100g * f,
        "fat_g": res.fat_per_100g * f,
        "carbs_g": res.carbs_per_100g * f,
        "protein_g": res.protein_per_100g * f,
        "fiber_g": res.fiber_per_100g * f,
        "sugar_g": res.sugar_per_100g * f,
        "sodium_mg": res.sodium_per_100g * f,
    }


@sleep_and_retry
@limits(calls=900, period=3600)
def _limited_post(url: str, *, params: dict[str, str], json_body: dict, timeout: int = 60) -> requests.Response:
    return requests.post(url, params=params, json=json_body, timeout=timeout)


@sleep_and_retry
@limits(calls=900, period=3600)
def _limited_get(url: str, *, params: dict[str, str], timeout: int = 60) -> requests.Response:
    return requests.get(url, params=params, timeout=timeout)


def _is_retry_http(exc: BaseException) -> bool:
    if isinstance(exc, requests.HTTPError):
        return exc.response.status_code in (429, 500, 502, 503, 504)
    return isinstance(exc, requests.Timeout)


@retry(
    retry=retry_if_exception(_is_retry_http),
    stop=stop_after_attempt(6),
    wait=wait_exponential(multiplier=1, min=1, max=32),
    reraise=True,
)
def _post_json(url: str, *, params: dict[str, str], json_body: dict) -> dict[str, Any]:
    r = _limited_post(url, params=params, json_body=json_body)
    if r.status_code in (429, 500, 502, 503, 504):
        r.raise_for_status()
    r.raise_for_status()
    return r.json()


@retry(
    retry=retry_if_exception(_is_retry_http),
    stop=stop_after_attempt(6),
    wait=wait_exponential(multiplier=1, min=1, max=32),
    reraise=True,
)
def _get_json(url: str, *, params: dict[str, str]) -> dict[str, Any]:
    r = _limited_get(url, params=params)
    if r.status_code in (429, 500, 502, 503, 504):
        r.raise_for_status()
    r.raise_for_status()
    return r.json()


class UsdaGrounder:
    """USDA FoodData Central implementation of NutritionGrounder."""

    def __init__(self, settings: Settings, conn: sqlite3.Connection) -> None:
        self._settings = settings
        self._conn = conn
        self._cache = UsdaCache(conn)

    def ground(self, food_name: str, serving_size_g: float) -> GroundingResult | None:
        if not self._settings.usda_api_key:
            return None

        now = int(time.time())
        key = normalize_food_name(food_name)
        row = self._cache.get_row(key, now)
        if row is not None:
            if self._cache.is_negative_hit(row):
                return None
            nutrients = json.loads(str(row["nutrients_json"]))
            return GroundingResult(
                normalized_food_id=str(row["fdc_id"]),
                source="usda_fdc",
                matched_name=str(row["matched_name"]),
                match_confidence=float(row["match_confidence"]),
                calories_per_100g=float(nutrients["calories_per_100g"]),
                fat_per_100g=float(nutrients["fat_per_100g"]),
                carbs_per_100g=float(nutrients["carbs_per_100g"]),
                protein_per_100g=float(nutrients["protein_per_100g"]),
                fiber_per_100g=float(nutrients["fiber_per_100g"]),
                sugar_per_100g=float(nutrients["sugar_per_100g"]),
                sodium_per_100g=float(nutrients["sodium_per_100g"]),
            )

        params = {"api_key": str(self._settings.usda_api_key)}
        body: dict[str, Any] = {
            "query": key,
            "dataType": ["Foundation", "SR Legacy", "Survey (FNDDS)", "Branded"],
            "pageSize": 5,
            "sortBy": "score",
            "sortOrder": "desc",
        }
        search = _post_json(f"{_BASE}/foods/search", params=params, json_body=body)
        foods = search.get("foods") or []
        best_score = 0.0
        best: dict[str, Any] | None = None
        for f in foods:
            desc = str(f.get("description") or "")
            score = difflib.SequenceMatcher(None, key, desc.lower()).ratio()
            if score > best_score:
                best_score = score
                best = f
        if best is None or best_score < float(self._settings.usda_match_threshold):
            neg_ttl = max(1, int(self._settings.usda_negative_cache_ttl_days)) * 86400
            self._cache.put_miss(key, now_ts=now, ttl_seconds=neg_ttl)
            return None

        fdc_id = str(best.get("fdcId"))
        data_type = str(best.get("dataType") or "")
        matched_name = str(best.get("description") or "")

        detail = _get_json(f"{_BASE}/food/{fdc_id}", params=params)
        nmap = _nutrient_amounts(detail)
        res = GroundingResult(
            normalized_food_id=fdc_id,
            source="usda_fdc",
            matched_name=matched_name,
            match_confidence=float(best_score),
            calories_per_100g=float(nmap["calories_per_100g"]),
            fat_per_100g=float(nmap["fat_per_100g"]),
            carbs_per_100g=float(nmap["carbs_per_100g"]),
            protein_per_100g=float(nmap["protein_per_100g"]),
            fiber_per_100g=float(nmap["fiber_per_100g"]),
            sugar_per_100g=float(nmap["sugar_per_100g"]),
            sodium_per_100g=float(nmap["sodium_per_100g"]),
        )

        pos_ttl: int | None
        if self._settings.usda_cache_ttl_days <= 0:
            pos_ttl = None
        else:
            pos_ttl = int(self._settings.usda_cache_ttl_days) * 86400

        self._cache.put_positive(
            key,
            fdc_id=fdc_id,
            matched_name=matched_name,
            match_confidence=float(best_score),
            data_type=data_type,
            nutrients_json=json.dumps(nmap, sort_keys=True),
            now_ts=now,
            ttl_seconds=pos_ttl,
        )
        return res

    @staticmethod
    def scaled_payload(result: GroundingResult, serving_size_g: float) -> dict[str, float]:
        return _scaled_macros(result, serving_size_g)
