from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, time
from typing import Iterable


@dataclass(frozen=True)
class MealWindows:
    breakfast_start: time
    breakfast_end: time
    lunch_start: time
    lunch_end: time
    dinner_start: time
    dinner_end: time


def meal_type_for(t: time, w: MealWindows) -> str:
    if w.breakfast_start <= t <= w.breakfast_end:
        return "Breakfast"
    if w.lunch_start <= t <= w.lunch_end:
        return "Lunch"
    if w.dinner_start <= t <= w.dinner_end:
        return "Dinner"
    return "Snack"


def deterministic_cluster_id(date: str, meal_type: str, photo_hashes: Iterable[str]) -> str:
    h = hashlib.sha256()
    h.update(date.encode("utf-8"))
    h.update(b"\0")
    h.update(meal_type.encode("utf-8"))
    h.update(b"\0")
    for ph in sorted(photo_hashes):
        h.update(ph.encode("utf-8"))
        h.update(b"\0")
    return h.hexdigest()


def parse_taken_at(iso: str) -> datetime:
    # stored as datetime.isoformat() without timezone in v1
    return datetime.fromisoformat(iso)

