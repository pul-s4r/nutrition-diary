from __future__ import annotations

import re

SYNONYM_MAP: dict[str, str] = {
    "chips": "potato chips",
    "crisps": "potato chips",
    "aubergine": "eggplant",
    "courgette": "zucchini",
    "mince": "ground beef",
}


def normalize_food_name(name: str) -> str:
    s = name.strip().lower()
    s = re.sub(r"^(a|an|the|some)\s+", "", s)
    for k, v in SYNONYM_MAP.items():
        if re.search(rf"\b{re.escape(k)}\b", s):
            s = re.sub(rf"\b{re.escape(k)}\b", v, s)
    s = re.sub(r"\s+", " ", s).strip()
    return s
