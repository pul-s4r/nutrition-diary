from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from nutrition_diary.schema.entry import DiaryEntry
from nutrition_diary.uploaders.base import SubmitResult


@dataclass
class CsvExportUploader:
    name: str = "csv"
    out_dir: Path = Path("data/csv_exports")

    def authenticate(self) -> None:
        return

    def submit_entry(self, entry: DiaryEntry) -> SubmitResult:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        out_path = self.out_dir / f"{entry.date}.csv"
        is_new = not out_path.exists()

        with out_path.open("a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if is_new:
                w.writerow(
                    [
                        "entry_id",
                        "date",
                        "meal_type",
                        "item_name",
                        "serving_size_g",
                        "calories",
                        "fat_g",
                        "carbs_g",
                        "protein_g",
                        "llm_confidence",
                        "grounding_source",
                    ]
                )
            for item in entry.items:
                w.writerow(
                    [
                        entry.entry_id,
                        entry.date,
                        entry.meal_type,
                        item.name,
                        item.serving_size_g,
                        item.calories,
                        item.fat_g,
                        item.carbs_g,
                        item.protein_g,
                        item.llm_confidence,
                        item.grounding_source,
                    ]
                )
        return SubmitResult(success=True, external_id=str(out_path))

