from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from nutrition_diary.schema.entry import DiaryEntry


@dataclass(frozen=True)
class SubmitResult:
    success: bool
    external_id: str | None = None
    error: str | None = None


class DiaryUploader(Protocol):
    name: str

    def authenticate(self) -> None: ...

    def submit_entry(self, entry: DiaryEntry) -> SubmitResult: ...

