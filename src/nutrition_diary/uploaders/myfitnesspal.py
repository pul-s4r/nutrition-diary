from __future__ import annotations

import csv
import http.cookiejar
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from nutrition_diary.config import Settings
from nutrition_diary.schema.entry import DiaryEntry
from nutrition_diary.uploaders.base import SubmitResult
from nutrition_diary.uploaders.mfp_mapper import MFP_CSV_HEADER, diary_entry_to_csv_rows


def _mfp_client_factory() -> type[Any]:
    try:
        from myfitnesspal import Client
    except ImportError as e:  # pragma: no cover - exercised when optional dep missing
        raise RuntimeError(
            "ND_MFP_MODE=live requires the third-party `myfitnesspal` package. "
            "Install it in your environment or use ND_MFP_MODE=csv."
        ) from e
    return Client


@contextmanager
def _plain_requests_session_for_mfp_client() -> Iterator[None]:
    """`myfitnesspal` uses cloudscraper by default; MFP often returns 403 on cookie auth with it."""
    import cloudscraper
    import requests

    orig = cloudscraper.create_scraper
    try:

        def _plain(sess: requests.Session | None = None, **kwargs: Any) -> requests.Session:
            return sess if sess is not None else requests.Session()

        cloudscraper.create_scraper = _plain  # type: ignore[method-assign]
        yield
    finally:
        cloudscraper.create_scraper = orig


@dataclass
class MyFitnessPalUploader:
    """MyFitnessPal target: CSV export for manual Premium import, with optional live session check."""

    name: str = "mfp"
    settings: Settings = field(default_factory=Settings)
    _client: Any | None = field(default=None, init=False, repr=False)

    def authenticate(self) -> None:
        if self.settings.mfp_mode != "live":
            self._client = None
            return
        self._client = self._build_client()

    @retry(
        retry=retry_if_exception_type((OSError, ConnectionError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        reraise=True,
    )
    def _verify_session(self, entry: DiaryEntry) -> None:
        assert self._client is not None
        y, m, d = (int(x) for x in entry.date.split("-"))
        self._client.get_date(y, m, d)

    def _build_client(self) -> Any:
        Client = _mfp_client_factory()
        if self.settings.mfp_cookie_path is not None:
            jar = http.cookiejar.MozillaCookieJar(str(self.settings.mfp_cookie_path))
            jar.load(ignore_discard=True, ignore_expires=True)
            with _plain_requests_session_for_mfp_client():
                return Client(cookiejar=jar)
        return Client()

    def submit_entry(self, entry: DiaryEntry) -> SubmitResult:
        try:
            rows = diary_entry_to_csv_rows(entry)
        except ValueError as e:
            return SubmitResult(success=False, error=str(e))

        if self.settings.mfp_mode == "live":
            try:
                if self._client is None:
                    self.authenticate()
                self._verify_session(entry)
            except Exception as e:  # noqa: BLE001
                return SubmitResult(success=False, error=f"MyFitnessPal session check failed: {e}")

        out_dir = Path(self.settings.mfp_csv_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        suffix = "_verified" if self.settings.mfp_mode == "live" else ""
        out_path = out_dir / f"{entry.date}{suffix}.csv"
        is_new = not out_path.exists()

        with out_path.open("a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if is_new:
                w.writerow(MFP_CSV_HEADER)
            for row in rows:
                w.writerow(row)

        return SubmitResult(success=True, external_id=str(out_path.resolve()))
