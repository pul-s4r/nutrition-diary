from __future__ import annotations

import io
import mimetypes
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from nutrition_diary.config import Settings
from nutrition_diary.sources.base import PhotoSource

_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".heic", ".webp"}
_FOLDER_MIME = "application/vnd.google-apps.folder"


def _http_retryable(exc: BaseException) -> bool:
    if isinstance(exc, HttpError):
        return int(exc.status_code) in (429, 500, 502, 503, 504)
    return isinstance(exc, TimeoutError)


def _retry_call(fn: Callable[[], Any]) -> Any:
    @retry(
        retry=retry_if_exception(_http_retryable),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=16),
        reraise=True,
    )
    def inner() -> Any:
        return fn()

    return inner()


def _load_credentials(settings: Settings) -> Credentials:
    if settings.gdrive_credentials_path is None:
        raise RuntimeError("ND_GDRIVE_CREDENTIALS_PATH is required for Google Drive ingestion.")

    token_path = settings.gdrive_token_path
    token_path.parent.mkdir(parents=True, exist_ok=True)
    scopes = list(settings.gdrive_scopes)
    creds: Credentials | None = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), scopes)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            _retry_call(lambda: creds.refresh(Request()))
            token_path.write_text(creds.to_json(), encoding="utf-8")
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                str(settings.gdrive_credentials_path), scopes
            )
            creds = flow.run_local_server(port=0)
            token_path.write_text(creds.to_json(), encoding="utf-8")
    return creds


def build_drive_service(settings: Settings) -> Any:
    creds = _load_credentials(settings)
    return build("drive", "v3", credentials=creds, cache_discovery=False)


class GoogleDrivePhotoSource(PhotoSource):
    name = "gdrive"

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def _service(self) -> Any:
        return build_drive_service(self._settings)

    def _since_cutoff(self, since_date: str | None) -> datetime | None:
        if since_date is None:
            return None
        dt0 = datetime.strptime(since_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return dt0

    def _modified_ok(self, modified_time: str | None, since: datetime | None) -> bool:
        if since is None or modified_time is None:
            return True
        try:
            mt = datetime.fromisoformat(modified_time.replace("Z", "+00:00"))
        except ValueError:
            return True
        return mt >= since

    def _is_image_file(self, meta: dict[str, Any]) -> bool:
        mime = str(meta.get("mimeType") or "")
        if mime.startswith("image/"):
            return True
        name = str(meta.get("name") or "")
        suf = Path(name).suffix.lower()
        return suf in _IMAGE_EXTS

    def _download_to_staging(self, service: Any, meta: dict[str, Any], staging: Path) -> Path:
        file_id = str(meta["id"])
        name = str(meta.get("name") or file_id)
        ext = Path(name).suffix.lower()
        if not ext:
            guess = mimetypes.guess_extension(str(meta.get("mimeType") or "")) or ".jpg"
            ext = guess
        out = staging / f"{file_id}{ext}"
        if out.exists():
            remote_size = meta.get("size")
            if remote_size is not None:
                try:
                    if out.stat().st_size == int(remote_size):
                        return out
                except OSError:
                    pass

        staging.mkdir(parents=True, exist_ok=True)

        def _download() -> bytes:
            buf = io.BytesIO()
            request = service.files().get_media(fileId=file_id)
            downloader = MediaIoBaseDownload(buf, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status is not None:
                    _ = status
            return buf.getvalue()

        data = _retry_call(_download)
        out.write_bytes(data)
        return out

    def _iter_drive_images(self, service: Any, folder_id: str, since: datetime | None) -> Iterator[dict[str, Any]]:
        q_root = deque([folder_id])
        while q_root:
            fid = q_root.popleft()

            def _list_page(page_token: str | None) -> dict[str, Any]:
                return (
                    service.files()
                    .list(
                        q=f"'{fid}' in parents and trashed = false",
                        spaces="drive",
                        fields="nextPageToken, files(id, name, mimeType, modifiedTime, size)",
                        pageSize=1000,
                        pageToken=page_token,
                    )
                    .execute()
                )

            page_token: str | None = None
            while True:
                resp = _retry_call(lambda pt=page_token: _list_page(pt))
                for item in resp.get("files", []) or []:
                    mime = str(item.get("mimeType") or "")
                    if mime == _FOLDER_MIME:
                        q_root.append(str(item["id"]))
                        continue
                    if not self._is_image_file(item):
                        continue
                    if not self._modified_ok(str(item.get("modifiedTime")), since):
                        continue
                    yield item
                page_token = resp.get("nextPageToken")
                if not page_token:
                    break

    def list_photos(self, root: Path, *, since_date: str | None = None) -> Iterator[Path]:
        folder_id = str(root).strip()
        if not folder_id:
            return iter(())

        service = self._service()
        staging = Path(self._settings.gdrive_staging_dir)
        since = self._since_cutoff(since_date)

        for meta in self._iter_drive_images(service, folder_id, since):
            yield self._download_to_staging(service, meta, staging)
