from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from nutrition_diary.config import Settings
from nutrition_diary.sources.google_drive import GoogleDrivePhotoSource


def _img_meta(**kwargs):
    base = {
        "id": "file1",
        "name": "meal.jpg",
        "mimeType": "image/jpeg",
        "modifiedTime": "2026-04-19T12:00:00.000Z",
        "size": "10",
    }
    base.update(kwargs)
    return base


def test_list_photos_downloads_staged(tmp_path: Path) -> None:
    settings = Settings(
        data_dir=tmp_path / "data",
        gdrive_staging_dir=tmp_path / "data" / "gdrive",
        gdrive_credentials_path=tmp_path / "client.json",
        gdrive_token_path=tmp_path / "data" / "token.json",
    )

    def _fake_iter(self, service, folder_id, since):
        yield _img_meta()

    staged = tmp_path / "data" / "gdrive" / "file1.jpg"

    def _fake_download(self, service, meta, staging):
        staged.parent.mkdir(parents=True, exist_ok=True)
        staged.write_bytes(b"\xff\xd8\xff")
        return staged

    src = GoogleDrivePhotoSource(settings=settings)
    with (
        patch.object(GoogleDrivePhotoSource, "_service", return_value=object()),
        patch.object(GoogleDrivePhotoSource, "_iter_drive_images", _fake_iter),
        patch.object(GoogleDrivePhotoSource, "_download_to_staging", _fake_download),
    ):
        paths = list(src.list_photos(Path("folderId123"), since_date=None))

    assert len(paths) == 1
    assert paths[0] == staged
    assert paths[0].exists()


def test_modified_time_filter() -> None:
    settings = Settings()
    src = GoogleDrivePhotoSource(settings=settings)
    since = src._since_cutoff("2026-04-15")
    assert since is not None
    assert src._modified_ok("2026-04-10T12:00:00.000Z", since) is False
    assert src._modified_ok("2026-04-16T12:00:00.000Z", since) is True
