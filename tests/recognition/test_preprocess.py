from __future__ import annotations

import io

from PIL import Image

from nutrition_diary.recognition.preprocess import preprocess_to_b64_jpeg


def test_resizes_longest_side_to_1024() -> None:
    img = Image.new("RGB", (4032, 3024), (200, 10, 10))
    buf = io.BytesIO()
    img.save(buf, format="JPEG")
    res = preprocess_to_b64_jpeg(buf.getvalue(), max_side=1024)
    out = Image.open(io.BytesIO(__import__("base64").b64decode(res.jpeg_b64)))
    assert max(out.size) == 1024
    assert out.size[0] == 1024 and out.size[1] == 768


def test_exif_orientation_applied() -> None:
    img = Image.new("RGB", (40, 20), (255, 0, 0))
    buf = io.BytesIO()
    exif = img.getexif()
    exif[274] = 6  # Orientation: rotate 270 CW
    img.save(buf, format="JPEG", exif=exif)
    res = preprocess_to_b64_jpeg(buf.getvalue(), max_side=1024)
    out = Image.open(io.BytesIO(__import__("base64").b64decode(res.jpeg_b64)))
    assert out.size == (20, 40)
