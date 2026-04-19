from __future__ import annotations

import base64
import io
from dataclasses import dataclass

from PIL import Image, ImageOps


@dataclass(frozen=True)
class PreprocessResult:
    jpeg_b64: str
    orig_size: tuple[int, int]
    final_size: tuple[int, int]


def _register_heif() -> None:
    try:
        from pillow_heif import register_heif_opener  # type: ignore[import-not-found]

        register_heif_opener()
    except ImportError:
        pass


def preprocess_to_b64_jpeg(image_bytes: bytes, *, max_side: int = 1024) -> PreprocessResult:
    """Normalize image for Bedrock: HEIC if available, EXIF orientation, resize, JPEG base64."""
    _register_heif()
    buf = io.BytesIO(image_bytes)
    img = Image.open(buf)
    orig_size = (img.width, img.height)
    img = ImageOps.exif_transpose(img)
    if img.mode != "RGB":
        img = img.convert("RGB")

    w, h = img.size
    longest = max(w, h)
    if longest > max_side:
        scale = max_side / float(longest)
        new_w = max(1, int(round(w * scale)))
        new_h = max(1, int(round(h * scale)))
        img = img.resize((new_w, new_h), Image.Resampling.LANCZOS)

    out = io.BytesIO()
    img.save(out, format="JPEG", quality=88, optimize=True)
    final_size = (img.width, img.height)
    b64 = base64.b64encode(out.getvalue()).decode("ascii")
    return PreprocessResult(jpeg_b64=b64, orig_size=orig_size, final_size=final_size)
