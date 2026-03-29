"""
Build square favicons from frontend/assets/logo.png (sphere crop, black #0D0F12).
Run after updating logo: python scripts/build_favicon_assets.py
"""

from __future__ import annotations

from pathlib import Path

from PIL import Image

ROOT = Path(__file__).resolve().parents[1]
ASSETS = ROOT / "frontend" / "assets"
LOGO = ASSETS / "logo.png"

BG = (13, 15, 18)  # #0D0F12
SIZES = (16, 32, 48, 64, 128, 256)


def _crop_sphere_square(im):
    """Take left-region square (icon / sphere); tune ratio for horizontal wordmark layouts."""
    im = im.convert("RGBA")
    w, h = im.size
    # Left square: sphere typically in first ~40% of width; never exceed height.
    side = max(1, min(h, int(w * 0.40)))
    top = max(0, (h - side) // 2)
    box = (0, top, side, top + side)
    return im.crop(box)


def _on_black_square(crop_rgba):
    """Composite cropped RGBA onto solid #0D0F12."""
    side = crop_rgba.size[0]
    base = Image.new("RGBA", (side, side), (*BG, 255))
    base.paste(crop_rgba, (0, 0), crop_rgba)
    return base.convert("RGB")


def main() -> int:
    if not LOGO.is_file():
        print(f"Missing {LOGO}", file=sys.stderr)
        return 1

    raw = Image.open(LOGO)
    cropped = _crop_sphere_square(raw)
    master_rgb = _on_black_square(cropped)

    ASSETS.mkdir(parents=True, exist_ok=True)

    png_paths = []
    for s in SIZES:
        out = ASSETS / f"favicon-{s}.png"
        img = master_rgb.resize((s, s), Image.Resampling.LANCZOS)
        img.save(out, format="PNG", optimize=True)
        png_paths.append(out)
        print(out.relative_to(ROOT))

    ico_path = ASSETS / "favicon.ico"
    # Multi-size ICO (common browsers use 16/32/48)
    icons = [master_rgb.resize((s, s), Image.Resampling.LANCZOS) for s in (16, 32, 48, 64)]
    icons[0].save(
        ico_path,
        format="ICO",
        sizes=[(i.width, i.height) for i in icons],
        append_images=icons[1:],
    )
    print(ico_path.relative_to(ROOT))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
