"""Record the 2-minute captioned demo video of the public site (make demo-video).

A scripted browser tour with on-screen captions: the site, then the dbt lineage and the
data-quality report. Playwright records WebM; ffmpeg converts it to web/media/demo.mp4
(H.264), which the site shows automatically once published.

    uv run --group demo python scripts/record_demo.py [--url http://localhost:8000/]
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import tempfile
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

SITE = "https://odedlaufer.github.io/crypto-recon-lakehouse/"
OUT = Path("web/media/demo.mp4")
SIZE = {"width": 1280, "height": 720}
# H.264 MP4 that plays everywhere (Safari, phones); no audio track.
ENCODE = ["-c:v", "libx264", "-preset", "slow", "-crf", "26", "-pix_fmt", "yuv420p"]
ENCODE += ["-movflags", "+faststart", "-an"]

CAPTION_JS = """
text => {
  let box = document.getElementById('demo-caption');
  if (!box) {
    box = document.createElement('div');
    box.id = 'demo-caption';
    Object.assign(box.style, {
      position: 'fixed', left: '50%', bottom: '28px', transform: 'translateX(-50%)',
      maxWidth: '86%', padding: '14px 22px', borderRadius: '12px', zIndex: 99999,
      background: 'rgba(5, 10, 25, 0.88)', color: '#fff', textAlign: 'center',
      font: '600 22px/1.35 Inter, system-ui, sans-serif',
      boxShadow: '0 8px 30px rgba(0,0,0,.45)', border: '1px solid #2a3a6b',
    });
    document.body.appendChild(box);
  }
  box.textContent = text;
  box.style.display = text ? 'block' : 'none';
}
"""

# (heading text to scroll to, or None to stay, caption, seconds on screen)
TOUR = [
    (
        None,
        "A market-data pipeline that heals itself. Live Coinbase trades, checked against "
        "the exchange's own candles.",
        7,
    ),
    (None, "Live numbers: share of minutes complete before and after automatic repair.", 6),
    (
        "Live market feeds lose data",
        "The problem: live feeds drop messages, and most "
        "pipelines silently store whatever arrives.",
        7,
    ),
    (
        "Every lost trade is found",
        "Raw vs repaired, every day. Each dip is an outage or a "
        "chaos test, repaired by the next run.",
        10,
    ),
    (
        "Big losses come from outages",
        "1.9 million lost trades recovered. And Coinbase's own "
        "candles are wrong on about 2% of minutes.",
        8,
    ),
    ("How it works", "Capture, refine, reconcile, detect by trade id, repair.", 8),
    (
        "Proven by breaking it on purpose",
        "Failures injected on the live system: dropped trades, "
        "a killed producer, poisoned data, a corrupted source.",
        9,
    ),
    ("Reliability targets", "Service-level objectives, measured over the last 7 days.", 7),
    (
        "Decisions and trade-offs",
        "Every decision is recorded: constraint, options, decision, trade-off.",
        9,
    ),
    ("What building it revealed", "What the system uncovered while being built.", 7),
]


def caption(page: Page, text: str) -> None:
    page.evaluate(CAPTION_JS, text)


def scroll_to_heading(page: Page, text: str) -> None:
    heading = page.locator("h2, h3", has_text=text).first
    heading.evaluate("el => el.scrollIntoView({behavior: 'smooth', block: 'start'})")
    page.wait_for_timeout(1200)


def tour(page: Page, site: str) -> None:
    page.goto(site, wait_until="networkidle")
    page.wait_for_selector("#completenessChart")
    page.wait_for_timeout(1500)  # charts and the diagram finish drawing
    for heading, text, seconds in TOUR:
        if heading:
            scroll_to_heading(page, heading)
        caption(page, text)
        page.wait_for_timeout(seconds * 1000)

    page.goto(f"{site}dbt/#!/model/model.crypto_recon.fct_reconciliation_1m")
    page.wait_for_timeout(2500)
    caption(page, "dbt docs: every model with its tests, contracts and lineage.")
    page.wait_for_timeout(7000)

    page.goto(f"{site}elementary/")
    page.wait_for_timeout(3000)
    caption(page, "Data-quality report: test results and anomaly detection after every run.")
    page.wait_for_timeout(7000)

    page.goto(site, wait_until="networkidle")
    page.wait_for_timeout(1000)
    caption(page, "github.com/odedlaufer/crypto-recon-lakehouse")
    page.wait_for_timeout(5000)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--url", default=SITE, help="site to record (default: the live site)")
    parser.add_argument("--out", type=Path, default=OUT)
    args = parser.parse_args()
    site = args.url if args.url.endswith("/") else args.url + "/"
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        parser.error("ffmpeg is required (macOS: brew install ffmpeg)")

    with tempfile.TemporaryDirectory() as tmp, sync_playwright() as p:
        browser = p.chromium.launch()
        context = browser.new_context(
            viewport=SIZE, record_video_dir=tmp, record_video_size=SIZE, color_scheme="dark"
        )
        page = context.new_page()
        tour(page, site)
        video = page.video
        context.close()  # finalizes the recording
        browser.close()
        webm = Path(video.path())
        args.out.parent.mkdir(parents=True, exist_ok=True)
        cmd = [ffmpeg, "-y", "-loglevel", "error", "-i", str(webm), *ENCODE, str(args.out)]
        subprocess.run(cmd, check=True)  # noqa: S603 (fixed arguments)
    print(f"wrote {args.out} ({args.out.stat().st_size / 1e6:.1f} MB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
