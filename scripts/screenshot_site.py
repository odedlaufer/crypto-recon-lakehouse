"""Capture README screenshots of the public pages into docs/img/ (make screenshots).

uv run --group demo python scripts/screenshot_site.py [--url http://localhost:8000/]
"""

from __future__ import annotations

import argparse
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

SITE = "https://odedlaufer.github.io/crypto-recon-lakehouse/"
OUT = Path("docs/img")


def section(page: Page, heading: str, path: Path) -> None:
    """Screenshot the <section> whose h2 contains `heading`."""
    page.locator("section").filter(has=page.locator("h2", has_text=heading)).first.screenshot(
        path=path
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--url", default=SITE)
    args = parser.parse_args()
    site = args.url if args.url.endswith("/") else args.url + "/"
    OUT.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 860}, color_scheme="dark")
        page.goto(site, wait_until="networkidle")
        page.wait_for_selector("#slos .slo")
        page.wait_for_timeout(2000)  # charts and the diagram finish drawing
        page.screenshot(path=OUT / "site-hero.png")
        section(page, "Every lost trade is found", OUT / "site-results.png")
        section(page, "How it works", OUT / "site-how-it-works.png")
        section(page, "Proven by breaking it on purpose", OUT / "site-proven.png")

        page.goto(f"{site}dbt/#!/model/model.crypto_recon.fct_reconciliation_1m")
        page.wait_for_timeout(3000)
        page.screenshot(path=OUT / "dbt-docs.png")

        page.goto(f"{site}elementary/")
        page.wait_for_timeout(4000)
        page.screenshot(path=OUT / "elementary.png")
        browser.close()

    for path in sorted(OUT.glob("*.png")):
        print(f"{path} ({path.stat().st_size // 1024} KB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
