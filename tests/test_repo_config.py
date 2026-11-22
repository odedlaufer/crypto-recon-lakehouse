from pathlib import Path

import yaml

from crypto_recon_common.config import load_products_config

ROOT = Path(__file__).resolve().parents[1]


def test_pipeline_allow_list_matches_products_config() -> None:
    # DQ-08 uses the bundle variable `products`; config/products.yml stays the source.
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))
    bundle_products = tuple(bundle["variables"]["products"]["default"].split(","))
    assert bundle_products == load_products_config(ROOT / "config" / "products.yml").products
