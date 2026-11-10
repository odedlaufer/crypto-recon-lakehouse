from pathlib import Path

import pytest
from pydantic import ValidationError

from crypto_recon_common.config import ProductsConfig, load_products_config

REPO_ROOT = Path(__file__).resolve().parents[3]


def _write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "products.yml"
    path.write_text(text, encoding="utf-8")
    return path


def test_repo_config_is_valid() -> None:
    config = load_products_config(REPO_ROOT / "config" / "products.yml")
    assert config.products == ("BTC-USD", "ETH-USD", "SOL-USD")


def test_loads_valid_file(tmp_path: Path) -> None:
    config = load_products_config(_write(tmp_path, "products:\n  - BTC-USD\n"))
    assert config.products == ("BTC-USD",)


@pytest.mark.parametrize(
    "text",
    [
        "products: []\n",
        "products:\n  - btc-usd\n",
        "products:\n  - BTCUSD\n",
        "products:\n  - BTC-USD\n  - BTC-USD\n",
        "products:\n  - BTC-USD\nextra: 1\n",
        "",
    ],
    ids=["empty-list", "lowercase", "no-dash", "duplicate", "unknown-key", "empty-file"],
)
def test_rejects_invalid_file(tmp_path: Path, text: str) -> None:
    with pytest.raises(ValidationError):
        load_products_config(_write(tmp_path, text))


def test_config_is_immutable() -> None:
    config = ProductsConfig(products=("BTC-USD",))
    with pytest.raises(ValidationError):
        config.products = ("ETH-USD",)
