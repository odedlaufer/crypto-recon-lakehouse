"""Load and validate the product configuration (config/products.yml)."""

from __future__ import annotations

import re
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

PRODUCT_ID_PATTERN = re.compile(r"[A-Z0-9]+-[A-Z0-9]+")


class ProductsConfig(BaseModel):
    """Products to ingest. Also the silver allow-list (DQ-08)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    products: tuple[str, ...] = Field(min_length=1)

    @field_validator("products")
    @classmethod
    def _validate_products(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        for product_id in value:
            if not PRODUCT_ID_PATTERN.fullmatch(product_id):
                raise ValueError(f"invalid product id {product_id!r}, expected e.g. 'BTC-USD'")
        if len(set(value)) != len(value):
            raise ValueError("duplicate product ids")
        return value


def load_products_config(path: str | Path) -> ProductsConfig:
    """Read and validate a products YAML file."""
    with Path(path).open(encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return ProductsConfig.model_validate(raw)
