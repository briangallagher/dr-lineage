"""Client used by ingestion to resolve a registry asset to its data location."""

from __future__ import annotations

import httpx

from lineage_demo.registry_api import AssetRecord


def get_asset(registry_url: str, asset_id: str, timeout: float = 10.0) -> AssetRecord:
    response = httpx.get(f"{registry_url.rstrip('/')}/v1/assets/{asset_id}", timeout=timeout)
    response.raise_for_status()
    return AssetRecord.model_validate(response.json())
