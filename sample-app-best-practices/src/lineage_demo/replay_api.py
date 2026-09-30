"""Small local replay API for the product demonstrator."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse


def _load(data_dir: Path) -> dict[str, Any]:
    return json.loads((data_dir / "demo.json").read_text(encoding="utf-8"))


def create_app(data_dir: str | Path = "build/product-demo") -> FastAPI:
    root = Path(data_dir)
    app = FastAPI(title="RHOAI Lineage Product Demonstrator", version="0.1.0")

    @app.get("/api/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "dataDir": str(root)}

    @app.get("/api/demo")
    def demo() -> dict[str, Any]:
        return _load(root)

    @app.get("/api/dossier")
    def dossier() -> dict[str, Any]:
        return _load(root)["auditDossier"]

    @app.get("/api/scenarios")
    def scenarios() -> list[dict[str, Any]]:
        return [
            {"id": item["id"], "title": item["title"], "description": item["description"]}
            for item in _load(root)["scenarios"]
        ]

    @app.get("/api/scenarios/{scenario_id}")
    def scenario(scenario_id: str) -> dict[str, Any]:
        for item in _load(root)["scenarios"]:
            if item["id"] == scenario_id:
                return item
        raise HTTPException(status_code=404, detail="scenario not found")

    @app.get("/")
    def cockpit() -> FileResponse:
        return FileResponse(root / "cockpit.html")

    return app
