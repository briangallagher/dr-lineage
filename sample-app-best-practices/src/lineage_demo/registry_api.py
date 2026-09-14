"""FastAPI mock of the subset of Data Registry needed by the sample."""

from __future__ import annotations

import threading
from datetime import UTC, datetime
from enum import Enum
from typing import Annotated

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator

from lineage_demo import facets
from lineage_demo.config import Settings, get_settings
from lineage_demo.events import LineageDeliveryError, LineageEmitter
from lineage_demo.identities import DatasetIdentity, canonical_s3_dataset, uuid7
from lineage_demo.registry_store import RegistryStore, build_store


def now() -> str:
    return datetime.now(UTC).isoformat()


class LineageStatus(str, Enum):
    pending = "PENDING"
    delivered = "DELIVERED"


class AssetCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    description: str = Field(default="", max_length=4000)
    owner: str = Field(default="data-science", min_length=1, max_length=200)
    location: str

    @field_validator("location")
    @classmethod
    def validate_location(cls, value: str) -> str:
        canonical_s3_dataset(value)
        return value


class AssetPatch(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=200)
    description: str | None = Field(default=None, max_length=4000)
    owner: str | None = Field(default=None, min_length=1, max_length=200)
    location: str | None = None

    @field_validator("location")
    @classmethod
    def validate_location(cls, value: str | None) -> str | None:
        if value is not None:
            canonical_s3_dataset(value)
        return value


class AssetRecord(BaseModel):
    assetId: str
    name: str
    description: str
    owner: str
    location: str
    lineageStatus: LineageStatus
    createdAt: str
    updatedAt: str


class RegistryService:
    def __init__(
        self,
        *,
        settings: Settings,
        store: RegistryStore,
        emitter: LineageEmitter,
    ) -> None:
        self.settings = settings
        self.store = store
        self.emitter = emitter
        self._lock = threading.RLock()

    def list_assets(self) -> list[AssetRecord]:
        state = self.store.read()
        return [AssetRecord.model_validate(value) for value in state["assets"].values()]

    def get_asset(self, asset_id: str) -> AssetRecord:
        state = self.store.read()
        value = state["assets"].get(asset_id)
        if value is None:
            raise KeyError(asset_id)
        return AssetRecord.model_validate(value)

    def create(self, request: AssetCreate, idempotency_key: str) -> tuple[AssetRecord, bool]:
        with self._lock:
            state = self.store.read()
            existing_id = state["idempotencyKeys"].get(idempotency_key)
            created = existing_id is None
            if created:
                asset_id = str(uuid7())
                timestamp = now()
                record = AssetRecord(
                    assetId=asset_id,
                    name=request.name,
                    description=request.description,
                    owner=request.owner,
                    location=request.location,
                    lineageStatus=LineageStatus.pending,
                    createdAt=timestamp,
                    updatedAt=timestamp,
                )
                state["assets"][asset_id] = record.model_dump(mode="json")
                state["idempotencyKeys"][idempotency_key] = asset_id
                self.store.write(state)  # durable before event delivery
            else:
                record = AssetRecord.model_validate(state["assets"][existing_id])

            if record.lineageStatus == LineageStatus.pending:
                record = self._deliver(record, "CREATE")
            return record, created

    def patch(self, asset_id: str, request: AssetPatch) -> AssetRecord:
        with self._lock:
            state = self.store.read()
            value = state["assets"].get(asset_id)
            if value is None:
                raise KeyError(asset_id)
            record = AssetRecord.model_validate(value)
            updates = request.model_dump(exclude_none=True)
            record = record.model_copy(
                update={
                    **updates,
                    "updatedAt": now(),
                    "lineageStatus": LineageStatus.pending,
                }
            )
            state["assets"][asset_id] = record.model_dump(mode="json")
            self.store.write(state)
            return self._deliver(record, "ALTER")

    def _deliver(self, record: AssetRecord, lifecycle: str) -> AssetRecord:
        physical = canonical_s3_dataset(record.location)
        logical = DatasetIdentity(self.settings.registry_namespace, record.assetId)
        dataset_facets = {
            "documentation": facets.documentation(record.description or record.name),
            "dataSource": facets.data_source(
                "RHOAI Data Registry", self.settings.registry_namespace
            ),
            "datasetType": facets.dataset_type("FILE"),
            "ownership": facets.dataset_ownership(record.owner),
            "lifecycleStateChange": facets.lifecycle_change(lifecycle),
            "symlinks": facets.symlinks(physical.namespace, physical.name, "OBJECT"),
        }
        try:
            self.emitter.dataset_event(logical, dataset_facets)
        except LineageDeliveryError:
            raise

        state = self.store.read()
        delivered = record.model_copy(
            update={"lineageStatus": LineageStatus.delivered, "updatedAt": now()}
        )
        state["assets"][record.assetId] = delivered.model_dump(mode="json")
        self.store.write(state)
        return delivered


def create_app(
    *,
    settings: Settings | None = None,
    store: RegistryStore | None = None,
    emitter: LineageEmitter | None = None,
) -> FastAPI:
    settings = settings or get_settings()
    service = RegistryService(
        settings=settings,
        store=store or build_store(settings),
        emitter=emitter or LineageEmitter(settings),
    )
    app = FastAPI(title="OpenLineage Data Registry Stub", version="0.1.0")
    app.state.registry_service = service

    @app.get("/healthz")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/v1/assets", response_model=list[AssetRecord])
    def list_assets() -> list[AssetRecord]:
        return service.list_assets()

    @app.get("/v1/assets/{asset_id}", response_model=AssetRecord)
    def get_asset(asset_id: str) -> AssetRecord:
        try:
            return service.get_asset(asset_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Asset not found") from exc

    @app.post("/v1/assets", response_model=AssetRecord)
    def create_asset(
        request: AssetCreate,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1)],
    ) -> AssetRecord | JSONResponse:
        try:
            record, created = service.create(request, idempotency_key)
            if created:
                return JSONResponse(status_code=201, content=record.model_dump(mode="json"))
            return record
        except LineageDeliveryError:
            state = service.store.read()
            asset_id = state["idempotencyKeys"][idempotency_key]
            record = AssetRecord.model_validate(state["assets"][asset_id])
            return JSONResponse(
                status_code=503,
                content={
                    "detail": "Asset persisted; OpenLineage delivery remains pending",
                    "asset": record.model_dump(mode="json"),
                },
            )

    @app.patch("/v1/assets/{asset_id}", response_model=AssetRecord)
    def patch_asset(asset_id: str, request: AssetPatch) -> AssetRecord | JSONResponse:
        try:
            return service.patch(asset_id, request)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Asset not found") from exc
        except LineageDeliveryError:
            record = service.get_asset(asset_id)
            return JSONResponse(
                status_code=503,
                content={
                    "detail": "Asset updated; OpenLineage delivery remains pending",
                    "asset": record.model_dump(mode="json"),
                },
            )

    return app
