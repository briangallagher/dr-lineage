"""Official OpenLineage client wrapper with explicit delivery semantics."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import (
    DatasetEvent,
    InputDataset,
    Job,
    JobEvent,
    OutputDataset,
    Run,
    RunEvent,
    RunState,
    StaticDataset,
    set_producer,
)

from lineage_demo.config import Settings
from lineage_demo.facets import PRODUCER
from lineage_demo.identities import DatasetIdentity

LOG = logging.getLogger(__name__)


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def input_dataset(
    identity: DatasetIdentity,
    facets: dict[str, Any] | None = None,
    input_facets: dict[str, Any] | None = None,
) -> InputDataset:
    return InputDataset(
        namespace=identity.namespace,
        name=identity.name,
        facets=facets or {},
        inputFacets=input_facets or {},
    )


def output_dataset(
    identity: DatasetIdentity,
    facets: dict[str, Any] | None = None,
    output_facets: dict[str, Any] | None = None,
) -> OutputDataset:
    return OutputDataset(
        namespace=identity.namespace,
        name=identity.name,
        facets=facets or {},
        outputFacets=output_facets or {},
    )


class LineageDeliveryError(RuntimeError):
    """Raised after all configured event-delivery attempts fail."""


class LineageEmitter:
    def __init__(
        self,
        settings: Settings,
        client: OpenLineageClient | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.settings = settings
        self._sleep = sleep
        set_producer(PRODUCER)
        self.client = client or OpenLineageClient(
            config={
                "transport": {
                    "type": "http",
                    "url": settings.marquez_url,
                    "timeout": settings.lineage_emit_timeout_seconds,
                    "retry": {"total": 0, "read": 0, "connect": 0},
                }
            }
        )

    def emit(self, event: RunEvent | DatasetEvent | JobEvent) -> None:
        last_error: Exception | None = None
        for attempt in range(1, self.settings.lineage_emit_attempts + 1):
            try:
                self.client.emit(event)
                return
            except Exception as exc:  # transport exceptions vary by configured client
                last_error = exc
                LOG.warning("OpenLineage delivery attempt %s failed: %s", attempt, exc)
                if attempt < self.settings.lineage_emit_attempts:
                    self._sleep(self.settings.lineage_emit_backoff_seconds * (2 ** (attempt - 1)))
        raise LineageDeliveryError(
            f"OpenLineage delivery failed after {self.settings.lineage_emit_attempts} attempts"
        ) from last_error

    def close(self) -> None:
        close = getattr(self.client, "close", None)
        if close:
            close(self.settings.lineage_emit_timeout_seconds)

    def dataset_event(self, identity: DatasetIdentity, facets: dict[str, Any]) -> None:
        self.emit(
            DatasetEvent(
                eventTime=utc_now(),
                dataset=StaticDataset(
                    namespace=identity.namespace,
                    name=identity.name,
                    facets=facets,
                ),
            )
        )

    def run_event(
        self,
        *,
        state: RunState,
        run_id: str,
        job_namespace: str,
        job_name: str,
        run_facets: dict[str, Any] | None = None,
        job_facets: dict[str, Any] | None = None,
        inputs: list[InputDataset] | None = None,
        outputs: list[OutputDataset] | None = None,
    ) -> None:
        self.emit(
            RunEvent(
                eventTime=utc_now(),
                eventType=state,
                run=Run(runId=run_id, facets=run_facets or {}),
                job=Job(namespace=job_namespace, name=job_name, facets=job_facets or {}),
                inputs=inputs or [],
                outputs=outputs or [],
            )
        )

    def job_event(
        self,
        *,
        job_namespace: str,
        job_name: str,
        job_facets: dict[str, Any],
    ) -> None:
        self.emit(
            JobEvent(
                eventTime=utc_now(),
                job=Job(namespace=job_namespace, name=job_name, facets=job_facets),
                inputs=[],
                outputs=[],
            )
        )
