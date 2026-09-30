"""Executable shape of the proposed native KFP lineage contract.

This module does not make KFP a native OpenLineage producer.  It keeps the
identity and context rules in one place so the adapter fixture can test the
shape that a future KFP control-plane producer must emit.
"""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from openlineage.client.event_v2 import Job, Run, RunEvent, RunState
from openlineage.client.serde import Serde

from lineage_demo import facets

PROFILE = "rhoai-kfp"
PROFILE_VERSION = "1.0"
KFP_CONTEXT_FACET = "rhoaiKfp"
KFP_CONTEXT_SCHEMA = "urn:rhoai:kfp:lineage-context:1.0"
SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]*\Z")
KFP_STATE_TO_OPENLINEAGE = {
    "PENDING": RunState.START,
    "RUNNING": RunState.RUNNING,
    "CANCELING": RunState.RUNNING,
    "SUCCEEDED": RunState.COMPLETE,
    "FAILED": RunState.FAIL,
    "ERROR": RunState.FAIL,
    "CANCELED": RunState.ABORT,
}
TERMINAL_RUN_STATES = frozenset({RunState.COMPLETE, RunState.FAIL, RunState.ABORT})
DELIVERY_STATUSES = frozenset({"PENDING", "RETRY", "DELIVERED", "DEAD_LETTER"})


class KFPLineageContractError(ValueError):
    """Raised when a producer cannot construct a conformant KFP context."""


def _safe_text(field: str, value: str) -> None:
    if not isinstance(value, str) or not value or not SAFE_ID.fullmatch(value):
        raise KFPLineageContractError(f"{field} must be a non-empty safe identifier")


def _reject_extra_fields(name: str, value: Mapping[str, Any], allowed: set[str]) -> None:
    extras = set(value) - allowed
    if extras:
        raise KFPLineageContractError(f"{name} has unsupported fields")


def _uuid(field: str, value: str) -> None:
    try:
        uuid.UUID(value)
    except (AttributeError, ValueError, TypeError) as exc:
        raise KFPLineageContractError(f"{field} must be a UUID") from exc


def _iso_now() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(frozen=True)
class KFPLineageContext:
    """Versioned, non-secret execution context for KFP-integrated producers.

    ``root_run_id`` is the exact KFP run UUID.  A task has its own run ID and
    attempt number; it never reuses the root ID.  ``parent_run_id`` defaults
    to the root for a direct child and is explicit for nested integrations.
    """

    deployment: str
    project: str
    pipeline_id: str
    pipeline_version_id: str
    pipeline_name: str
    root_run_id: str
    task_name: str | None = None
    task_id: str | None = None
    task_run_id: str | None = None
    attempt: int | None = None
    parent_run_id: str | None = None

    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> KFPLineageContext:
        """Parse one supported direct-task or root context document."""

        if not isinstance(document, Mapping):
            raise KFPLineageContractError("lineage context must be a JSON object")
        if document.get("profile") != PROFILE or document.get("version") != PROFILE_VERSION:
            raise KFPLineageContractError("unsupported lineage context profile or version")
        try:
            root = document["root"]
            pipeline = document["pipeline"]
            deployment = document["deployment"]
            project = document["project"]
        except KeyError as exc:
            raise KFPLineageContractError(f"lineage context is missing {exc.args[0]}") from exc
        if not isinstance(root, Mapping) or not isinstance(pipeline, Mapping):
            raise KFPLineageContractError("root and pipeline must be JSON objects")
        _reject_extra_fields(
            "lineage context",
            document,
            {"profile", "version", "deployment", "project", "root", "pipeline", "parent", "task"},
        )
        _reject_extra_fields("root", root, {"jobNamespace", "jobName", "runId"})
        _reject_extra_fields("pipeline", pipeline, {"id", "versionId", "name"})
        task = document.get("task")
        parent = document.get("parent")
        if (task is None) != (parent is None):
            raise KFPLineageContractError("task and parent must be supplied together")
        if task is not None and (not isinstance(task, Mapping) or not isinstance(parent, Mapping)):
            raise KFPLineageContractError("task and parent must be JSON objects")
        if task is not None and parent is not None:
            _reject_extra_fields("task", task, {"name", "id", "runId", "attempt"})
            _reject_extra_fields("parent", parent, {"jobNamespace", "jobName", "runId"})
        if root.get("jobNamespace") != f"kfp://{deployment}/{project}" or root.get(
            "jobName"
        ) != pipeline.get("name"):
            raise KFPLineageContractError(
                "root job does not match deployment, project, and pipeline"
            )
        if parent is not None and (
            parent.get("jobNamespace") != root.get("jobNamespace")
            or parent.get("jobName") != root.get("jobName")
        ):
            raise KFPLineageContractError("direct task parent must match the KFP root job")
        try:
            return cls(
                deployment=deployment,
                project=project,
                pipeline_id=pipeline["id"],
                pipeline_version_id=pipeline["versionId"],
                pipeline_name=pipeline["name"],
                root_run_id=root["runId"],
                task_name=task.get("name") if task is not None else None,
                task_id=task.get("id") if task is not None else None,
                task_run_id=task.get("runId") if task is not None else None,
                attempt=task.get("attempt") if task is not None else None,
                parent_run_id=parent.get("runId") if parent is not None else None,
            )
        except KeyError as exc:
            raise KFPLineageContractError(f"lineage context is missing {exc.args[0]}") from exc

    @classmethod
    def from_json_file(cls, path: str | Path) -> KFPLineageContext:
        """Read and validate a context file without exposing its contents in logs."""

        try:
            document = json.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise KFPLineageContractError("lineage context file is unreadable JSON") from exc
        return cls.from_document(document)

    def __post_init__(self) -> None:
        for field_name, value in (
            ("deployment", self.deployment),
            ("project", self.project),
            ("pipeline_id", self.pipeline_id),
            ("pipeline_version_id", self.pipeline_version_id),
        ):
            _safe_text(field_name, value)
        if not self.pipeline_name or any(char in self.pipeline_name for char in "\n\r\t"):
            raise KFPLineageContractError("pipeline_name must be a non-empty display name")
        _uuid("root_run_id", self.root_run_id)

        task_fields = (self.task_name, self.task_id, self.task_run_id, self.attempt)
        if self.task_name is None:
            if any(value is not None for value in task_fields[1:]):
                raise KFPLineageContractError("task fields require task_name")
            if self.parent_run_id is not None:
                raise KFPLineageContractError("parent_run_id is only valid for a task context")
            return

        if not self.task_name or any(char in self.task_name for char in "\n\r\t"):
            raise KFPLineageContractError("task_name must be a non-empty display name")
        if not self.task_id:
            raise KFPLineageContractError("task_id is required for a task context")
        _safe_text("task_id", self.task_id)
        if not self.task_run_id:
            raise KFPLineageContractError("task_run_id is required for a task context")
        _uuid("task_run_id", self.task_run_id)
        if self.task_run_id == self.root_run_id:
            raise KFPLineageContractError("task_run_id must differ from root_run_id")
        if not isinstance(self.attempt, int) or isinstance(self.attempt, bool) or self.attempt < 1:
            raise KFPLineageContractError("attempt must be a positive integer")
        if self.parent_run_id is not None:
            _uuid("parent_run_id", self.parent_run_id)

    @property
    def root_namespace(self) -> str:
        return f"kfp://{self.deployment}/{self.project}"

    @property
    def root_job_name(self) -> str:
        return self.pipeline_name

    @property
    def effective_parent_run_id(self) -> str:
        return self.parent_run_id or self.root_run_id

    def task(self, *, name: str, task_id: str, run_id: str, attempt: int = 1) -> KFPLineageContext:
        """Return a child context for one task attempt."""

        return KFPLineageContext(
            deployment=self.deployment,
            project=self.project,
            pipeline_id=self.pipeline_id,
            pipeline_version_id=self.pipeline_version_id,
            pipeline_name=self.pipeline_name,
            root_run_id=self.root_run_id,
            task_name=name,
            task_id=task_id,
            task_run_id=run_id,
            attempt=attempt,
            parent_run_id=self.task_run_id if self.task_name is not None else self.parent_run_id,
        )

    def kfp_facet(self) -> dict[str, Any]:
        """Return the proposed RHOAI KFP context facet payload."""

        payload: dict[str, Any] = {
            "_producer": facets.PRODUCER,
            "_schemaURL": KFP_CONTEXT_SCHEMA,
            "profile": PROFILE,
            "profileVersion": PROFILE_VERSION,
            "deployment": self.deployment,
            "project": self.project,
            "pipelineId": self.pipeline_id,
            "pipelineVersionId": self.pipeline_version_id,
            "pipelineName": self.pipeline_name,
            "rootRunId": self.root_run_id,
        }
        if self.task_name is not None:
            payload["task"] = {
                "name": self.task_name,
                "id": self.task_id,
                "runId": self.task_run_id,
                "attempt": self.attempt,
            }
            payload["parentRunId"] = self.effective_parent_run_id
        return payload

    def parent_facet(self) -> dict[str, Any]:
        if self.task_name is None:
            raise KFPLineageContractError("A root context has no parent run facet")
        return facets.parent_run(
            parent_namespace=self.root_namespace,
            parent_name=self.root_job_name,
            parent_run_id=self.effective_parent_run_id,
            root_namespace=self.root_namespace,
            root_name=self.root_job_name,
            root_run_id=self.root_run_id,
        )

    def run_facets(self) -> dict[str, Any]:
        result = {KFP_CONTEXT_FACET: self.kfp_facet()}
        if self.task_name is not None:
            result["parent"] = self.parent_facet()
        return result

    def document(self) -> dict[str, Any]:
        """Return the transport-neutral context envelope."""

        document: dict[str, Any] = {
            "profile": PROFILE,
            "version": PROFILE_VERSION,
            "deployment": self.deployment,
            "project": self.project,
            "root": {
                "jobNamespace": self.root_namespace,
                "jobName": self.root_job_name,
                "runId": self.root_run_id,
            },
            "pipeline": {
                "id": self.pipeline_id,
                "versionId": self.pipeline_version_id,
                "name": self.pipeline_name,
            },
        }
        if self.task_name is not None:
            document["parent"] = {
                "jobNamespace": self.root_namespace,
                "jobName": self.root_job_name,
                "runId": self.effective_parent_run_id,
            }
            document["task"] = {
                "name": self.task_name,
                "id": self.task_id,
                "runId": self.task_run_id,
                "attempt": self.attempt,
            }
        return document

    def event_id(self, state: RunState) -> str:
        """Return the stable idempotency key for one lifecycle transition."""

        run_id = self.task_run_id if self.task_name is not None else self.root_run_id
        return f"{PROFILE}:{PROFILE_VERSION}:{run_id}:{state.value}"


@dataclass(frozen=True)
class KFPStateHistoryEntry:
    """One KFP state-history transition with its authoritative timestamp."""

    state: str
    event_time: str

    def __post_init__(self) -> None:
        if self.state not in KFP_STATE_TO_OPENLINEAGE:
            raise KFPLineageContractError(f"unsupported KFP run state: {self.state}")
        if not isinstance(self.event_time, str) or not self.event_time:
            raise KFPLineageContractError("event_time must be a non-empty ISO timestamp")


@dataclass(frozen=True)
class KFPRunSnapshot:
    """Sanitized fields needed to project one KFP run state into a root event."""

    run_id: str
    pipeline_id: str
    pipeline_version_id: str
    pipeline_name: str
    state: str
    event_time: str
    state_history: tuple[KFPStateHistoryEntry, ...] = ()

    def __post_init__(self) -> None:
        _uuid("run_id", self.run_id)
        for field_name, value in (
            ("pipeline_id", self.pipeline_id),
            ("pipeline_version_id", self.pipeline_version_id),
        ):
            _safe_text(field_name, value)
        if not self.pipeline_name or any(char in self.pipeline_name for char in "\n\r\t"):
            raise KFPLineageContractError("pipeline_name must be a non-empty display name")
        KFPStateHistoryEntry(state=self.state, event_time=self.event_time)
        if not isinstance(self.state_history, tuple):
            raise KFPLineageContractError("state_history must be an ordered tuple")
        if any(not isinstance(entry, KFPStateHistoryEntry) for entry in self.state_history):
            raise KFPLineageContractError("state_history contains an invalid entry")
        if self.state_history and self.state_history[-1].state != self.state:
            raise KFPLineageContractError("state_history must end at the current KFP state")

    def context(self, *, deployment: str, project: str) -> KFPLineageContext:
        """Build the root context represented by this KFP API snapshot."""

        return KFPLineageContext(
            deployment=deployment,
            project=project,
            pipeline_id=self.pipeline_id,
            pipeline_version_id=self.pipeline_version_id,
            pipeline_name=self.pipeline_name,
            root_run_id=self.run_id,
        )


@dataclass(frozen=True)
class KFPEventEmission:
    """One root event plus the key an outbox must use for idempotent delivery."""

    idempotency_key: str
    event: RunEvent


@dataclass(frozen=True)
class KFPOutboxRecord:
    """Storage-neutral durable record for one root-event delivery transition."""

    idempotency_key: str
    root_run_id: str
    event_type: RunState
    event: dict[str, Any]
    created_at: str
    attempts: int = 0
    delivery_status: str = "PENDING"

    @classmethod
    def from_emission(cls, emission: KFPEventEmission, *, created_at: str) -> KFPOutboxRecord:
        """Wrap one projected event without exposing credentials or runtime state."""

        event = json.loads(Serde.to_json(emission.event))
        try:
            event_type = RunState(event["eventType"])
            root_run_id = event["run"]["runId"]
        except (KeyError, TypeError, ValueError) as exc:
            raise KFPLineageContractError("root event cannot become an outbox record") from exc
        return cls(
            idempotency_key=emission.idempotency_key,
            root_run_id=root_run_id,
            event_type=event_type,
            event=event,
            created_at=created_at,
        )

    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> KFPOutboxRecord:
        """Read a persisted record and revalidate its event/key relationship."""

        if not isinstance(document, Mapping):
            raise KFPLineageContractError("outbox record must be a JSON object")
        if document.get("profile") != PROFILE or document.get("version") != PROFILE_VERSION:
            raise KFPLineageContractError("unsupported outbox record profile or version")
        try:
            return cls(
                idempotency_key=document["idempotencyKey"],
                root_run_id=document["rootRunId"],
                event_type=RunState(document["eventType"]),
                event=document["event"],
                created_at=document["createdAt"],
                attempts=document["attempts"],
                delivery_status=document["deliveryStatus"],
            )
        except KFPLineageContractError:
            raise
        except (KeyError, TypeError, ValueError) as exc:
            raise KFPLineageContractError("malformed outbox record") from exc

    def __post_init__(self) -> None:
        _uuid("root_run_id", self.root_run_id)
        if not isinstance(self.event_type, RunState):
            raise KFPLineageContractError("outbox record has an invalid root event state")
        if self.event_type not in {RunState.START, RunState.RUNNING} | TERMINAL_RUN_STATES:
            raise KFPLineageContractError("outbox record has an unsupported root event state")
        if not isinstance(self.idempotency_key, str) or self.idempotency_key != (
            f"{PROFILE}:{PROFILE_VERSION}:{self.root_run_id}:{self.event_type.value}"
        ):
            raise KFPLineageContractError("outbox idempotency key does not match the root event")
        if not isinstance(self.created_at, str) or not self.created_at:
            raise KFPLineageContractError("outbox created_at must be a non-empty ISO timestamp")
        if (
            not isinstance(self.attempts, int)
            or isinstance(self.attempts, bool)
            or self.attempts < 0
        ):
            raise KFPLineageContractError("outbox attempts must be a non-negative integer")
        if self.delivery_status not in DELIVERY_STATUSES:
            raise KFPLineageContractError("outbox delivery status is unsupported")
        if not isinstance(self.event, Mapping):
            raise KFPLineageContractError("outbox event must be a JSON object")
        if self.event.get("eventType") != self.event_type.value:
            raise KFPLineageContractError("outbox event type does not match its record")
        run = self.event.get("run")
        if not isinstance(run, Mapping) or run.get("runId") != self.root_run_id:
            raise KFPLineageContractError("outbox event run does not match its record")
        if (
            not isinstance(self.event.get("inputs"), list)
            or not isinstance(self.event.get("outputs"), list)
            or self.event["inputs"]
            or self.event["outputs"]
        ):
            raise KFPLineageContractError("root outbox events cannot contain data edges")
        facets = run.get("facets")
        if not isinstance(facets, Mapping):
            raise KFPLineageContractError("outbox event run facets are missing")
        kfp_facet = facets.get(KFP_CONTEXT_FACET)
        if not isinstance(kfp_facet, Mapping):
            raise KFPLineageContractError("outbox event KFP facet is missing")
        if kfp_facet.get("rootRunId") != self.root_run_id:
            raise KFPLineageContractError("outbox KFP facet does not match its root run")

    def retry(self) -> KFPOutboxRecord:
        """Record a delivery attempt that should be retried."""

        return replace(self, attempts=self.attempts + 1, delivery_status="RETRY")

    def delivered(self) -> KFPOutboxRecord:
        """Mark delivery successful without changing the event identity."""

        return replace(self, attempts=max(self.attempts, 1), delivery_status="DELIVERED")

    def dead_letter(self) -> KFPOutboxRecord:
        """Record an exhausted delivery path separately from KFP run failure."""

        return replace(self, attempts=max(self.attempts, 1), delivery_status="DEAD_LETTER")

    def document(self) -> dict[str, Any]:
        """Return the versioned storage document for this outbox record."""

        return {
            "profile": PROFILE,
            "version": PROFILE_VERSION,
            "idempotencyKey": self.idempotency_key,
            "rootRunId": self.root_run_id,
            "eventType": self.event_type.value,
            "createdAt": self.created_at,
            "attempts": self.attempts,
            "deliveryStatus": self.delivery_status,
            "event": self.event,
        }


@dataclass
class KFPRootEventProjector:
    """Project KFP run snapshots into an idempotent root-event sequence.

    This is an executable model for the proposed control-plane seam. A native
    implementation must persist the emitted keys and terminal state in its
    durable outbox or equivalent rather than relying on this process-local set.
    """

    context: KFPLineageContext
    _emitted_keys: set[str] = field(default_factory=set)
    _terminal_state: RunState | None = None

    def observe(self, snapshot: KFPRunSnapshot) -> tuple[KFPEventEmission, ...]:
        """Return new root events for a validated KFP state observation."""

        self._validate_identity(snapshot)
        history = snapshot.state_history or (
            KFPStateHistoryEntry(state=snapshot.state, event_time=snapshot.event_time),
        )
        emissions: list[KFPEventEmission] = []
        for entry in history:
            emissions.extend(self._observe_transition(entry.state, entry.event_time))
        return tuple(emissions)

    def _observe_transition(self, kfp_state: str, event_time: str) -> tuple[KFPEventEmission, ...]:
        state = KFP_STATE_TO_OPENLINEAGE[kfp_state]
        if self._terminal_state is not None:
            if state != self._terminal_state:
                raise KFPLineageContractError("a terminal KFP state cannot be rewritten")
            return ()

        states: list[RunState] = []
        if self.context.event_id(RunState.START) not in self._emitted_keys:
            states.append(RunState.START)
        if state != RunState.START and self.context.event_id(state) not in self._emitted_keys:
            states.append(state)

        emissions = tuple(
            KFPEventEmission(
                idempotency_key=self.context.event_id(next_state),
                event=root_event(self.context, state=next_state, event_time=event_time),
            )
            for next_state in states
        )
        self._emitted_keys.update(emission.idempotency_key for emission in emissions)
        if state in TERMINAL_RUN_STATES:
            self._terminal_state = state
        return emissions

    def _validate_identity(self, snapshot: KFPRunSnapshot) -> None:
        expected = {
            "run_id": self.context.root_run_id,
            "pipeline_id": self.context.pipeline_id,
            "pipeline_version_id": self.context.pipeline_version_id,
            "pipeline_name": self.context.pipeline_name,
        }
        actual = {
            "run_id": snapshot.run_id,
            "pipeline_id": snapshot.pipeline_id,
            "pipeline_version_id": snapshot.pipeline_version_id,
            "pipeline_name": snapshot.pipeline_name,
        }
        if actual != expected:
            raise KFPLineageContractError("KFP run snapshot does not match the root context")


def root_event(
    context: KFPLineageContext,
    *,
    state: RunState,
    job_facets: dict[str, Any] | None = None,
    event_time: str | None = None,
) -> RunEvent:
    """Build a root lifecycle event owned by the KFP control plane."""

    if context.task_name is not None:
        raise KFPLineageContractError("root_event requires a root context")
    return RunEvent(
        eventTime=event_time or _iso_now(),
        eventType=state,
        run=Run(runId=context.root_run_id, facets=context.run_facets()),
        job=Job(
            namespace=context.root_namespace,
            name=context.root_job_name,
            facets=job_facets or {},
        ),
    )


def task_event(
    context: KFPLineageContext,
    *,
    state: RunState,
    job_namespace: str,
    job_name: str,
    job_facets: dict[str, Any] | None = None,
    run_facets: dict[str, Any] | None = None,
    inputs: list[Any] | None = None,
    outputs: list[Any] | None = None,
    event_time: str | None = None,
) -> RunEvent:
    """Build a child event whose parent/root context came from KFP."""

    if context.task_name is None or context.task_run_id is None:
        raise KFPLineageContractError("task_event requires a task context")
    effective_run_facets = context.run_facets()
    effective_run_facets.update(run_facets or {})
    return RunEvent(
        eventTime=event_time or _iso_now(),
        eventType=state,
        run=Run(runId=context.task_run_id, facets=effective_run_facets),
        job=Job(namespace=job_namespace, name=job_name, facets=job_facets or {}),
        inputs=inputs or [],
        outputs=outputs or [],
    )
