"""Local prototype for the proposed native KFP context injection seam.

This module models the boundary described in the implementation-readiness ADR;
it does not change KFP, RHOAI, or a cluster.  The production launcher wrapper
would resolve the context by exact KFP run UUID, validate it, and expose the
read-only file to workload integrations before invoking the normal KFP
launcher behavior.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from lineage_demo.kfp_lineage import (
    KFPLineageContext,
    KFPLineageContractError,
)

PROFILE = "rhoai-kfp"
PROFILE_VERSION = "1.0"
CONTEXT_PATH = "/var/run/rhoai/lineage/context.json"
CONTEXT_PATH_ENV = "RHOAI_LINEAGE_CONTEXT_PATH"
PROFILE_VERSION_ENV = "RHOAI_LINEAGE_PROFILE_VERSION"
LAUNCHER_IMAGE_ENV = "V2_LAUNCHER_IMAGE"
LAUNCHER_COMMAND_ENV = "V2_LAUNCHER_COMMAND"
FORWARDED_KFP_FLAGS = (
    "--pipeline_name",
    "--run_id",
    "--execution_id",
    "--executor_input",
    "--component_spec",
    "--pod_name",
    "--pod_uid",
)


def _resolver_url(value: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise KFPLineageContractError("context resolver URL must be an HTTP(S) URL")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise KFPLineageContractError(
            "context resolver URL must not contain credentials, query, or fragment"
        )
    return value.rstrip("/")


@dataclass(frozen=True)
class KFPLauncherContextPlan:
    """Safe, transport-neutral plan for a launcher-compatible context wrapper."""

    context: KFPLineageContext
    resolver_url: str
    launcher_image: str = "rhoai-kfp-launcher"
    launcher_command: str = "rhoai-kfp-launcher"
    context_path: str = CONTEXT_PATH

    def __post_init__(self) -> None:
        if self.context.task_name or self.context.task_id or self.context.task_run_id:
            raise KFPLineageContractError("launcher context plan requires a root context")
        if self.context.parent_run_id is not None or self.context.attempt is not None:
            raise KFPLineageContractError("launcher context plan cannot contain task ancestry")
        _resolver_url(self.resolver_url)
        if self.context_path != CONTEXT_PATH:
            raise KFPLineageContractError(
                f"launcher context path must remain {CONTEXT_PATH}"
            )
        if not self.launcher_image or not self.launcher_command:
            raise KFPLineageContractError("launcher image and command are required")

    @property
    def lookup_url(self) -> str:
        return f"{self.resolver_url}/v1/lineage/contexts/{self.context.root_run_id}"

    def runtime_environment(self) -> dict[str, str]:
        """Return only safe environment values supplied to the workload."""

        return {
            CONTEXT_PATH_ENV: self.context_path,
            PROFILE_VERSION_ENV: PROFILE_VERSION,
        }

    def document(self) -> dict[str, Any]:
        """Serialize the plan without credentials or mutable pod identity."""

        return {
            "profile": f"{PROFILE}:{PROFILE_VERSION}",
            "mechanism": "kfp-v2-launcher-wrapper-context-resolver",
            "contextPath": self.context_path,
            "runtimeEnvironment": self.runtime_environment(),
            "launcherOverride": {
                "imageEnvironmentVariable": LAUNCHER_IMAGE_ENV,
                "commandEnvironmentVariable": LAUNCHER_COMMAND_ENV,
                "image": self.launcher_image,
                "command": self.launcher_command,
                "preservesFlags": list(FORWARDED_KFP_FLAGS),
            },
            "resolver": {
                "method": "GET",
                "url": self.lookup_url,
                "authorization": "in-cluster-service-account",
                "lookupKey": "exact-kfp-root-run-id",
            },
            "missingContext": "UNLINKED_DIAGNOSTIC",
            "rootContext": self.context.document(),
        }


def build_launcher_context_plan(
    context: KFPLineageContext, *, resolver_url: str
) -> KFPLauncherContextPlan:
    """Build and validate the local prototype's native-integration plan."""

    # Round-trip through the existing parser so this boundary cannot silently
    # weaken the version, identity, or redaction rules of the context contract.
    KFPLineageContext.from_document(context.document())
    return KFPLauncherContextPlan(context=context, resolver_url=resolver_url)


def materialize_runtime_context(
    plan: KFPLauncherContextPlan, directory: Path
) -> tuple[Path, dict[str, str]]:
    """Materialize a test-only read-only context file and its environment.

    A real launcher wrapper would write the response received from the resolver
    using the same validation rules.  This helper intentionally writes only the
    already-validated root document and never accepts a token or secret input.
    """

    directory.mkdir(parents=True, exist_ok=True)
    context_file = directory / "context.json"
    temporary_file = directory / ".context.json.tmp"
    temporary_file.write_text(
        json.dumps(plan.context.document(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary_file.replace(context_file)
    context_file.chmod(0o444)
    return context_file, plan.runtime_environment()


def read_materialized_context(path: Path) -> KFPLineageContext:
    """Read a materialized context without including its contents in errors."""

    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise KFPLineageContractError("materialized lineage context is unreadable") from exc
    return KFPLineageContext.from_document(document)
