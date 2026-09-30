"""Validate a sanitized, release-pinned RHOAI/KFP qualification baseline.

The validator deliberately treats a current no-go observation as a valid
qualification result.  It proves that the evidence pack is structurally
complete and reports which native gates are blocked or unknown; it does not
turn an absent platform seam into a passing integration.
"""

from __future__ import annotations

import argparse
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

QUALIFICATION_SCHEMA_VERSION = "rhoai-kfp-qualification:1.0"
IMAGE_REF = re.compile(r"^.+@sha256:[0-9a-f]{64}$")
REQUIRED_IMAGES = (
    "dspo",
    "apiServer",
    "launcher",
    "driver",
    "argoExec",
    "argoWorkflowController",
)
REQUIRED_KFP_FLAGS = (
    "--pipeline_name",
    "--run_id",
    "--execution_id",
    "--executor_input",
    "--component_spec",
    "--pod_name",
    "--pod_uid",
)
CONTRACT_CHECK_IDS = tuple(f"C-{index:02d}" for index in range(1, 15))
UPGRADE_SCENARIO_IDS = (
    "U-01-dspo-image",
    "U-02-api-server-image",
    "U-03-launcher-abi",
    "U-04-context-contract",
)
SENSITIVE_TERMS = ("token", "password", "credential", "secret", "signedurl")


class QualificationError(ValueError):
    """Raised when a qualification fixture cannot be trusted as evidence."""


@dataclass(frozen=True)
class QualificationCheck:
    """One normalized qualification gate result."""

    id: str
    status: str
    evidence: str

    def document(self) -> dict[str, str]:
        return {"id": self.id, "status": self.status, "evidence": self.evidence}


def _required(document: Mapping[str, Any], field: str) -> Any:
    try:
        return document[field]
    except KeyError as exc:
        raise QualificationError("qualification fixture is missing a required field") from exc


def _assert_mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise QualificationError(f"qualification fixture field {field} must be an object")
    return value


def _assert_image(value: Any, field: str) -> str:
    if not isinstance(value, str) or not IMAGE_REF.fullmatch(value):
        raise QualificationError(f"qualification fixture field {field} must be an image digest")
    return value


def _assert_no_sensitive_material(value: Any, path: str = "fixture") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            lowered = str(key).lower()
            if any(term in lowered for term in SENSITIVE_TERMS):
                raise QualificationError("qualification fixture contains a sensitive field")
            _assert_no_sensitive_material(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _assert_no_sensitive_material(child, f"{path}[{index}]")
    elif isinstance(value, str):
        lowered = value.lower()
        if any(term in lowered for term in ("bearer ", "access_key", "secret_access_key")):
            raise QualificationError("qualification fixture contains credential-like material")


def validate_baseline(document: Mapping[str, Any]) -> None:
    """Validate evidence shape and cross-object invariants."""

    _assert_no_sensitive_material(document)
    if document.get("schemaVersion") != QUALIFICATION_SCHEMA_VERSION:
        raise QualificationError("unsupported qualification fixture version")

    release = _assert_mapping(_required(document, "release"), "release")
    if release.get("phase") != "Succeeded":
        raise QualificationError("qualification release CSV is not succeeded")
    related_images = set(release.get("relatedImages", []))
    if not related_images:
        raise QualificationError("qualification release has no related images")

    images = _assert_mapping(_required(document, "images"), "images")
    resolved_images: dict[str, str] = {}
    for image_name in REQUIRED_IMAGES:
        resolved_images[image_name] = _assert_image(
            _required(images, image_name), f"images.{image_name}"
        )
        if resolved_images[image_name] not in related_images:
            raise QualificationError("qualification image is not pinned by the release CSV")

    api_server = _assert_mapping(_required(document, "apiServer"), "apiServer")
    _assert_image(_required(api_server, "image"), "apiServer.image")
    if api_server["image"] != resolved_images["apiServer"]:
        raise QualificationError("API-server image does not match the release image map")
    launcher_env = _assert_mapping(_required(api_server, "launcherEnv"), "apiServer.launcherEnv")
    for env_name in ("V2_LAUNCHER_IMAGE", "V2_LAUNCHER_COMMAND", "V2_DRIVER_IMAGE"):
        setting = _assert_mapping(_required(launcher_env, env_name), f"launcherEnv.{env_name}")
        if not isinstance(setting.get("configured"), bool):
            raise QualificationError("launcher environment configuration must be boolean")
    if launcher_env["V2_LAUNCHER_IMAGE"].get("value") != resolved_images["launcher"]:
        raise QualificationError("launcher environment does not match the pinned launcher")
    if launcher_env["V2_DRIVER_IMAGE"].get("value") != resolved_images["driver"]:
        raise QualificationError("driver environment does not match the pinned driver")

    task_pod = _assert_mapping(_required(document, "taskPod"), "taskPod")
    if task_pod.get("phase") != "Succeeded":
        raise QualificationError("qualification task pod is not succeeded")
    launcher = _assert_mapping(_required(task_pod, "launcher"), "taskPod.launcher")
    if launcher.get("image") != resolved_images["launcher"]:
        raise QualificationError("task launcher does not match the API-server launcher")
    if launcher.get("command") != ["launcher-v2"]:
        raise QualificationError("task launcher command is not the observed launcher-v2 ABI")
    if launcher.get("args") != ["--copy", "/kfp-launcher/launch", "--cache_disabled"]:
        raise QualificationError("task launcher args do not match the observed launcher ABI")

    user_container = _assert_mapping(
        _required(task_pod, "userContainer"), "taskPod.userContainer"
    )
    if not set(REQUIRED_KFP_FLAGS).issubset(set(user_container.get("flagNames", []))):
        raise QualificationError("task pod is missing a required KFP launcher flag")

    coverage = _assert_mapping(_required(document, "coverage"), "coverage")
    contract_checks = coverage.get("contractChecks")
    if not isinstance(contract_checks, list) or {
        item.get("id") for item in contract_checks if isinstance(item, Mapping)
    } != set(CONTRACT_CHECK_IDS):
        raise QualificationError("qualification fixture must cover C-01 through C-14 exactly")
    upgrade_scenarios = coverage.get("upgradeScenarios")
    if not isinstance(upgrade_scenarios, list) or {
        item.get("id") for item in upgrade_scenarios if isinstance(item, Mapping)
    } != set(UPGRADE_SCENARIO_IDS):
        raise QualificationError("qualification fixture must cover all upgrade scenarios")


def load_baseline(path: str | Path) -> dict[str, Any]:
    """Load and validate a qualification fixture without echoing its contents."""

    try:
        document = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise QualificationError("qualification fixture is unreadable JSON") from exc
    if not isinstance(document, Mapping):
        raise QualificationError("qualification fixture must be a JSON object")
    validate_baseline(document)
    return dict(document)


def build_report(document: Mapping[str, Any]) -> dict[str, Any]:
    """Return the current release-gate result for a validated fixture."""

    validate_baseline(document)
    api_server = document["apiServer"]
    task_pod = document["taskPod"]
    provenance = document["provenance"]
    launcher_env = api_server["launcherEnv"]
    task_context = task_pod["contextInjection"]

    checks = [
        QualificationCheck(
            "G-00-release-mapping",
            "pass",
            "OLM relatedImages pins the observed component digests to the installed release",
        ),
        QualificationCheck(
            "G-01-launcher-override",
            "pass" if launcher_env["V2_LAUNCHER_IMAGE"]["configured"] else "blocked",
            "V2_LAUNCHER_IMAGE is observed on the packaged API server",
        ),
        QualificationCheck(
            "G-02-launcher-runtime",
            "pass",
            "completed task pod uses the pinned launcher-v2 command and ABI",
        ),
        QualificationCheck(
            "G-03-context-transport",
            "blocked"
            if not task_context["contextVolumePresent"]
            and not task_context["contextPathEnvNames"]
            else "pass",
            "task pod has no platform-injected context path or volume",
        ),
        QualificationCheck(
            "G-04-component-source-provenance",
            "unknown"
            if provenance["componentSourceRefs"]["status"] == "unknown"
            else "pass",
            "component source/build refs are not exposed by the installed release manifest",
        ),
        QualificationCheck(
            "G-05-owner-api",
            "unknown" if provenance["ownerApi"]["status"] == "unknown" else "pass",
            "supported owner/API approval is not recorded",
        ),
    ]
    decision = "GO" if all(check.status == "pass" for check in checks) else "NO_GO"
    return {
        "schemaVersion": QUALIFICATION_SCHEMA_VERSION,
        "decision": decision,
        "release": document["release"],
        "checks": [check.document() for check in checks],
        "contractCoverage": document["coverage"]["contractChecks"],
        "upgradeScenarios": document["coverage"]["upgradeScenarios"],
    }


def _main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fixture", type=Path)
    parser.add_argument("--expect", choices=("GO", "NO_GO"))
    args = parser.parse_args()
    try:
        report = build_report(load_baseline(args.fixture))
    except QualificationError as exc:
        parser.error(str(exc))
    print(json.dumps(report, indent=2, sort_keys=True))
    if args.expect and report["decision"] != args.expect:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
