# ruff: noqa: E501

"""Build the replayable, stakeholder-facing lineage product demonstrator."""

from __future__ import annotations

import copy
import html
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator
from openlineage.client.event_v2 import RunEvent
from openlineage.client.serde import Serde

from lineage_demo import facets
from lineage_demo.audit_dossier import build_audit_dossier
from lineage_demo.config import Settings
from lineage_demo.decision_package import build_decision_package
from lineage_demo.kfp_lineage import KFPLineageContext, KFPOutboxRecord
from lineage_demo.native_validation import build_native_path_validation
from lineage_demo.showcase import ShowcaseBundle, build_showcase

FAILURE_ROOT_RUN_ID = "9d0d3bd9-cd56-4e8b-9f0b-6d04f2e4b101"
FAILURE_RESOLVE_ATTEMPT_1 = "9d0d3bd9-cd56-4e8b-9f0b-6d04f2e4b102"
FAILURE_RESOLVE_ATTEMPT_2 = "9d0d3bd9-cd56-4e8b-9f0b-6d04f2e4b103"
FAILURE_PREPARE_RUN_ID = "9d0d3bd9-cd56-4e8b-9f0b-6d04f2e4b104"
FAILURE_TRAIN_RUN_ID = "9d0d3bd9-cd56-4e8b-9f0b-6d04f2e4b105"


@dataclass(frozen=True)
class DemoScenario:
    """Serializable scenario data and optional typed events for Marquez delivery."""

    scenario_id: str
    title: str
    description: str
    events: tuple[dict[str, Any], ...]
    contexts: tuple[dict[str, Any], ...]
    outbox: tuple[dict[str, Any], ...]
    metadata: dict[str, Any]
    typed_events: tuple[RunEvent, ...] = ()

    def document(self) -> dict[str, Any]:
        return {
            "id": self.scenario_id,
            "title": self.title,
            "description": self.description,
            "events": list(self.events),
            "contexts": list(self.contexts),
            "outbox": list(self.outbox),
            "metadata": self.metadata,
        }


def _event_document(event: RunEvent) -> dict[str, Any]:
    return json.loads(Serde.to_json(event))


def _scenario_from_showcase(bundle: ShowcaseBundle) -> DemoScenario:
    return DemoScenario(
        scenario_id="happy-path",
        title="Governed training succeeds",
        description=(
            "A governed Data Registry asset flows through KFP, feature preparation, "
            "evaluation, and a candidate model."
        ),
        events=tuple(_event_document(event) for event in bundle.events),
        contexts=bundle.contexts,
        outbox=bundle.outbox,
        metadata={**bundle.metadata, "scenarioId": "happy-path", "scenarioStatus": "SUCCEEDED"},
        typed_events=bundle.events,
    )


def _rewrite_event(
    event: dict[str, Any],
    *,
    root_run_id: str,
    task_run_id: str | None = None,
    attempt: int | None = None,
    event_type: str | None = None,
    event_time: str | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    result = copy.deepcopy(event)
    result["eventType"] = event_type or result["eventType"]
    if event_time:
        result["eventTime"] = event_time
    result["run"]["runId"] = root_run_id if task_run_id is None else task_run_id
    kfp_facet = result["run"]["facets"].get("rhoaiKfp")
    if not isinstance(kfp_facet, dict):
        raise ValueError("demo event is missing the KFP context facet")
    kfp_facet["rootRunId"] = root_run_id
    task = kfp_facet.get("task")
    if task_run_id is not None:
        if not isinstance(task, dict):
            raise ValueError("task event is missing its task identity")
        task["runId"] = task_run_id
        if attempt is not None:
            task["attempt"] = attempt
        kfp_facet["parentRunId"] = root_run_id
        parent = result["run"]["facets"].get("parent")
        if isinstance(parent, dict):
            parent["run"]["runId"] = root_run_id
            parent["root"]["run"]["runId"] = root_run_id
    if error:
        result["run"]["facets"]["errorMessage"] = facets.error_message(error)
    return result


def _rewrite_context(
    document: dict[str, Any], *, root_run_id: str, task_run_id: str, attempt: int
) -> dict[str, Any]:
    result = copy.deepcopy(document)
    result["root"]["runId"] = root_run_id
    result["parent"]["runId"] = root_run_id
    result["task"]["runId"] = task_run_id
    result["task"]["attempt"] = attempt
    return result


def _rewrite_outbox(document: dict[str, Any], root_run_id: str) -> dict[str, Any]:
    result = copy.deepcopy(document)
    event_type = result["eventType"]
    result["idempotencyKey"] = f"rhoai-kfp:1.0:{root_run_id}:{event_type}"
    result["rootRunId"] = root_run_id
    result["event"]["run"]["runId"] = root_run_id
    result["event"]["run"]["facets"]["rhoaiKfp"]["rootRunId"] = root_run_id
    return result


def _retry_scenario(happy: DemoScenario) -> DemoScenario:
    events = happy.events
    root_start, root_running = events[0], events[1]
    resolve_start, resolve_complete = events[2], events[3]
    prepare_start, prepare_complete = events[4], events[5]
    train_start, train_complete = events[6], events[7]
    root_complete = events[8]
    retry_events = (
        _rewrite_event(
            root_start, root_run_id=FAILURE_ROOT_RUN_ID, event_time="2026-09-30T14:00:00Z"
        ),
        _rewrite_event(
            root_running, root_run_id=FAILURE_ROOT_RUN_ID, event_time="2026-09-30T14:00:02Z"
        ),
        _rewrite_event(
            resolve_start,
            root_run_id=FAILURE_ROOT_RUN_ID,
            task_run_id=FAILURE_RESOLVE_ATTEMPT_1,
            attempt=1,
            event_time="2026-09-30T14:00:04Z",
        ),
        _rewrite_event(
            resolve_complete,
            root_run_id=FAILURE_ROOT_RUN_ID,
            task_run_id=FAILURE_RESOLVE_ATTEMPT_1,
            attempt=1,
            event_type="FAIL",
            event_time="2026-09-30T14:00:08Z",
            error="The first source-resolution attempt timed out before delivery.",
        ),
        _rewrite_event(
            resolve_start,
            root_run_id=FAILURE_ROOT_RUN_ID,
            task_run_id=FAILURE_RESOLVE_ATTEMPT_2,
            attempt=2,
            event_time="2026-09-30T14:00:10Z",
        ),
        _rewrite_event(
            resolve_complete,
            root_run_id=FAILURE_ROOT_RUN_ID,
            task_run_id=FAILURE_RESOLVE_ATTEMPT_2,
            attempt=2,
            event_time="2026-09-30T14:00:15Z",
        ),
        _rewrite_event(
            prepare_start,
            root_run_id=FAILURE_ROOT_RUN_ID,
            task_run_id=FAILURE_PREPARE_RUN_ID,
            event_time="2026-09-30T14:00:16Z",
        ),
        _rewrite_event(
            prepare_complete,
            root_run_id=FAILURE_ROOT_RUN_ID,
            task_run_id=FAILURE_PREPARE_RUN_ID,
            event_time="2026-09-30T14:00:25Z",
        ),
        _rewrite_event(
            train_start,
            root_run_id=FAILURE_ROOT_RUN_ID,
            task_run_id=FAILURE_TRAIN_RUN_ID,
            event_time="2026-09-30T14:00:26Z",
        ),
        _rewrite_event(
            train_complete,
            root_run_id=FAILURE_ROOT_RUN_ID,
            task_run_id=FAILURE_TRAIN_RUN_ID,
            event_time="2026-09-30T14:00:38Z",
        ),
        _rewrite_event(
            root_complete, root_run_id=FAILURE_ROOT_RUN_ID, event_time="2026-09-30T14:00:40Z"
        ),
    )
    contexts = (
        _rewrite_context(
            happy.contexts[0],
            root_run_id=FAILURE_ROOT_RUN_ID,
            task_run_id=FAILURE_RESOLVE_ATTEMPT_1,
            attempt=1,
        ),
        _rewrite_context(
            happy.contexts[0],
            root_run_id=FAILURE_ROOT_RUN_ID,
            task_run_id=FAILURE_RESOLVE_ATTEMPT_2,
            attempt=2,
        ),
        _rewrite_context(
            happy.contexts[1],
            root_run_id=FAILURE_ROOT_RUN_ID,
            task_run_id=FAILURE_PREPARE_RUN_ID,
            attempt=1,
        ),
        _rewrite_context(
            happy.contexts[2],
            root_run_id=FAILURE_ROOT_RUN_ID,
            task_run_id=FAILURE_TRAIN_RUN_ID,
            attempt=1,
        ),
    )
    outbox = tuple(_rewrite_outbox(document, FAILURE_ROOT_RUN_ID) for document in happy.outbox)
    terminal_record = KFPOutboxRecord.from_document(outbox[-1])
    delivery_history = (
        terminal_record.document(),
        terminal_record.retry().document(),
        terminal_record.retry().retry().document(),
        terminal_record.retry().retry().dead_letter().document(),
    )
    metadata = {
        **happy.metadata,
        "scenarioId": "failure-retry",
        "scenarioStatus": "SUCCEEDED_AFTER_RETRY",
        "rootRunId": FAILURE_ROOT_RUN_ID,
        "evidence": "DEMO_FIXTURE",
        "retry": {
            "task": "resolve-and-stage-data",
            "failedAttempt": 1,
            "successfulAttempt": 2,
            "error": "The first source-resolution attempt timed out before delivery.",
        },
        "deadLetter": {
            "eventType": terminal_record.event_type.value,
            "attempts": 3,
            "meaning": "Lineage delivery failed independently of the workload outcome.",
        },
        "deliveryHistory": list(delivery_history),
    }
    return DemoScenario(
        scenario_id="failure-retry",
        title="A failed attempt is retained and recovered",
        description=(
            "The first source-resolution attempt fails, a second attempt succeeds, "
            "and an independent delivery dead-letter remains visible."
        ),
        events=retry_events,
        contexts=contexts,
        outbox=outbox,
        metadata=metadata,
    )


def _load_verified_evidence(fixture_root: Path) -> dict[str, Any]:
    return json.loads(
        (fixture_root / "examples/product-demo/verified-rhoai-run.json").read_text(encoding="utf-8")
    )


def build_product_demo(
    settings: Settings,
    fixture_root: Path,
    verified_evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build and validate both stakeholder scenarios and verified evidence."""

    happy = _scenario_from_showcase(build_showcase(settings))
    scenarios = [happy, _retry_scenario(happy)]
    document = {
        "product": "RHOAI lineage proof of concept",
        "contractProfile": "rhoai-kfp:1.0",
        "backend": "Marquez via OpenLineage, with offline replay mode",
        "scenarios": scenarios,
        "verifiedEvidence": verified_evidence or _load_verified_evidence(fixture_root),
    }
    document["auditDossier"] = build_audit_dossier(document)
    document["nativeValidation"] = build_native_path_validation(document)
    document["decisionPackage"] = build_decision_package(document)
    return document


def _validate_demo(document: dict[str, Any], contracts_dir: Path) -> None:
    context_schema = json.loads(
        (contracts_dir / "rhoai-kfp-lineage-context-1.0.schema.json").read_text(encoding="utf-8")
    )
    root_schema = json.loads(
        (contracts_dir / "rhoai-kfp-root-event-1.0.schema.json").read_text(encoding="utf-8")
    )
    context_validator = Draft202012Validator(context_schema)
    root_validator = Draft202012Validator(root_schema)
    for scenario in document["scenarios"]:
        for context in scenario.contexts:
            KFPLineageContext.from_document(context)
            if list(context_validator.iter_errors(context)):
                raise ValueError(f"invalid context in {scenario.scenario_id}")
        for outbox in scenario.outbox:
            KFPOutboxRecord.from_document(outbox)
        for event in scenario.events:
            if (
                event["job"]["namespace"].startswith("kfp://")
                and not event["inputs"]
                and list(root_validator.iter_errors(event))
            ):
                raise ValueError(f"invalid root event in {scenario.scenario_id}")


def _scenario_document(scenario: DemoScenario) -> dict[str, Any]:
    return scenario.document()


def _render_poc_brief(document: dict[str, Any]) -> str:
    """Render the PM/architecture decision brief from the same demo payload."""

    happy = next(item for item in document["scenarios"] if item.scenario_id == "happy-path")
    retry = next(item for item in document["scenarios"] if item.scenario_id == "failure-retry")
    metadata = happy.metadata
    evidence = document["verifiedEvidence"]
    dossier = document["auditDossier"]
    validation = document["nativeValidation"]
    decision = document["decisionPackage"]
    escape = html.escape
    limitations = "".join(f"<li>{escape(item)}</li>" for item in dossier["limitations"])
    validation_rows = "".join(
        f"<tr><td><strong>{escape(check['id'])}</strong></td><td>{escape(check['area'])}</td>"
        f"<td><span class=\"tag\">{escape(check['status'])}</span></td>"
        f"<td>{escape(check['evidence'])}</td></tr>"
        for check in validation["checks"]
    )
    journey_rows = "".join(
        f"<tr><td><strong>{escape(item['title'])}</strong></td>"
        f"<td>{escape(item['question'])}</td><td>{escape(item['result'])}</td>"
        f"<td>{escape(item['evidence'])}</td></tr>"
        for item in decision["userJourneys"]
    )
    capability_rows = "".join(
        f"<tr><td><strong>{escape(item['id'])}</strong></td><td>{escape(item['capability'])}</td>"
        f"<td><span class=\"tag\">{escape(item['state'])}</span></td>"
        f"<td>{escape(item['owner'])}</td><td>{escape(item['decisionImpact'])}</td></tr>"
        for item in decision["capabilityMatrix"]
    )
    boundary_rows = "".join(
        f"<tr><td><strong>{escape(item['boundary'])}</strong></td><td>{escape(item['owner'])}</td>"
        f"<td>{escape(item['trustedFact'])}</td><td>{escape(item['proofRequired'])}</td></tr>"
        for item in decision["trustBoundaries"]
    )
    option_rows = "".join(
        f"<tr><td><strong>{escape(item['name'])}</strong><br><span class=\"tag\">{escape(item['classification'])}</span></td>"
        f"<td>{escape(item['value'])}</td><td>{escape(item['tradeoff'])}</td><td>{escape(item['recommendation'])}</td></tr>"
        for item in decision["architectureOptions"]
    )
    scope_items = "".join(
        f"<li>{escape(item)}</li>" for item in decision["investmentRecommendation"]["scope"]
    )
    exit_items = "".join(
        f"<li>{escape(item)}</li>" for item in decision["investmentRecommendation"]["exitCriteria"]
    )
    live_section = ""
    live_kfp = evidence.get("kfp")
    live_lineage = evidence.get("openLineage")
    if isinstance(live_kfp, dict) and isinstance(live_lineage, dict):
        native_facet_observed = any(
            isinstance(event, dict)
            and isinstance(event.get("run"), dict)
            and isinstance(event["run"].get("facets"), dict)
            and "rhoaiKfp" in event["run"]["facets"]
            for event in live_lineage.get("events", [])
        )
        live_registry = evidence.get("dataRegistry", {})
        live_mlflow = evidence.get("mlflow", {})
        live_section = f"""
<section><h2>Installed-path observation [Observed]</h2><p><span class="tag warning">NO-GO AS NATIVE SUPPORT</span> The read-only snapshot confirms KFP identity/state, Registry metadata, MLflow correlation, and adapter-owned lineage, but native KFP context production is not evidenced.</p><table><thead><tr><th>System</th><th>Observed fact</th><th>Boundary</th></tr></thead><tbody>
<tr><td><strong>KFP</strong></td><td>{escape(str(live_kfp.get("state")))} · {escape(str(len(live_kfp.get("tasks", []))))} task records · {escape(str(len(live_kfp.get("stateHistory", []))))} state transitions</td><td>Exact run identity/state only</td></tr>
<tr><td><strong>Data Registry</strong></td><td>{escape(str(live_registry.get("assetId")))} · {escape(str(live_registry.get("name")))}</td><td>Read-only asset metadata</td></tr>
<tr><td><strong>MLflow</strong></td><td>{escape(str(live_mlflow.get("runId")))} · {escape(str(len(live_mlflow.get("metrics") or [])))} metrics</td><td>Correlated by root/asset tags</td></tr>
<tr><td><strong>Marquez</strong></td><td>{escape(str(live_lineage.get("eventCount")))} events · native <code>rhoaiKfp</code> facet: {escape(str(native_facet_observed))}</td><td>Adapter-owned events; no native producer observed</td></tr>
</tbody></table><p class="muted">[Proposed] Use this observation to support bounded discovery across the user journeys, architecture options, and native context/root seam; keep production adoption NO-GO until the supported context path, delivery model, and operational baseline are qualified.</p></section>"""
    ownership = "".join(
        f"<tr><td><strong>{escape(system)}</strong></td><td>{escape(owner)}</td>"
        f"<td>{escape(boundary)}</td></tr>"
        for system, owner, boundary in (
            (
                "Data Registry",
                "Governed asset identity",
                "Authoritative metadata and location reference; does not copy the data.",
            ),
            (
                "KFP",
                "Orchestration and lifecycle",
                "Owns the root run, task hierarchy, state, and retry context.",
            ),
            (
                "DCH / Spark",
                "Observed data edges",
                "Reports the physical reads, transformations, and derived datasets it performs.",
            ),
            (
                "MLflow / training",
                "Model outcome",
                "Reports evaluation metrics, candidate decision, and model artefact identity.",
            ),
            (
                "Lineage platform",
                "Cross-system view",
                "Correlates events and exposes an audit/replay view without inventing ownership.",
            ),
        )
    )
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>RHOAI lineage decision-ready demonstrator</title><style>
:root{{--ink:#162b3a;--muted:#5f6b76;--teal:#087f8c;--blue:#1769aa;--orange:#a65100;--line:#d9e1e8;--soft:#f4f7fa;--green:#086b3c}}
*{{box-sizing:border-box}}body{{margin:0;font:15px/1.52 system-ui,-apple-system,"Segoe UI",sans-serif;color:var(--ink);background:#eef2f5}}
main{{max-width:1180px;margin:0 auto;padding:28px 22px 60px}}header{{background:linear-gradient(120deg,#123b59,#126b75);color:#fff;border-radius:18px;padding:32px 36px;box-shadow:0 14px 32px #123b5926}}
.eyebrow{{text-transform:uppercase;letter-spacing:.12em;font-size:11px;opacity:.82}}h1{{margin:8px 0;font-size:34px;line-height:1.15}}header p{{max-width:820px;margin:0;opacity:.93;font-size:17px}}
.actions{{display:flex;gap:10px;flex-wrap:wrap;margin:18px 0}}.actions a{{background:#fff;color:var(--blue);padding:9px 14px;border-radius:999px;text-decoration:none;font-weight:700}}
section,.card{{background:#fff;border:1px solid var(--line);border-radius:14px;padding:20px;box-shadow:0 3px 10px #17324d0b}}section{{margin-top:16px}}h2{{margin:0 0 12px;font-size:22px}}h3{{margin-bottom:6px}}
.grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:16px 0}}.metric{{font-size:22px;font-weight:750;color:var(--blue)}}.label{{font-size:12px;color:var(--muted)}}
.flow{{display:flex;gap:9px;align-items:stretch;overflow:auto;padding:4px 0}}.node{{min-width:185px;background:var(--soft);border:1px solid var(--line);border-radius:11px;padding:13px}}.node strong{{display:block;margin-bottom:5px}}.arrow{{display:flex;align-items:center;color:var(--teal);font-size:22px}}
.tag{{display:inline-block;border-radius:999px;padding:3px 8px;margin:2px 3px 2px 0;background:#e5f4f4;color:#075b64;font-size:11px;font-weight:700}}.warning{{background:#fff1e4;color:var(--orange)}}.good{{color:var(--green);font-weight:700}}.muted{{color:var(--muted)}}
.columns{{display:grid;grid-template-columns:1fr 1fr;gap:16px}}.proves{{border-top:4px solid var(--green)}}.needs{{border-top:4px solid var(--orange)}}table{{width:100%;border-collapse:collapse;font-size:13px}}th,td{{text-align:left;padding:9px 7px;border-bottom:1px solid var(--line);vertical-align:top}}th{{color:var(--muted)}}code{{background:var(--soft);padding:2px 5px;border-radius:4px}}
@media(max-width:820px){{.grid{{grid-template-columns:repeat(2,1fr)}}.columns{{grid-template-columns:1fr}}h1{{font-size:28px}}}}
</style></head><body><main>
<header><div class="eyebrow">RHOAI lineage · {escape(decision["title"])}</div>
<h1>Can we make governed asset-to-model traceability useful and explainable?</h1>
<p>{escape(decision["objective"])} The demonstrator follows the happy path, makes recovery visible, and exposes the boundary between observed platform behavior, adapter behavior, and proposed product capability.</p>
<div class="actions"><a href="cockpit.html">Open interactive cockpit</a><a href="decision-package.json">Inspect decision package</a><a href="demo.json">Inspect replay data</a></div></header>
<div class="grid"><div class="card"><div class="metric">10–15 min</div><div class="label">stakeholder walkthrough</div></div><div class="card"><div class="metric">3</div><div class="label">user journeys</div></div><div class="card"><div class="metric">{escape(decision["investmentRecommendation"]["decision"])}</div><div class="label">investment posture</div></div><div class="card"><div class="metric">{escape(evidence["evidenceLevel"])}</div><div class="label">live evidence level</div></div></div>
<section><h2>The story in one picture</h2><div class="flow">
<div class="node"><strong>1 · Governed asset</strong><span class="tag">Data Registry</span><p class="muted">{escape(metadata["assetId"])}</p></div><div class="arrow">→</div>
<div class="node"><strong>2 · Orchestrated run</strong><span class="tag">KFP</span><p class="muted">root {escape(metadata["rootRunId"][:8])}…</p></div><div class="arrow">→</div>
<div class="node"><strong>3 · Processing + evaluation</strong><span class="tag">DCH / Spark</span><p class="muted">features, {escape(str(metadata["metrics"]["trainingRows"]))} training rows, {escape(str(metadata["metrics"]["holdoutAccuracy"]))} holdout accuracy</p></div><div class="arrow">→</div>
<div class="node"><strong>4 · Decision + audit</strong><span class="tag">MLflow / lineage</span><p class="muted">candidate model and explainable event trail</p></div></div></section>
<section><h2>Suggested talk track</h2><ol><li><strong>Start with the product question:</strong> ask which governed asset produced the candidate model and what evidence would make that answer trustworthy.</li><li><strong>Walk the happy path:</strong> follow the Registry asset, KFP execution, workload-owned data edges, evaluation signal, and model outcome.</li><li><strong>Make failure useful:</strong> show attempt 1 failing, attempt 2 succeeding, and delivery remaining independently auditable.</li><li><strong>Review the architecture:</strong> use the cockpit’s decision view to discuss trust boundaries, ownership, and capability states.</li><li><strong>Make the investment decision:</strong> agree whether the bounded discovery scope and exit criteria are worth funding.</li></ol></section>
<section><h2>Stakeholder journeys</h2><table><thead><tr><th>Journey</th><th>Question</th><th>Result</th><th>Evidence boundary</th></tr></thead><tbody>{journey_rows}</tbody></table></section>
<section><h2>Capability matrix</h2><p class="muted">The matrix separates what the demonstrator can show from what the platform or future product must still prove.</p><table><thead><tr><th>ID</th><th>Capability</th><th>State</th><th>Owner</th><th>Decision impact</th></tr></thead><tbody>{capability_rows}</tbody></table></section>
<section><h2>Architecture and trust boundaries</h2><p class="muted">The architecture is intentionally ownership-oriented: each boundary names the fact that can be trusted and the proof still required.</p><table><thead><tr><th>Boundary</th><th>Owner</th><th>Trusted fact</th><th>Proof still required</th></tr></thead><tbody>{boundary_rows}</tbody></table></section>
<section><h2>Native path checkpoint</h2><p><span class="tag">{escape(validation["status"])}</span> {escape(validation["recommendation"])} — four local checks passed against the <code>{escape(metadata["contractProfile"])}</code> fixture.</p><table><thead><tr><th>ID</th><th>Seam</th><th>Status</th><th>Evidence</th></tr></thead><tbody>{validation_rows}</tbody></table><p class="muted">This is [Observed] local contract evidence only. It supports a narrow native spike; it does not establish native KFP/RHOAI support or production readiness.</p></section>
{live_section}
<section><h2>What the POC proves vs. what production requires</h2><div class="columns"><div class="card proves"><h3 class="good">POC proves [Observed]</h3><ul><li>A governed asset can be the starting point for a coherent data-to-model narrative.</li><li>A shared root/context contract can correlate KFP-shaped orchestration with workload-owned lineage edges.</li><li>Success, retry, and independent delivery failure can be shown without hiding the original attempt.</li><li>A PM/architect can inspect the same story as a cockpit, event timeline, contract payload, and audit dossier.</li></ul></div><div class="card needs"><h3 style="color:var(--orange)">Production still requires [Proposed / Unknown]</h3><ul><li>Native KFP lifecycle and task-context production in a supported RHOAI integration.</li><li>Authoritative Data Registry revision/reproducibility semantics and exact source-byte evidence.</li><li>First-party DCH/Spark and MLflow ownership/adapters, plus durable delivery/reconciliation.</li><li>Operational ownership, upgrade qualification, security, retention, scale, and support gates.</li></ul></div></div></section>
<section><h2>Architecture and ownership</h2><table><thead><tr><th>System</th><th>Owns in the story</th><th>Boundary</th></tr></thead><tbody>{ownership}</tbody></table><p class="muted">The key architectural rule is simple: KFP owns orchestration context and lifecycle; each workload owns the data edges and model facts it actually observed; the lineage view correlates those facts.</p></section>
<section><h2>Architecture options and investment recommendation</h2><p><span class="tag">DECISION REQUEST</span> <span class="tag">{escape(decision["investmentRecommendation"]["decision"])}</span> <strong>{escape(decision["investmentRecommendation"]["headline"])}</strong></p><p>{escape(decision["investmentRecommendation"]["rationale"])}</p><table><thead><tr><th>Option</th><th>Value</th><th>Trade-off</th><th>Recommendation</th></tr></thead><tbody>{option_rows}</tbody></table><div class="columns"><div class="card proves"><h3 class="good">Bounded scope</h3><ul>{scope_items}</ul></div><div class="card needs"><h3 style="color:var(--orange)">Exit criteria</h3><ul>{exit_items}</ul></div></div></section>
<section><h2>Evidence note</h2><p>The default pack is deterministic and offline. The included RHOAI snapshot is labelled <span class="tag">{escape(evidence["evidenceLevel"])}</span> and is read-only evidence of KFP identity/state, not proof of native KFP OpenLineage production.</p><ul>{limitations}</ul><p class="muted">Retry scenario outcome: {escape(retry.metadata["scenarioStatus"])}; delivery outcome is intentionally independent of workload outcome.</p></section>
</main></body></html>"""


def _render_cockpit(document: dict[str, Any]) -> str:
    payload = json.dumps(
        {
            "contractProfile": document["contractProfile"],
            "backend": document["backend"],
            "scenarios": [_scenario_document(scenario) for scenario in document["scenarios"]],
            "verifiedEvidence": document["verifiedEvidence"],
            "auditDossier": document["auditDossier"],
            "nativeValidation": document["nativeValidation"],
            "decisionPackage": document["decisionPackage"],
        },
        separators=(",", ":"),
    ).replace("<", "\\u003c")
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        "<title>RHOAI Lineage Product Demonstrator</title><style>"
        ":root{--ink:#17212b;--muted:#5f6b76;--blue:#1769aa;--teal:#087f8c;--orange:#a65100;--line:#d9e1e8;--soft:#f4f7fa}"
        '*{box-sizing:border-box}body{margin:0;font:15px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif;color:var(--ink);background:#eef2f5}'
        "main{max-width:1240px;margin:0 auto;padding:30px 22px 60px}"
        "header{background:linear-gradient(120deg,#123b59,#126b75);color:white;border-radius:18px;padding:30px 34px;box-shadow:0 14px 32px #123b5926}"
        ".eyebrow{text-transform:uppercase;letter-spacing:.12em;font-size:11px;opacity:.82}h1{margin:8px 0;font-size:32px}header p{max-width:780px;margin:0;opacity:.92;font-size:16px}"
        ".toolbar{display:flex;flex-wrap:wrap;gap:8px;margin:20px 0 12px}button{border:1px solid var(--line);background:white;color:var(--ink);border-radius:999px;padding:9px 14px;cursor:pointer;font-weight:650}button.active{background:var(--blue);color:white;border-color:var(--blue)}"
        ".scenario{display:flex;gap:10px;flex-wrap:wrap}.scenario button{border-radius:10px}"
        ".grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:16px 0}.card,section{background:white;border:1px solid var(--line);border-radius:14px;padding:18px;box-shadow:0 3px 10px #17324d0b}.metric{color:var(--blue);font-size:22px;font-weight:750}.label{color:var(--muted);font-size:12px}"
        ".flow{display:flex;gap:9px;align-items:stretch;overflow:auto;padding:4px 0}.node{min-width:175px;background:var(--soft);border:1px solid var(--line);border-radius:11px;padding:13px}.node strong{display:block;margin-bottom:5px}.arrow{display:flex;align-items:center;color:var(--teal);font-size:22px}.tag{display:inline-block;border-radius:999px;padding:3px 8px;margin:2px 3px 2px 0;background:#e5f4f4;color:#075b64;font-size:11px;font-weight:700}.warning{background:#fff1e4;color:var(--orange)}"
        "table{width:100%;border-collapse:collapse;font-size:13px}th,td{text-align:left;padding:9px 7px;border-bottom:1px solid var(--line);vertical-align:top}th{color:var(--muted)}code{background:var(--soft);padding:2px 5px;border-radius:4px}.muted{color:var(--muted)}.verified{color:#086b3c;font-weight:700}"
        "@media(max-width:780px){.grid{grid-template-columns:repeat(2,1fr)}h1{font-size:27px}}"
        '</style></head><body><main><header><div class="eyebrow">RHOAI product demonstrator · KFP + OpenLineage + Data Registry</div>'
        "<h1>From governed data to an explainable model</h1><p>This cockpit lets a product manager or architect switch between a successful governed run and a recovered failure, inspect the same lineage story as events, contracts, and audit evidence, then compare architecture options and decide what to invest in.</p></header>"
        '<div class="scenario" id="scenarioButtons"></div><div class="toolbar" id="viewButtons"></div><div id="content"></div>'
        "<script>const DEMO="
        + payload
        + r""";let selected=0;let view="overview";
const esc=function(v){return String(v==null?"":v).replace(/[&<>\"']/g,function(c){return {"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c]})};
const scenario=function(){return DEMO.scenarios[selected]};
const row=function(e){const k=e.run&&e.run.facets&&e.run.facets.rhoaiKfp||{};const t=k.task||{};return "<tr><td>"+esc(e.eventTime)+"</td><td><strong>"+esc(e.eventType)+"</strong></td><td>"+esc(e.job.name)+"</td><td><code>"+esc((e.run.runId||"").slice(0,8))+"…</code></td><td>"+esc(t.attempt==null?"root":t.attempt)+"</td><td>"+e.inputs.length+" in / "+e.outputs.length+" out</td></tr>"};
function render(){const s=scenario();const m=s.metadata;document.getElementById("scenarioButtons").innerHTML=DEMO.scenarios.map(function(x,i){return "<button class=\""+(i===selected?"active":"")+"\" onclick=\"pickScenario("+i+")\">"+esc(x.title)+"</button>"}).join("");
document.getElementById("viewButtons").innerHTML=["overview","lineage","timeline","dossier","decision","evidence"].map(function(x){return "<button class=\""+(x===view?"active":"")+"\" onclick=\"pickView('"+x+"')\">"+x.charAt(0).toUpperCase()+x.slice(1)+"</button>"}).join("");
let body="";
if(view==="overview"){body="<section><h2>"+esc(s.title)+"</h2><p>"+esc(s.description)+"</p><div class=\"grid\"><div class=\"card\"><div class=\"metric\">"+esc(m.scenarioStatus)+"</div><div class=\"label\">outcome</div></div><div class=\"card\"><div class=\"metric\">"+s.events.length+"</div><div class=\"label\">events</div></div><div class=\"card\"><div class=\"metric\">"+esc(m.contractProfile)+"</div><div class=\"label\">contract profile</div></div><div class=\"card\"><div class=\"metric\">"+esc(m.evidence)+"</div><div class=\"label\">evidence level</div></div></div><div class=\"flow\"><div class=\"node\"><strong>Data Registry asset</strong><span class=\"tag\">governed</span><p class=\"muted\">"+esc(m.assetId)+"</p></div><div class=\"arrow\">→</div><div class=\"node\"><strong>KFP root</strong><span class=\"tag\">orchestration</span><p class=\"muted\">"+esc(m.rootRunId.slice(0,8))+"…</p></div><div class=\"arrow\">→</div><div class=\"node\"><strong>Feature preparation</strong><span class=\"tag\">Spark</span><p class=\"muted\">derived feature table</p></div><div class=\"arrow\">→</div><div class=\"node\"><strong>Candidate model</strong><span class=\"tag\">MLflow</span><p class=\"muted\">"+esc(m.model)+"</p></div></div></section>";if(m.retry){body+="<section><h2>Recovery story</h2><p><span class=\"tag warning\">attempt "+m.retry.failedAttempt+" failed</span> "+esc(m.retry.error)+" <span class=\"tag\">attempt "+m.retry.successfulAttempt+" succeeded</span></p><p class=\"muted\">"+esc(m.deadLetter.meaning)+"</p></section>"}}
if(view==="lineage"){const edges=[];s.events.forEach(function(e){e.inputs.forEach(function(x){edges.push(x.namespace+"/"+x.name+" → "+e.job.name)});e.outputs.forEach(function(x){edges.push(e.job.name+" → "+x.namespace+"/"+x.name)})});body="<section><h2>Lineage edges</h2><p class=\"muted\">These are workload-reported data edges. The KFP root provides execution context but does not fabricate them.</p><ul>"+edges.map(function(x){return "<li><code>"+esc(x)+"</code></li>"}).join("")+"</ul></section>"}
if(view==="timeline")body="<section><h2>Audit timeline</h2><table><thead><tr><th>Time</th><th>State</th><th>Job</th><th>Run</th><th>Attempt</th><th>Edges</th></tr></thead><tbody>"+s.events.map(row).join("")+"</tbody></table></section>";
if(view==="dossier"){const d=DEMO.auditDossier;body="<section><h2>Audit dossier</h2><p>"+esc(d.purpose)+"</p><div class=\"grid\"><div class=\"card\"><div class=\"metric\">"+esc(d.trustLevel)+"</div><div class=\"label\">trust level</div></div><div class=\"card\"><div class=\"metric\">"+esc(d.subject.kfpRun.state)+"</div><div class=\"label\">live KFP state</div></div><div class=\"card\"><div class=\"metric\">"+d.systems.length+"</div><div class=\"label\">system evidence sources</div></div><div class=\"card\"><div class=\"metric\">"+d.limitations.length+"</div><div class=\"label\">known gaps</div></div></div><h3>Evidence by system</h3><table><thead><tr><th>System</th><th>Status</th><th>Key facts</th></tr></thead><tbody>"+d.systems.map(function(x){return "<tr><td><strong>"+esc(x.system)+"</strong></td><td><span class=\"tag\">"+esc(x.status)+"</span></td><td><code>"+esc(JSON.stringify(x.facts))+"</code></td></tr>"}).join("")+"</tbody></table><h3>Known gaps</h3><ul>"+d.limitations.map(function(x){return "<li>"+esc(x)+"</li>"}).join("")+"</ul></section>"}
if(view==="decision"){const d=DEMO.decisionPackage;body="<section><h2>Decision view</h2><p>"+esc(d.objective)+"</p><div class=\"grid\"><div class=\"card\"><div class=\"metric\">"+esc(d.investmentRecommendation.decision)+"</div><div class=\"label\">investment posture</div></div><div class=\"card\"><div class=\"metric\">"+d.userJourneys.length+"</div><div class=\"label\">user journeys</div></div><div class=\"card\"><div class=\"metric\">"+d.capabilityMatrix.length+"</div><div class=\"label\">capabilities assessed</div></div><div class=\"card\"><div class=\"metric\">"+d.trustBoundaries.length+"</div><div class=\"label\">trust boundaries</div></div></div><h3>Recommendation</h3><p><strong>"+esc(d.investmentRecommendation.headline)+"</strong> "+esc(d.investmentRecommendation.rationale)+"</p><h3>Capability matrix</h3><table><thead><tr><th>Capability</th><th>State</th><th>Owner</th><th>Decision impact</th></tr></thead><tbody>"+d.capabilityMatrix.map(function(x){return "<tr><td><strong>"+esc(x.capability)+"</strong></td><td><span class=\"tag\">"+esc(x.state)+"</span></td><td>"+esc(x.owner)+"</td><td>"+esc(x.decisionImpact)+"</td></tr>"}).join("")+"</tbody></table><h3>Architecture options</h3><table><thead><tr><th>Option</th><th>Value</th><th>Trade-off</th><th>Recommendation</th></tr></thead><tbody>"+d.architectureOptions.map(function(x){return "<tr><td><strong>"+esc(x.name)+"</strong></td><td>"+esc(x.value)+"</td><td>"+esc(x.tradeoff)+"</td><td>"+esc(x.recommendation)+"</td></tr>"}).join("")+"</tbody></table><h3>Exit criteria</h3><ul>"+d.investmentRecommendation.exitCriteria.map(function(x){return "<li>"+esc(x)+"</li>"}).join("")+"</ul></section>"}
if(view==="evidence")body="<section><h2>Evidence and boundaries</h2><p><span class=\"tag\">"+esc(m.evidence)+"</span> "+esc(m.scenarioStatus)+"</p><ul>"+(m.claims||[]).map(function(x){return "<li>"+esc(x)+"</li>"}).join("")+"</ul><h3>Native path checkpoint</h3><p><span class=\"tag\">"+esc(DEMO.nativeValidation.status)+"</span> "+esc(DEMO.nativeValidation.recommendation)+" · local fixture only</p><ul>"+DEMO.nativeValidation.checks.map(function(x){return "<li><strong>"+esc(x.id)+" · "+esc(x.area)+"</strong>: "+esc(x.status)+" — "+esc(x.evidence)+"</li>"}).join("")+"</ul><h3>Verified RHOAI snapshot</h3><p class=\"verified\">"+esc(DEMO.verifiedEvidence.evidenceLevel)+"</p><p>"+esc(DEMO.verifiedEvidence.summary)+"</p><p class=\"muted\">Run "+esc(DEMO.verifiedEvidence.runId)+" · pipeline "+esc(DEMO.verifiedEvidence.pipelineId)+" · state "+esc(DEMO.verifiedEvidence.state)+"</p></section>";
document.getElementById("content").innerHTML=body}
function pickScenario(i){selected=i;view="overview";render()}function pickView(x){view=x;render()}render();</script></main></body></html>"""
    )


def write_product_demo(
    document: dict[str, Any],
    output_dir: Path,
    fixture_root: Path,
    *,
    emit: bool = False,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Validate and write the complete replay/cockpit artefact pack."""

    _validate_demo(document, fixture_root / "contracts")
    output_dir.mkdir(parents=True, exist_ok=True)
    serializable = {
        "product": document["product"],
        "contractProfile": document["contractProfile"],
        "backend": document["backend"],
        "verifiedEvidence": document["verifiedEvidence"],
        "auditDossier": document["auditDossier"],
        "nativeValidation": document["nativeValidation"],
        "decisionPackage": document["decisionPackage"],
        "scenarios": [_scenario_document(scenario) for scenario in document["scenarios"]],
    }
    (output_dir / "demo.json").write_text(
        json.dumps(serializable, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    for scenario in document["scenarios"]:
        scenario_dir = output_dir / "scenarios" / scenario.scenario_id
        scenario_dir.mkdir(parents=True, exist_ok=True)
        for name, value in (
            ("events.json", scenario.events),
            ("contexts.json", scenario.contexts),
            ("outbox.json", scenario.outbox),
        ):
            (scenario_dir / name).write_text(
                json.dumps(list(value), indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
    (output_dir / "verified-rhoai-run.json").write_text(
        json.dumps(document["verifiedEvidence"], indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (output_dir / "native-path-validation.json").write_text(
        json.dumps(document["nativeValidation"], indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (output_dir / "decision-package.json").write_text(
        json.dumps(document["decisionPackage"], indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (output_dir / "cockpit.html").write_text(_render_cockpit(document), encoding="utf-8")
    (output_dir / "poc-brief.html").write_text(_render_poc_brief(document), encoding="utf-8")

    emitted = 0
    if emit:
        from lineage_demo.events import LineageEmitter

        emitter = LineageEmitter(settings or Settings())
        try:
            for event in document["scenarios"][0].typed_events:
                emitter.emit(event)
                emitted += 1
        finally:
            emitter.close()
    return {
        "outputDir": str(output_dir),
        "cockpit": str(output_dir / "cockpit.html"),
        "pocBrief": str(output_dir / "poc-brief.html"),
        "nativeValidation": str(output_dir / "native-path-validation.json"),
        "decisionPackage": str(output_dir / "decision-package.json"),
        "replayData": str(output_dir / "demo.json"),
        "scenarioCount": len(document["scenarios"]),
        "eventCounts": {s.scenario_id: len(s.events) for s in document["scenarios"]},
        "emittedHappyPathEventsToMarquez": emitted,
        "verifiedEvidence": document["verifiedEvidence"]["evidenceLevel"],
    }
