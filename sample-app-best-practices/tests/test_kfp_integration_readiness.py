from __future__ import annotations

import json
import stat

import pytest

from lineage_demo.kfp_integration_readiness import (
    CONTEXT_PATH,
    CONTEXT_PATH_ENV,
    FORWARDED_KFP_FLAGS,
    PROFILE_VERSION_ENV,
    build_launcher_context_plan,
    materialize_runtime_context,
    read_materialized_context,
)
from lineage_demo.kfp_lineage import KFPLineageContractError
from tests.test_kfp_lineage import context


def test_launcher_plan_uses_exact_root_id_and_observed_kfp_override_seams() -> None:
    plan = build_launcher_context_plan(context(), resolver_url="https://lineage.svc")

    assert plan.lookup_url.endswith(f"/v1/lineage/contexts/{context().root_run_id}")
    assert plan.runtime_environment() == {
        CONTEXT_PATH_ENV: CONTEXT_PATH,
        PROFILE_VERSION_ENV: "1.0",
    }
    document = plan.document()
    assert document["mechanism"] == "kfp-v2-launcher-wrapper-context-resolver"
    assert document["resolver"]["lookupKey"] == "exact-kfp-root-run-id"
    assert document["launcherOverride"]["preservesFlags"] == list(FORWARDED_KFP_FLAGS)
    assert "pod_name" not in document["rootContext"]
    assert "pod_uid" not in document["rootContext"]


def test_launcher_plan_contains_no_credentials_or_signed_locations() -> None:
    plan = build_launcher_context_plan(context(), resolver_url="https://lineage.svc")
    serialized = json.dumps(plan.document()).lower()

    assert all(
        secret not in serialized
        for secret in ("token", "password", "credential", "secret", "signedurl")
    )


def test_launcher_plan_materializes_a_read_only_context_file(tmp_path) -> None:
    plan = build_launcher_context_plan(context(), resolver_url="https://lineage.svc")

    path, environment = materialize_runtime_context(plan, tmp_path / "runtime")

    assert path.read_text(encoding="utf-8") == json.dumps(
        context().document(), indent=2, sort_keys=True
    ) + "\n"
    assert stat.S_IMODE(path.stat().st_mode) == 0o444
    assert environment[CONTEXT_PATH_ENV] == CONTEXT_PATH
    assert read_materialized_context(path).root_run_id == context().root_run_id


@pytest.mark.parametrize(
    "resolver_url",
    [
        "lineage.svc",
        "https://user:password@lineage.svc",
        "https://lineage.svc?token=hidden",
        "https://lineage.svc/#secret",
    ],
)
def test_launcher_plan_rejects_unsafe_resolver_urls(resolver_url: str) -> None:
    with pytest.raises(KFPLineageContractError, match="resolver URL"):
        build_launcher_context_plan(context(), resolver_url=resolver_url)


def test_launcher_plan_rejects_task_context_as_a_root_context() -> None:
    task_context = context().task(
        name="train-governed-baseline",
        task_id="train-governed-baseline",
        run_id="a2fef4ea-9654-4e6d-9b64-53599bbf33cd",
    )

    with pytest.raises(KFPLineageContractError, match="root context"):
        build_launcher_context_plan(task_context, resolver_url="https://lineage.svc")


def test_materialized_context_reader_does_not_echo_malformed_contents(tmp_path) -> None:
    path = tmp_path / "context.json"
    path.write_text('{"profile":"rhoai-kfp","token":"do-not-echo"}', encoding="utf-8")

    with pytest.raises(KFPLineageContractError) as exc_info:
        read_materialized_context(path)

    assert "do-not-echo" not in str(exc_info.value)
