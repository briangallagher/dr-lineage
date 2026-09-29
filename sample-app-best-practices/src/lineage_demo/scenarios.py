"""Submit the learning scenarios and verify the raw events persisted by Marquez."""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, TextIO

import boto3
import httpx
import kfp
import urllib3
from urllib3.exceptions import InsecureRequestWarning

# The KFP endpoint is reached only through an authenticated localhost port-forward;
# its cluster certificate cannot validate for 127.0.0.1.
urllib3.disable_warnings(InsecureRequestWarning)


@dataclass
class RunResult:
    scenario: str
    run_id: str
    expected_state: str
    actual_state: str


def _kfp_client(endpoint: str, token: str, namespace: str) -> kfp.Client:
    return kfp.Client(
        host=endpoint,
        existing_token=token or None,
        namespace=namespace,
        verify_ssl=False,
    )


def read_credentials(stream: TextIO) -> tuple[str, str, str]:
    """Read local test credentials from stdin, keeping them out of argv and reports."""

    values: list[str] = []
    for label in ("KFP token", "S3 access key", "S3 secret key"):
        value = stream.readline().rstrip("\n")
        if not value:
            raise ValueError(f"Missing {label} on credentials stdin")
        values.append(value)
    return values[0], values[1], values[2]


def _state(run: Any) -> str:
    value = getattr(run, "state", "")
    return getattr(value, "value", value) or "UNKNOWN"


def run_pipeline(
    client: kfp.Client,
    *,
    package: str,
    namespace: str,
    asset_id: str,
    failure_mode: str,
    scenario: str,
    timeout: int,
) -> RunResult:
    submitted = client.create_run_from_pipeline_package(
        pipeline_file=package,
        arguments={"asset_id": asset_id, "failure_mode": failure_mode},
        run_name=f"openlineage-{scenario}-{int(time.time())}",
        experiment_name="OpenLineage best practices",
        namespace=namespace,
        enable_caching=False,
        service_account="pipeline-runner-dspa",
    )
    completed = client.wait_for_run_completion(submitted.run_id, timeout=timeout)
    actual = str(_state(completed)).upper()
    expected = "SUCCEEDED" if failure_mode == "none" else "FAILED"
    if actual != expected:
        raise RuntimeError(f"Scenario {scenario} expected {expected}, got {actual}")
    return RunResult(scenario, submitted.run_id, expected, actual)


def register_asset(registry_url: str) -> dict:
    response = httpx.post(
        f"{registry_url.rstrip('/')}/v1/assets",
        headers={"Idempotency-Key": "default-source"},
        json={
            "name": "Customer documents",
            "description": "Mutable CSV source with no durable row or content identifier",
            "owner": "data-science",
            "location": "s3://sample-data/raw/documents.csv",
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def event_count(marquez_url: str) -> int:
    response = httpx.get(
        f"{marquez_url.rstrip('/')}/api/v1/events/lineage",
        params={"limit": 10000},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    return int(payload.get("totalCount", len(payload.get("events", []))))


def bypass_overwrite(
    *,
    endpoint: str,
    access_key: str,
    secret_key: str,
    source: Path,
) -> None:
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    client.put_object(
        Bucket="sample-data",
        Key="raw/documents.csv",
        Body=source.read_bytes(),
    )


def execute(args: argparse.Namespace) -> dict:
    client = _kfp_client(args.kfp_endpoint, args.kfp_token, args.namespace)
    asset = register_asset(args.registry_url)
    results: list[RunResult] = []
    for ordinal in (1, 2):
        results.append(
            run_pipeline(
                client,
                package=args.pipeline,
                namespace=args.namespace,
                asset_id=asset["assetId"],
                failure_mode="none",
                scenario=f"success-{ordinal}",
                timeout=args.timeout,
            )
        )

    # A KFP run can finish just before Marquez commits the listener's last event.
    # Establish a quiet baseline before proving that an uninstrumented write is invisible.
    time.sleep(5)
    before = event_count(args.marquez_url)
    bypass_overwrite(
        endpoint=args.s3_endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
        source=Path(args.source_v2),
    )
    time.sleep(2)
    after = event_count(args.marquez_url)
    if before != after:
        raise RuntimeError(
            f"Bypass overwrite unexpectedly changed lineage event count: {before} -> {after}"
        )

    results.append(
        run_pipeline(
            client,
            package=args.pipeline,
            namespace=args.namespace,
            asset_id=asset["assetId"],
            failure_mode="none",
            scenario="success-after-bypass",
            timeout=args.timeout,
        )
    )
    for failure_mode in ("ingest", "spark", "embed"):
        results.append(
            run_pipeline(
                client,
                package=args.pipeline,
                namespace=args.namespace,
                asset_id=asset["assetId"],
                failure_mode=failure_mode,
                scenario=f"failure-{failure_mode}",
                timeout=args.timeout,
            )
        )

    report = {
        "asset": asset,
        "bypass": {"eventCountBefore": before, "eventCountAfter": after},
        "runs": [asdict(result) for result in results],
    }
    destination = Path(args.output)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--kfp-endpoint", required=True)
    parser.add_argument("--credentials-stdin", action="store_true", required=True)
    parser.add_argument("--registry-url", required=True)
    parser.add_argument("--marquez-url", required=True)
    parser.add_argument("--s3-endpoint", required=True)
    parser.add_argument("--pipeline", required=True)
    parser.add_argument("--source-v2", required=True)
    parser.add_argument("--namespace", default="ol-best-practices")
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--output", default="build/scenario-results.json")
    args = parser.parse_args()
    try:
        args.kfp_token, args.access_key, args.secret_key = read_credentials(sys.stdin)
    except ValueError as error:
        parser.error(str(error))
    print(json.dumps(execute(args), indent=2))


if __name__ == "__main__":
    main()
