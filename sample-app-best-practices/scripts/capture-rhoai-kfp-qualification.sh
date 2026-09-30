#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
APP_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
cd "$APP_DIR"

NAMESPACE=${RHOAI_KFP_NAMESPACE:-scenario-b}
DSPA_NAME=${RHOAI_KFP_DSPA_NAME:-dspa}
DEPLOYMENT_NAME=${RHOAI_KFP_DEPLOYMENT_NAME:-ds-pipeline-dspa}
TASK_POD=${RHOAI_KFP_TASK_POD:-governed-asset-to-model-8gddt-system-container-impl-3191863202}
CSV_NAMESPACE=${RHOAI_CSV_NAMESPACE:-rhoai-model-registries}
CSV_NAME=${RHOAI_CSV_NAME:-rhods-operator.3.6.0-ea.1}
OPERATOR_NAMESPACE=${RHOAI_OPERATOR_NAMESPACE:-redhat-ods-applications}
OPERATOR_DEPLOYMENT=${RHOAI_OPERATOR_DEPLOYMENT:-data-science-pipelines-operator-controller-manager}
OBSERVED_AT=${RHOAI_QUALIFICATION_OBSERVED_AT:-$(date -u +%F)}
OUTPUT=${RHOAI_QUALIFICATION_OUTPUT:-examples/rhoai-kfp-qualification/current-baseline.json}

TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

oc get csv "$CSV_NAME" -n "$CSV_NAMESPACE" -o json >"$TMP_DIR/csv.json"
oc get deployment "$DEPLOYMENT_NAME" -n "$NAMESPACE" -o json >"$TMP_DIR/api-server.json"
oc get deployment "$OPERATOR_DEPLOYMENT" -n "$OPERATOR_NAMESPACE" -o json >"$TMP_DIR/operator.json"
oc get pod "$TASK_POD" -n "$NAMESPACE" -o json >"$TMP_DIR/task-pod.json"

mkdir -p "$(dirname "$OUTPUT")"

jq -n \
  --arg observed_at "$OBSERVED_AT" \
  --arg namespace "$NAMESPACE" \
  --arg dspa_name "$DSPA_NAME" \
  --arg deployment_name "$DEPLOYMENT_NAME" \
  --slurpfile csv "$TMP_DIR/csv.json" \
  --slurpfile api_server "$TMP_DIR/api-server.json" \
  --slurpfile operator "$TMP_DIR/operator.json" \
  --slurpfile task_pod "$TMP_DIR/task-pod.json" \
  '
  def one($value): $value[0];
  def image($fragment): first(one($csv).spec.relatedImages[]?.image | select(contains($fragment)));
  def env_setting($envs; $name):
    [$envs[]? | select(.name == $name)] as $matches
    | if ($matches | length) == 0
      then {configured: false}
      else {configured: true, value: ($matches[0].value // null)}
      end;
  def context_env_names($envs): [$envs[]?.name | select(test("^RHOAI_LINEAGE_"))];
  def context_volume_present($volumes):
    any($volumes[]?.name; test("(^|[-_])(lineage|context)([-_]|$)"; "i"));
  def flag_names($command):
    [$command[]? | select(type == "string" and startswith("--")) | sub("=.*$"; "")];
  def coverage($id; $status; $evidence): {id: $id, status: $status, evidence: $evidence};
  (one($api_server).spec.template.spec.containers[] | select(.name == "ds-pipeline-api-server")) as $api
  | (one($operator).spec.template.spec.containers[] | select(.name == "manager")) as $manager
  | (one($task_pod).spec.initContainers[] | select(.name == "kfp-launcher")) as $launcher
  | (one($task_pod).spec.containers[] | select(.name == "main")) as $main
  | (one($operator).spec.template.spec.containers[0].env[]? | select(.name == "DSPO_PLATFORMVERSION").value) as $platform_version
  | {
      schemaVersion: "rhoai-kfp-qualification:1.0",
      observedAt: $observed_at,
      environment: {
        platformVersion: ($platform_version // "unknown"),
        namespace: $namespace,
        dspaName: $dspa_name,
        deployment: $deployment_name
      },
      release: {
        csvName: one($csv).metadata.name,
        version: one($csv).spec.version,
        phase: one($csv).status.phase,
        repository: one($csv).metadata.annotations.repository,
        createdAt: one($csv).metadata.annotations.createdAt,
        relatedImages: [
          one($csv).spec.relatedImages[]?.image
          | select(contains("data-science-pipelines") or contains("ml-pipelines"))
        ]
      },
      images: {
        dspo: image("odh-data-science-pipelines-operator-controller-rhel9"),
        apiServer: image("odh-ml-pipelines-api-server-v2-rhel9"),
        launcher: image("odh-ml-pipelines-launcher-rhel9"),
        driver: image("odh-ml-pipelines-driver-rhel9"),
        argoExec: image("odh-data-science-pipelines-argo-argoexec-rhel9"),
        argoWorkflowController: image("odh-data-science-pipelines-argo-workflowcontroller-rhel9")
      },
      apiServer: {
        image: $api.image,
        serviceAccount: one($api_server).spec.template.spec.serviceAccountName,
        command: ($api.command // []),
        args: ($api.args // []),
        configHash: one($api_server).spec.template.metadata.annotations.configHash,
        launcherEnv: {
          V2_LAUNCHER_IMAGE: env_setting($api.env; "V2_LAUNCHER_IMAGE"),
          V2_LAUNCHER_COMMAND: env_setting($api.env; "V2_LAUNCHER_COMMAND"),
          V2_DRIVER_IMAGE: env_setting($api.env; "V2_DRIVER_IMAGE")
        },
        contextInjection: {
          contextPathEnvNames: context_env_names($api.env),
          contextVolumePresent: context_volume_present(one($api_server).spec.template.spec.volumes),
          resolverConfigured: false,
          outboxConfigured: false
        }
      },
      taskPod: {
        name: one($task_pod).metadata.name,
        phase: one($task_pod).status.phase,
        launcher: {
          image: $launcher.image,
          command: ($launcher.command // []),
          args: ($launcher.args // []),
          contextEnvNames: context_env_names($launcher.env),
          contextVolumePresent: context_volume_present(one($task_pod).spec.volumes)
        },
        userContainer: {
          launcherPath: "/kfp-launcher/launch",
          flagNames: flag_names($main.command),
          envNames: [$main.env[]?.name],
          contextVolumePresent: context_volume_present(one($task_pod).spec.volumes)
        },
        contextInjection: {
          contextPathEnvNames: context_env_names($main.env),
          contextVolumePresent: context_volume_present(one($task_pod).spec.volumes),
          resolverConfigured: false,
          outboxConfigured: false
        }
      },
      provenance: {
        releaseMapping: {
          status: "observed",
          source: "installed OLM CSV relatedImages",
          result: one($csv).metadata.name
        },
        componentSourceRefs: {
          status: "unknown",
          source: "installed release manifest does not expose component source commits"
        },
        registryMetadata: {
          status: "unknown",
          source: "not collected by the cluster capture script"
        },
        ownerApi: {
          status: "unknown",
          source: "owner approval is not represented in cluster state"
        }
      },
      coverage: {
        contractChecks: [
          coverage("C-01"; "local-exercised"; "root context and START projection tests"),
          coverage("C-02"; "local-exercised"; "terminal COMPLETE projection tests"),
          coverage("C-03"; "local-exercised"; "FAIL and ABORT projection tests"),
          coverage("C-04"; "local-exercised"; "state-history and idempotency tests"),
          coverage("C-05"; "local-exercised"; "launcher context plan and materialization tests"),
          coverage("C-06"; "local-exercised"; "missing-context diagnostic tests"),
          coverage("C-07"; "local-exercised"; "retry child identity tests"),
          coverage("C-08"; "local-exercised"; "child ownership contract tests"),
          coverage("C-09"; "planned"; "native outbox outage test not implemented"),
          coverage("C-10"; "local-exercised"; "replay and conflicting-payload tests"),
          coverage("C-11"; "planned"; "native cross-project authorization test not implemented"),
          coverage("C-12"; "local-exercised"; "secret-safety tests"),
          coverage("C-13"; "local-exercised"; "launcher flag preservation tests"),
          coverage("C-14"; "planned"; "upgrade qualification not run")
        ],
        upgradeScenarios: [
          coverage("U-01-dspo-image"; "planned"; "rerun release mapping and operator reconciliation checks"),
          coverage("U-02-api-server-image"; "planned"; "rerun API-server launcher settings and task-pod shape checks"),
          coverage("U-03-launcher-abi"; "planned"; "rerun wrapper flag, retry, cancellation, and exit checks"),
          coverage("U-04-context-contract"; "planned"; "rerun schema, authorization, outage, and replay checks")
        ]
      }
    }
  ' \
  >"$OUTPUT"

UV_CACHE_DIR=${UV_CACHE_DIR:-/tmp/dr-lineage-uv-cache}
export UV_CACHE_DIR
uv run python -m lineage_demo.rhoai_kfp_qualification "$OUTPUT" --expect NO_GO >/dev/null
echo "Wrote sanitized qualification baseline to $OUTPUT"
