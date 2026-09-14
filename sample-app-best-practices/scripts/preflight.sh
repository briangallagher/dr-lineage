#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

for command in oc uv jq curl openssl; do
  require_command "$command"
done

oc whoami >/dev/null

cluster_state="$(oc get datasciencecluster -o json | jq -r '
  [.items[]
   | (.spec.components.aipipelines.managementState
      // .spec.components.datasciencepipelines.managementState
      // empty)]
  | first // empty
')"
if [[ "$cluster_state" != "Managed" ]]; then
  echo "Preflight failed: RHOAI AI Pipelines is not Managed (state: ${cluster_state:-missing})." >&2
  exit 1
fi

if ! oc get pods -n redhat-ods-applications \
  -l app.kubernetes.io/name=data-science-pipelines-operator \
  -o json | jq -e '.items | any(.status.phase == "Running")' >/dev/null; then
  echo "Preflight failed: no running Data Science Pipelines Operator pod was found." >&2
  exit 1
fi

required_crds=(
  datasciencepipelinesapplications.datasciencepipelinesapplications.opendatahub.io
  sparkapplications.sparkoperator.k8s.io
)
missing_crds=()
for crd in "${required_crds[@]}"; do
  if ! oc get crd "$crd" >/dev/null 2>&1; then
    missing_crds+=("$crd")
  fi
done
if ((${#missing_crds[@]})); then
  echo "Preflight failed: the connected cluster is missing required operator APIs:" >&2
  printf '  - %s\n' "${missing_crds[@]}" >&2
  echo "Install or select a cluster with RHOAI Data Science Pipelines and the Spark Operator, then retry." >&2
  exit 1
fi

spark_state="$(oc get datasciencecluster -o json | jq -r '
  [.items[] | .spec.components.sparkoperator.managementState // empty]
  | first // empty
')"
if [[ "$spark_state" != "Managed" ]]; then
  echo "Preflight failed: the RHOAI Spark Operator is not Managed (state: ${spark_state:-missing})." >&2
  exit 1
fi

oc get storageclass >/dev/null
oc get service image-registry -n openshift-image-registry >/dev/null

if ! oc auth can-i create namespaces | grep -q '^yes$' \
  && ! oc auth can-i create projectrequests.project.openshift.io | grep -q '^yes$'; then
  echo "Current user cannot create a project" >&2
  exit 1
fi

echo "Preflight passed for $(oc whoami) on $(oc whoami --show-server)"
