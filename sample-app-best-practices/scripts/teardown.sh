#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

if [[ "${1:-}" != "--yes" ]]; then
  read -r -p "Delete project $NAMESPACE and all demo data? [y/N] " answer
  [[ "$answer" == "y" || "$answer" == "Y" ]] || exit 0
fi
oc delete project "$NAMESPACE"
echo "Project $NAMESPACE was deleted; recovery requires redeployment."

