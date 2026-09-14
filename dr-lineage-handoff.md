# Session Handoff: OpenLineage Data Registry Learning Application

## Current objective

Continue work on the OpenLineage Data Registry learning application in:

`/Users/briangallagher/dev/workspaces/dr-lineage/sample-app-best-practices`

The application demonstrates:

`Data Registry logical asset ↔ mutable S3 identity → ingestion → Spark transformation → mock embeddings`

## Completed work

- Built the deployable `sample-app-best-practices` application with real KFP, Spark
  Operator, native OpenLineage Spark integration, MinIO, Marquez, PostgreSQL, and
  namespace-scoped OpenShift manifests.
- Added the registry stub, source CSV variants, ingestion/staging, Spark Parquet
  transformation, deterministic mock embeddings, failure injection, retries, and
  scenario verification.
- Added the design guide, runbook, live cluster validation record, and event catalog.
- The event catalog documents DatasetEvent, RunEvent, JobEvent ownership and the
  proposed manual registry revision model. See:
  `/Users/briangallagher/dev/workspaces/dr-lineage/sample-app-best-practices/docs/event-catalog.md`.
- The design guide and runbook contain the detailed OpenLineage identity, namespace,
  symlink, correlation, assurance, retention, and business-value decisions.
- The live cluster was validated with RHOAI/KFP/Spark/Marquez. Exact versions and
  reproducible commands are recorded in:
  `/Users/briangallagher/dev/workspaces/dr-lineage/sample-app-best-practices/docs/cluster-validation.md`.
- Final live scenario report is in:
  `/Users/briangallagher/dev/workspaces/dr-lineage/sample-app-best-practices/build/scenario-results.json`.
- Final live contract verification passed 18 checks. Original sample verification
  passed 27 tests; after adding the conformance harness, local verification passed
  31 tests and Ruff checks.
- The final live deployment was configured as internal-only. DSPA API and metadata
  Routes were disabled; a stale operator-owned metadata Route was explicitly removed.
  KFP API access through Service port-forward was verified.

## Feast versus Marquez verification status

The current architectural conclusion is deliberately scoped:

> Feast passed the core Marquez semantic compatibility test for the sample's
> operational lineage contract, but it has not yet been proven to be a complete
> production replacement for Marquez in RHOAI.

The conformance harness is implemented at:

- `/Users/briangallagher/dev/workspaces/dr-lineage/sample-app-best-practices/src/lineage_demo/conformance.py`
- `/Users/briangallagher/dev/workspaces/dr-lineage/sample-app-best-practices/tests/test_conformance.py`
- `/Users/briangallagher/dev/workspaces/dr-lineage/sample-app-best-practices/scripts/verify-feast-vs-marquez.sh`
- `/Users/briangallagher/dev/workspaces/dr-lineage/sample-app-best-practices/docs/feast-vs-marquez-conformance.md`

It posts the same deterministic OpenLineage fixture to Feast and Marquez and
compares normalized semantics rather than raw JSON. Each finding records a simple
explanation, why it matters, significance, observed evidence, status, gap impact,
and mitigation. Statuses are `PROVEN`, `FAILED`, `UNSUPPORTED`,
`NOT_OBSERVABLE`, and `REQUIRES_EXTERNAL_INTEGRATION`.

The live differential run used suite `differential-live-20260913c` and proved:

- exact dataset identity and registry-to-physical SymlinksDatasetFacet linkage;
- RunEvent, DatasetEvent, and JobEvent acceptance;
- parent-run hierarchy and data convergence;
- lifecycle states, failures, and distinct retry identities;
- standard and RHOAI evidence facet round-trip;
- Feast native graph queries and batch ingestion;
- late registry alias correlation; and
- semantic parity with Marquez for the core fixture projection.

The live result also recorded these differences and limits:

- Feast's current test endpoint allowed unauthenticated reads. This is a critical
  deployment/security gap because the manually created test service bypasses the
  normal kube-rbac-proxy path.
- Retention configuration was visible, but pruning, archive, restore, and
  graph-after-expiry behavior were not proven.
- Durability after outage, replay/dead-letter behavior, migrations, backup/restore,
  and capacity require controlled external integration tests.
- The RHOAI asset-centric business API, Data Registry ownership/RBAC, revision and
  evidence semantics, and query-time lineage were not proven by the generic Feast
  API test.
- Marquez did not expose the same batch-ingestion route. Marquez also added
  backend-derived `sql` and `nominalTime` facets; these were treated as additive,
  not as semantic incompatibilities.

The narrow Feast-only live run had 9 proven findings, 1 failed critical
authorization finding, 1 retention finding that was not observable end-to-end, and
6 findings requiring external integration. Local verification after adding the
harness passed 31 tests and Ruff checks.

Do not summarize the result as “no critical gaps.” The precise statement is:

> No critical semantic mismatch was found in the tested Feast-versus-Marquez
> operational lineage fixture. A critical authorization gap was observed in the
> current test deployment, and critical RHOAI business/revision contracts remain
> unproven.

### Current Feast test deployment caveat

The operator-managed `FeatureStore/data-registry` was originally registry-only.
Adding the OpenLineage consumer and `lineageServer` fields to the live CR was
accepted by the installed CRD, but the installed operator did not reconcile a
lineage deployment or service. A test-only manual deployment was therefore created:

- namespace: `redhat-ods-applications`;
- deployment: `feast-data-registry-lineage`;
- service port: `6580`;
- local test access: `oc port-forward service/feast-data-registry-lineage 16580:6580`;
- endpoint: `http://127.0.0.1:16580/api/v1/lineage`.

The manual service uses the existing catalog database secret by reference and does
not copy secret values into the repository or handoff. It has no kube-rbac-proxy
sidecar and is suitable only for isolated API verification. The original
operator-managed Data Registry configuration was restored after the test.

The operator/CRD mismatch is a separate deployment qualification finding; it must
not be confused with Feast's core OpenLineage event or graph behavior.

### Parked follow-up tests

If the work resumes, use disposable databases/deployments rather than the current
shared database:

1. Retention: short TTL, known event timestamps, prune, then inspect raw events,
   run details, graph state, symlinks, archive, and restore behavior separately.
2. Durability: fault-injection proxy, timeout/retry/duplicate/replay tests, and
   explicit at-most-once or at-least-once acceptance criteria.
3. Migrations: deploy version A, ingest data, upgrade to B, verify all entities and
   graph relationships, then test restart and backup/restore.
4. RHOAI semantics: real Data Registry, Feast, BFF/API, and project RBAC with
   cross-project negative cases and physical-URI redaction.
5. Revision evidence: two revisions of one asset backed by different immutable
   evidence, proving that runs do not collapse to one mutable URI.
6. Query-time lineage: only if RHOAI wants feature retrieval/model-serving traces
   linked to operational lineage; use OTel/trace IDs as a separate plane.

## Important observed limitation

With the pinned OpenLineage Spark integration, a data-bearing failed Spark write emits
native `FAIL`, but may emit `COMPLETE` immediately afterward for the same SQL run and
does not provide a native `errorMessage` facet. The KFP root and SparkApplication state
are authoritative for final status, while Spark driver logs provide error detail. This
is documented in the design guide, runbook, event catalog, and live validation record.

## Key implementation decisions

- Registry registration emits a logical UUID-based DatasetEvent with a standard
  SymlinksDatasetFacet to the physical S3 identity.
- Dataset identity is exact `(namespace, name)` equality. Processing outputs are not
  symlinked to the source.
- KFP root, ingestion, Spark, and embedding use separate run UUIDs. Child runs point
  to the KFP root through ParentRunFacet.
- Spark lineage comes only from the native listener; the KFP Spark submit/wait helper
  emits no duplicate Spark event.
- KFP custom containers obtain the real KFP run UUID through the Downward API because
  the literal KFP pipeline UUID placeholder was observed to remain unresolved.
- Spark Operator compatibility uses its implemented `envVars` and
  `envSecretKeyRefs` fields because this cluster accepted but dropped ordinary `env`
  and `envFrom` fields.
- Do not claim immutable source versions. Current assurance level is Linked, not
  Observed or Reproducible.

## Current repository state

The repository is intentionally uncommitted/dirty and contains the new application
tree plus an unrelated pre-existing `prompt.txt`. Preserve unrelated changes.

Important paths:

- `/Users/briangallagher/dev/workspaces/dr-lineage/sample-app-best-practices/README.md`
- `/Users/briangallagher/dev/workspaces/dr-lineage/sample-app-best-practices/docs/openlineage-design.md`
- `/Users/briangallagher/dev/workspaces/dr-lineage/sample-app-best-practices/docs/runbook.md`
- `/Users/briangallagher/dev/workspaces/dr-lineage/sample-app-best-practices/docs/event-catalog.md`
- `/Users/briangallagher/dev/workspaces/dr-lineage/sample-app-best-practices/docs/cluster-validation.md`
- `/Users/briangallagher/dev/workspaces/dr-lineage/sample-app-best-practices/src/lineage_demo/verify.py`
- `/Users/briangallagher/dev/workspaces/dr-lineage/sample-app-best-practices/spark/transform.py`
- `/Users/briangallagher/dev/workspaces/dr-lineage/sample-app-best-practices/openshift/dspa.yaml`
- `/Users/briangallagher/dev/workspaces/dr-lineage/sample-app-best-practices/scripts/deploy.sh`

## Reproducible checks

From the application directory:

```bash
uv sync --frozen --extra dev
uv run python -m pytest
uv run ruff check src tests pipeline
for script in scripts/*.sh; do bash -n "$script"; done
oc kustomize openshift >/dev/null
./scripts/verify.sh
```

`./scripts/deploy.sh`, `./scripts/upload-pipeline.sh`, and
`./scripts/run-scenarios.sh` are the live-cluster workflow. Authentication to a
compatible OpenShift/RHOAI cluster is required for live checks; credentials must not
be recorded in a handoff.

## Suggested next actions

- If continuing implementation, inspect `git status` and preserve the unrelated
  `prompt.txt` change.
- If preparing a commit or review, include the entire
  `sample-app-best-practices` tree and reference the validation documents rather than
  copying their contents into another artifact.
- If extending registry versioning, implement immutable revision records and a
  registry-specific revision facet; do not label a manual counter as
  `DatasetVersionFacet` without immutable evidence.
- If upgrading Spark/OpenLineage, rerun the targeted failure scenario first and verify
  native event ordering, duplicate terminal states, error facets, and exact dataset
  handoffs before rerunning the complete suite.

## Suggested skills

- `$kfp-setup` for future KFP/DSPA configuration changes.
- `$verify-openshift-cluster` for broader cluster health checks.
- `$grill-with-docs` when stress-testing the design against Data Registry terminology
  and updating the design documentation.
