# Feast versus Marquez conformance harness

This harness tests whether the deployed Feast OpenLineage API preserves the
semantic contract used by the sample application when compared with Marquez.
It is not a generic HTTP smoke test and it is not a test of Feast's feature-store
serving behavior.

## What it produces

Each finding includes:

- a plain-language explanation of the area;
- why the area matters to a Marquez replacement;
- significance (`Critical`, `High`, `Medium`, or `Low`);
- a deterministic status: `PROVEN`, `FAILED`, `UNSUPPORTED`,
  `NOT_OBSERVABLE`, or `REQUIRES_EXTERNAL_INTEGRATION`;
- observed and expected evidence;
- gap impact; and
- a mitigation, including a possible Feast extension or RHOAI adapter where
  appropriate.

The JSON report is intended for automation. The Markdown report is intended for
architecture and release review.

## Areas covered

The fixture directly tests event ingestion, exact dataset identity, registry
symlinks, parent-run hierarchy, lifecycle and retries, facet preservation,
batch delivery, Feast's native graph and query routes, authorization exposure,
retention visibility, and late-event correlation.

The harness also records explicit non-observable findings for durability after
outage, migrations and backup/restore, capacity, the RHOAI asset-centric API,
query-time lineage, exact version qualification, and authoritative Data Registry
revision/evidence semantics. Those are not silently treated as passing merely
because the generic OpenLineage API accepted an event.

## Running against the current cluster setup

The current test-only Feast lineage service is reachable through a port-forward:

```bash
oc port-forward -n redhat-ods-applications \
  service/feast-data-registry-lineage 16580:6580
```

The sample Marquez service can be forwarded separately:

```bash
oc port-forward -n ol-best-practices service/marquez 5000:80
```

Then run:

```bash
cd sample-app-best-practices
CONFORMANCE_SUITE_ID=feast-vs-marquez-20260913 \
  ./scripts/verify-feast-vs-marquez.sh
```

Use a new `CONFORMANCE_SUITE_ID` for each run against persistent storage. The
harness does not call a global reset endpoint because that would delete unrelated
lineage history from a shared database.

To test only Feast:

```bash
uv run python -m lineage_demo.conformance \
  --feast-url http://127.0.0.1:16580 \
  --suite-id feast-only-20260913 \
  --json-output /tmp/feast-conformance.json \
  --markdown-output /tmp/feast-conformance.md
```

Use `--strict` when a CI job should exit non-zero for a `FAILED` result. The
default exits successfully after producing the report, which allows known gaps
and not-observable areas to be reviewed rather than hiding their evidence.

## Interpretation

`PROVEN` means the deployed endpoint accepted the fixture and exposed the tested
semantic result. It does not mean that every surrounding RHOAI product contract
is complete.

`FAILED` means the endpoint was reachable but contradicted the expected contract.
`UNSUPPORTED` means the route or capability is absent. `NOT_OBSERVABLE` means the
API exposes some configuration but cannot prove the full behavior. The external
integration status is used for behaviors that require controlled outage,
authorization, migration, load, storage, or cross-service tests.

The comparison uses the same deterministic event catalog for both backends and
compares a normalized projection of identities, jobs, runs, parent relationships,
symlinks, data convergence, and required facet keys. API response evidence is
retained in the JSON report where it is needed to explain a result.

## Validated result from the current cluster

The live differential run `differential-live-20260913c` proved semantic parity
between Feast and Marquez for the sample fixture: identities, registry symlinks,
run hierarchy, lifecycle states, retry identities, data convergence, and required
facets all matched.

The result is not a blanket production-readiness approval. The current manually
deployed Feast test service allowed unauthenticated reads, which is a critical
security/deployment gap. Retention behavior beyond configuration visibility,
durable delivery and replay, migrations, the RHOAI asset-centric API, query-time
lineage, and Data Registry revision/evidence semantics remain external integration
work. Marquez also lacks the Feast batch-ingestion route; Marquez-generated
`sql` and `nominalTime` facets were treated as additive backend projections rather
than semantic incompatibilities.

The operator-managed Data Registry was restored to its original registry-only
configuration after testing. The current Feast lineage endpoint was a test-only
manual service because the installed operator accepted the lineage CRD fields but
did not reconcile the expected lineage deployment.
