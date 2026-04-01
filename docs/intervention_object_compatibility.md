# Intervention Object Compatibility

Status: active
Owner branch: Jesse-L-M/contract-freeze
Last updated: 2026-04-01

## Purpose

Milestone 0 freezes how future intervention-object-native work can dual-write
back to the current gene, module, policy, and packet consumers without
breaking `v0`, `v1`, benchmark fixtures, rescue baselines, or packet flows.

`intervention_object_id` is the source of truth for future replay, rescue,
atlas, policy, and packet work. The current shipped artifact names remain the
compatibility surface during the dual-write period.

## Hard Rules

- New work may add intervention-object-native artifacts, but it must not rename,
  delete, or repurpose the current shipped artifact names or CLI paths.
- Every dual-write bundle must carry an explicit mapping from each
  `intervention_object_id` to every projected legacy consumer row.
- Projection multiplicity must be explicit. `1 -> 1`, `1 -> n`, and `n -> 1`
  projections are all allowed, but silent `n -> 1` collapse is forbidden.
- If multiple intervention objects project onto one legacy consumer key and no
  declared aggregation rule exists, the legacy projection must fail closed.
- A release bundle may ship intervention-object-native outputs even when a
  legacy projection is blocked. The failure is in the projection, not the new
  native surface.

## Compatibility Matrix

### `gene_target_ledgers.json`

Current consumer key:
`entity_id` where `entity_type` is implicitly gene.

Projection from `intervention_object_id`:
the legacy row keeps only the gene-level target identity. Mechanism,
direction, modality, population, stage, endpoint domain, and regimen are
collapsed out of the key.

Projection multiplicity:
many intervention objects can project to one gene ledger row, and one
intervention object can project to multiple genes when `target_set` is not a
singleton.

Dual-write rule:
keep emitting one ledger row per gene. Intervention-object-native work must
ship the exact projection mapping as a sidecar file inside the release bundle.

Collision rule:
never pick one intervention object as the silent winner for a shared gene row.
If no declared aggregation rule exists, fail closed on the legacy projection.

### `decision_vectors_v1.json` and `domain_head_rankings_v1.csv`

Current consumer key:
`entity_type` + `entity_id`, where current shipped values are `gene` or
`module`.

Projection from `intervention_object_id`:
gene projections collapse to singleton target members. Module projections
collapse to the current curated module membership rather than preserving
mechanism, modality, regimen, or population differences.

Projection multiplicity:
many intervention objects can land on the same gene or module consumer row, and
one intervention object can fan out across multiple current consumers.

Dual-write rule:
emit intervention-object-native vectors separately. Only materialize the legacy
gene or module view when the projection rule is declared and the mapping file is
carried in the release bundle.

Collision rule:
no silent overwrite of a gene or module vector when two intervention objects
would land on the same current consumer row.

### `policy_decision_vectors_v2.json` and `policy_pareto_fronts_v1.json`

Current consumer key:
`policy_id` + current `entity_type` + current `entity_id`.

Projection from `intervention_object_id`:
the current policy surface still scores genes and modules, not full
intervention objects. Population, stage, regimen, and endpoint-domain
distinctions collapse unless a later milestone replaces the policy entity key.

Projection multiplicity:
many intervention objects can map to one policy entity row under the same
policy, and a single intervention object can contribute to multiple legacy
policies.

Dual-write rule:
policy releases may add intervention-object-native scoring outputs, but the
current policy artifacts remain the compatibility surface for existing
consumers. Any projected legacy row must be backed by an explicit mapping entry.

Collision rule:
silent legacy-consumer collisions are forbidden. If the projection is ambiguous,
the legacy policy output must fail closed.

### `hypothesis_packets_v1.json`

Current consumer key:
`packet_id`, which is currently gene-scoped and policy-scoped.

Projection from `intervention_object_id`:
packet consumers collapse from full intervention-object identity down to the
current gene and policy framing. Packet prose may discuss directionality,
failure memory, or modality, but the key itself is not yet
`intervention_object_id`.

Projection multiplicity:
many intervention objects can project to one current packet topic, especially
when the target gene and policy lens are shared across mechanisms or regimens.

Dual-write rule:
new intervention-object-native packets must use separate packet identifiers and
carry explicit back-references to every source `intervention_object_id` plus the
legacy packet projection they feed.

Collision rule:
do not silently merge multiple intervention objects into one current packet.
When the projection is ambiguous, fail closed on the legacy packet output.

## Release-Bundle Requirement

The top-level release manifest for any dual-write bundle is where the
compatibility mapping gets pinned for distribution. That release manifest must
carry:

- the required files for the bundle
- SHA256 digests for those files
- `artifact_name` plus `expected_schema_version` for any nested registered
  artifact
- the compatibility sidecar or mapping files needed to explain every projected
  legacy consumer row

See [docs/artifact_schemas.md](artifact_schemas.md) for the registered release
families and [docs/decisions/0002-release-manifest-contract.md](decisions/0002-release-manifest-contract.md)
for the cross-cutting manifest contract.
