from __future__ import annotations

from dataclasses import asdict, dataclass

from scz_target_engine.scoring import RankedEntity, parse_optional_float


AVAILABLE_STATUS = "available"
PARTIAL_STATUS = "partial"
MISSING_INPUTS_STATUS = "missing_inputs"
NOT_APPLICABLE_STATUS = "not_applicable"
PR7_SUBSTRATE_STATUS = "pr7_substrate_available"


@dataclass(frozen=True)
class WeightedInputSpec:
    layer_weights: tuple[tuple[str, float], ...] = ()
    metadata_weights: tuple[tuple[str, float], ...] = ()

    @property
    def input_names(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.layer_weights + self.metadata_weights)

    @property
    def total_weight(self) -> float:
        return sum(weight for _, weight in self.layer_weights + self.metadata_weights)


@dataclass(frozen=True)
class DecisionHeadDefinition:
    name: str
    label: str
    semantics: str
    entity_inputs: dict[str, WeightedInputSpec]
    pending_reason: str | None = None


@dataclass(frozen=True)
class DomainHeadDefinition:
    slug: str
    label: str
    axis: str
    semantics: str
    decision_head_weights: tuple[tuple[str, float], ...]

    @property
    def total_weight(self) -> float:
        return sum(weight for _, weight in self.decision_head_weights)


@dataclass(frozen=True)
class DecisionHeadScore:
    head_name: str
    label: str
    score: float | None
    status: str
    semantics: str
    used_inputs: tuple[str, ...]
    missing_inputs: tuple[str, ...]
    coverage_weight_fraction: float
    pending_reason: str | None


@dataclass(frozen=True)
class DomainHeadScore:
    domain_slug: str
    domain_label: str
    axis: str
    score: float | None
    status: str
    semantics: str
    coverage_weight_fraction: float
    available_head_count: int
    total_head_count: int
    pending_head_names: tuple[str, ...]
    missing_head_names: tuple[str, ...]


@dataclass(frozen=True)
class DecisionVectorV1:
    entity_type: str
    entity_id: str
    entity_label: str
    eligible_v0: bool
    heuristic_score_v0: float | None
    heuristic_rank_v0: int | None
    heuristic_stable_v0: bool
    warning_count: int
    warning_severity: str
    head_scores: tuple[DecisionHeadScore, ...]
    domain_head_scores: tuple[DomainHeadScore, ...]


DECISION_HEAD_DEFINITIONS = (
    DecisionHeadDefinition(
        name="human_support_score",
        label="Human support",
        semantics=(
            "Human genetic and patient-linked signal supporting target or module "
            "relevance in schizophrenia."
        ),
        entity_inputs={
            "gene": WeightedInputSpec(
                layer_weights=(
                    ("common_variant_support", 0.5),
                    ("rare_variant_support", 0.5),
                )
            ),
            "module": WeightedInputSpec(
                layer_weights=(("member_gene_genetic_enrichment", 1.0),)
            ),
        },
    ),
    DecisionHeadDefinition(
        name="biology_context_score",
        label="Biology context",
        semantics=(
            "Cell-state and regulatory context indicating whether the entity sits "
            "inside disease-relevant schizophrenia biology."
        ),
        entity_inputs={
            "gene": WeightedInputSpec(
                layer_weights=(
                    ("cell_state_support", 0.5),
                    ("developmental_regulatory_support", 0.5),
                )
            ),
            "module": WeightedInputSpec(
                layer_weights=(
                    ("cell_state_specificity", 0.5),
                    ("developmental_regulatory_relevance", 0.5),
                )
            ),
        },
    ),
    DecisionHeadDefinition(
        name="intervention_readiness_score",
        label="Intervention readiness",
        semantics=(
            "Actionability context for whether the entity already looks reachable "
            "for intervention, based on tractability and platform context."
        ),
        entity_inputs={
            "gene": WeightedInputSpec(
                layer_weights=(("tractability_compoundability", 0.6),),
                metadata_weights=(("generic_platform_baseline", 0.4),),
            )
        },
    ),
    DecisionHeadDefinition(
        name="failure_burden_score",
        label="Failure burden",
        semantics=(
            "Clinical failure-history burden and unresolved program baggage relevant "
            "to this use case."
        ),
        entity_inputs={},
        pending_reason=(
            "PR7 failure-history substrate is now available in structural ledgers, "
            "but PR8 still leaves this numeric head explicit and unscored."
        ),
    ),
    DecisionHeadDefinition(
        name="directionality_confidence",
        label="Directionality confidence",
        semantics=(
            "Confidence that the intervention direction is known and coherent for the "
            "intended schizophrenia use case."
        ),
        entity_inputs={},
        pending_reason=(
            "PR7 directionality substrate is now available in structural ledgers, "
            "but PR8 still leaves this numeric head explicit and unscored."
        ),
    ),
    DecisionHeadDefinition(
        name="subgroup_resolution_score",
        label="Subgroup resolution",
        semantics=(
            "Resolution around the population, illness stage, or symptom subgroup "
            "most likely to benefit."
        ),
        entity_inputs={},
        pending_reason=(
            "PR7-backed subgroup and heterogeneity substrate is now available "
            "structurally, but PR8 still leaves this numeric head explicit and unscored."
        ),
    ),
)

DOMAIN_HEAD_DEFINITIONS = (
    DomainHeadDefinition(
        slug="acute_positive_symptoms",
        label="Acute positive symptoms",
        axis="outcome_domain",
        semantics=(
            "Weights near-term human signal and intervention readiness for acute "
            "symptom control during active psychosis."
        ),
        decision_head_weights=(
            ("human_support_score", 0.35),
            ("biology_context_score", 0.20),
            ("intervention_readiness_score", 0.30),
            ("directionality_confidence", 0.10),
            ("subgroup_resolution_score", 0.05),
        ),
    ),
    DomainHeadDefinition(
        slug="relapse_prevention",
        label="Relapse prevention",
        axis="outcome_domain",
        semantics=(
            "Weights biology context and durability-facing evidence for maintenance "
            "and post-stabilization recurrence control."
        ),
        decision_head_weights=(
            ("human_support_score", 0.20),
            ("biology_context_score", 0.35),
            ("intervention_readiness_score", 0.20),
            ("failure_burden_score", 0.10),
            ("directionality_confidence", 0.10),
            ("subgroup_resolution_score", 0.05),
        ),
    ),
    DomainHeadDefinition(
        slug="treatment_resistant_schizophrenia",
        label="Treatment-resistant schizophrenia",
        axis="population_or_stage",
        semantics=(
            "Weights biologic plausibility plus future failure and subgroup inputs "
            "needed for resistant populations."
        ),
        decision_head_weights=(
            ("human_support_score", 0.25),
            ("biology_context_score", 0.20),
            ("intervention_readiness_score", 0.15),
            ("failure_burden_score", 0.15),
            ("directionality_confidence", 0.10),
            ("subgroup_resolution_score", 0.15),
        ),
    ),
    DomainHeadDefinition(
        slug="clozapine_resistant_schizophrenia",
        label="Clozapine-resistant schizophrenia",
        axis="population_or_stage",
        semantics=(
            "Weights the narrowest refractory subgroup profile and leaves large room "
            "for future PR7 failure and subgroup evidence."
        ),
        decision_head_weights=(
            ("human_support_score", 0.20),
            ("biology_context_score", 0.20),
            ("intervention_readiness_score", 0.10),
            ("failure_burden_score", 0.20),
            ("directionality_confidence", 0.10),
            ("subgroup_resolution_score", 0.20),
        ),
    ),
    DomainHeadDefinition(
        slug="negative_symptoms",
        label="Negative symptoms",
        axis="outcome_domain",
        semantics=(
            "Weights biologic context and subgroup fit more heavily than acute "
            "intervention readiness."
        ),
        decision_head_weights=(
            ("human_support_score", 0.15),
            ("biology_context_score", 0.45),
            ("intervention_readiness_score", 0.10),
            ("directionality_confidence", 0.15),
            ("subgroup_resolution_score", 0.15),
        ),
    ),
    DomainHeadDefinition(
        slug="cognition",
        label="Cognition",
        axis="outcome_domain",
        semantics=(
            "Weights biology context and directionality over acute tractability for "
            "cognition-focused use cases."
        ),
        decision_head_weights=(
            ("human_support_score", 0.20),
            ("biology_context_score", 0.45),
            ("intervention_readiness_score", 0.10),
            ("directionality_confidence", 0.15),
            ("subgroup_resolution_score", 0.10),
        ),
    ),
    DomainHeadDefinition(
        slug="chr_transition_prevention",
        label="CHR / transition prevention",
        axis="population_or_stage",
        semantics=(
            "Weights human signal and biology context for pre-threshold transition "
            "risk questions."
        ),
        decision_head_weights=(
            ("human_support_score", 0.30),
            ("biology_context_score", 0.35),
            ("intervention_readiness_score", 0.15),
            ("directionality_confidence", 0.10),
            ("subgroup_resolution_score", 0.10),
        ),
    ),
    DomainHeadDefinition(
        slug="functioning_durable_recovery_relevance",
        label="Functioning / durable recovery relevance",
        axis="cross_cutting_outcome",
        semantics=(
            "Weights biology, subgroup fit, and longitudinal risk context for "
            "durable recovery relevance."
        ),
        decision_head_weights=(
            ("human_support_score", 0.15),
            ("biology_context_score", 0.35),
            ("intervention_readiness_score", 0.10),
            ("failure_burden_score", 0.10),
            ("directionality_confidence", 0.10),
            ("subgroup_resolution_score", 0.20),
        ),
    ),
)


def compute_weighted_average(values: list[tuple[float, float]]) -> float | None:
    denominator = sum(weight for _, weight in values)
    if denominator == 0:
        return None
    numerator = sum(value * weight for value, weight in values)
    return round(numerator / denominator, 6)


def build_decision_head_score(
    entity: RankedEntity,
    definition: DecisionHeadDefinition,
) -> DecisionHeadScore:
    if definition.pending_reason is not None:
        return DecisionHeadScore(
            head_name=definition.name,
            label=definition.label,
            score=None,
            status=PR7_SUBSTRATE_STATUS,
            semantics=definition.semantics,
            used_inputs=(),
            missing_inputs=(),
            coverage_weight_fraction=0.0,
            pending_reason=definition.pending_reason,
        )

    input_spec = definition.entity_inputs.get(entity.entity_type)
    if input_spec is None:
        return DecisionHeadScore(
            head_name=definition.name,
            label=definition.label,
            score=None,
            status=NOT_APPLICABLE_STATUS,
            semantics=definition.semantics,
            used_inputs=(),
            missing_inputs=(),
            coverage_weight_fraction=0.0,
            pending_reason=None,
        )

    weighted_values: list[tuple[float, float]] = []
    used_inputs: list[str] = []
    missing_inputs: list[str] = []
    present_weight = 0.0

    for layer_name, weight in input_spec.layer_weights:
        value = entity.layer_values.get(layer_name)
        if value is None:
            missing_inputs.append(layer_name)
            continue
        weighted_values.append((value, weight))
        used_inputs.append(layer_name)
        present_weight += weight

    for metadata_name, weight in input_spec.metadata_weights:
        value = parse_optional_float(entity.metadata.get(metadata_name))
        if value is None:
            missing_inputs.append(metadata_name)
            continue
        weighted_values.append((value, weight))
        used_inputs.append(metadata_name)
        present_weight += weight

    score = compute_weighted_average(weighted_values)
    status = MISSING_INPUTS_STATUS
    if score is not None:
        status = AVAILABLE_STATUS if not missing_inputs else PARTIAL_STATUS

    coverage = 0.0
    if input_spec.total_weight:
        coverage = round(present_weight / input_spec.total_weight, 6)

    return DecisionHeadScore(
        head_name=definition.name,
        label=definition.label,
        score=score,
        status=status,
        semantics=definition.semantics,
        used_inputs=tuple(used_inputs),
        missing_inputs=tuple(missing_inputs),
        coverage_weight_fraction=coverage,
        pending_reason=None,
    )


def build_domain_head_score(
    head_scores: dict[str, DecisionHeadScore],
    definition: DomainHeadDefinition,
) -> DomainHeadScore:
    weighted_values: list[tuple[float, float]] = []
    present_weight = 0.0
    available_head_count = 0
    pending_head_names: list[str] = []
    missing_head_names: list[str] = []

    for head_name, weight in definition.decision_head_weights:
        head_score = head_scores[head_name]
        if head_score.score is not None:
            weighted_values.append((head_score.score, weight))
            present_weight += weight
            available_head_count += 1
            continue
        if head_score.status == PR7_SUBSTRATE_STATUS:
            pending_head_names.append(head_name)
            continue
        missing_head_names.append(head_name)

    score = compute_weighted_average(weighted_values)
    status = MISSING_INPUTS_STATUS
    if score is not None:
        status = AVAILABLE_STATUS if present_weight == definition.total_weight else PARTIAL_STATUS

    coverage = 0.0
    if definition.total_weight:
        coverage = round(present_weight / definition.total_weight, 6)

    return DomainHeadScore(
        domain_slug=definition.slug,
        domain_label=definition.label,
        axis=definition.axis,
        score=score,
        status=status,
        semantics=definition.semantics,
        coverage_weight_fraction=coverage,
        available_head_count=available_head_count,
        total_head_count=len(definition.decision_head_weights),
        pending_head_names=tuple(pending_head_names),
        missing_head_names=tuple(missing_head_names),
    )


def build_decision_vector(entity: RankedEntity) -> DecisionVectorV1:
    head_scores = tuple(
        build_decision_head_score(entity, definition)
        for definition in DECISION_HEAD_DEFINITIONS
    )
    head_score_index = {score.head_name: score for score in head_scores}
    domain_head_scores = tuple(
        build_domain_head_score(head_score_index, definition)
        for definition in DOMAIN_HEAD_DEFINITIONS
    )
    return DecisionVectorV1(
        entity_type=entity.entity_type,
        entity_id=entity.entity_id,
        entity_label=entity.entity_label,
        eligible_v0=entity.eligible,
        heuristic_score_v0=entity.composite_score,
        heuristic_rank_v0=entity.rank,
        heuristic_stable_v0=entity.heuristic_stable,
        warning_count=entity.warning_count,
        warning_severity=entity.warning_severity,
        head_scores=head_scores,
        domain_head_scores=domain_head_scores,
    )


def build_decision_vectors(entities: list[RankedEntity]) -> list[DecisionVectorV1]:
    return [build_decision_vector(entity) for entity in entities]


def serialize_decision_head_score(score: DecisionHeadScore) -> dict[str, object]:
    return {
        "score": score.score,
        "label": score.label,
        "status": score.status,
        "semantics": score.semantics,
        "used_inputs": list(score.used_inputs),
        "missing_inputs": list(score.missing_inputs),
        "coverage_weight_fraction": score.coverage_weight_fraction,
        "pending_reason": score.pending_reason,
    }


def serialize_domain_head_score(score: DomainHeadScore) -> dict[str, object]:
    return {
        "score": score.score,
        "label": score.domain_label,
        "axis": score.axis,
        "status": score.status,
        "semantics": score.semantics,
        "coverage_weight_fraction": score.coverage_weight_fraction,
        "available_head_count": score.available_head_count,
        "total_head_count": score.total_head_count,
        "pending_head_names": list(score.pending_head_names),
        "missing_head_names": list(score.missing_head_names),
    }


def serialize_decision_vector(vector: DecisionVectorV1) -> dict[str, object]:
    head_score_index = {score.head_name: score for score in vector.head_scores}
    domain_score_index = {
        score.domain_slug: score for score in vector.domain_head_scores
    }
    payload: dict[str, object] = {
        "entity_type": vector.entity_type,
        "entity_id": vector.entity_id,
        "entity_label": vector.entity_label,
        "eligible_v0": vector.eligible_v0,
        "heuristic_score_v0": vector.heuristic_score_v0,
        "heuristic_rank_v0": vector.heuristic_rank_v0,
        "heuristic_stable_v0": vector.heuristic_stable_v0,
        "warning_count": vector.warning_count,
        "warning_severity": vector.warning_severity,
        "decision_vector": {
            definition.name: serialize_decision_head_score(
                head_score_index[definition.name]
            )
            for definition in DECISION_HEAD_DEFINITIONS
        },
        "domain_profiles": {
            definition.slug: serialize_domain_head_score(
                domain_score_index[definition.slug]
            )
            for definition in DOMAIN_HEAD_DEFINITIONS
        },
        "head_scores": [asdict(score) for score in vector.head_scores],
        "domain_head_scores": [asdict(score) for score in vector.domain_head_scores],
    }
    for definition in DECISION_HEAD_DEFINITIONS:
        head_score = head_score_index[definition.name]
        payload[definition.name] = head_score.score
        payload[f"{definition.name}_status"] = head_score.status
    return payload


def build_decision_vector_payload(
    gene_vectors: list[DecisionVectorV1],
    module_vectors: list[DecisionVectorV1],
) -> dict[str, object]:
    return {
        "schema_version": "v1",
        "decision_head_definitions": [asdict(definition) for definition in DECISION_HEAD_DEFINITIONS],
        "domain_head_definitions": [asdict(definition) for definition in DOMAIN_HEAD_DEFINITIONS],
        "entities": {
            "gene": [serialize_decision_vector(vector) for vector in gene_vectors],
            "module": [serialize_decision_vector(vector) for vector in module_vectors],
        },
    }


def rank_domain_head_rows(vectors: list[DecisionVectorV1]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str], list[tuple[DecisionVectorV1, DomainHeadScore]]] = {}
    for vector in vectors:
        head_score_index = {score.head_name: score for score in vector.head_scores}
        for domain_score in vector.domain_head_scores:
            grouped.setdefault((vector.entity_type, domain_score.domain_slug), []).append(
                (vector, domain_score)
            )

    rank_maps: dict[tuple[str, str], dict[str, int]] = {}
    for key, entries in grouped.items():
        ranked_entries = [
            (vector, domain_score)
            for vector, domain_score in entries
            if domain_score.score is not None
        ]
        ranked_entries.sort(
            key=lambda item: (
                -float(item[1].score),
                -item[1].coverage_weight_fraction,
                item[0].entity_label.lower(),
            )
        )
        rank_maps[key] = {
            vector.entity_id: index
            for index, (vector, _) in enumerate(ranked_entries, start=1)
        }

    rows: list[dict[str, object]] = []
    domain_order = {definition.slug: index for index, definition in enumerate(DOMAIN_HEAD_DEFINITIONS)}
    for vector in vectors:
        head_score_index = {score.head_name: score for score in vector.head_scores}
        for domain_score in vector.domain_head_scores:
            row: dict[str, object] = {
                "entity_type": vector.entity_type,
                "entity_id": vector.entity_id,
                "entity_label": vector.entity_label,
                "domain_slug": domain_score.domain_slug,
                "domain_label": domain_score.domain_label,
                "domain_axis": domain_score.axis,
                "domain_head_score_v1": domain_score.score,
                "domain_rank_v1": rank_maps[(vector.entity_type, domain_score.domain_slug)].get(
                    vector.entity_id
                ),
                "domain_score_status": domain_score.status,
                "domain_coverage_weight_fraction": domain_score.coverage_weight_fraction,
                "domain_available_head_count": domain_score.available_head_count,
                "domain_total_head_count": domain_score.total_head_count,
                "domain_pending_heads": " | ".join(domain_score.pending_head_names),
                "domain_missing_heads": " | ".join(domain_score.missing_head_names),
                "heuristic_score_v0": vector.heuristic_score_v0,
                "heuristic_rank_v0": vector.heuristic_rank_v0,
                "heuristic_stable_v0": vector.heuristic_stable_v0,
                "warning_count": vector.warning_count,
                "warning_severity": vector.warning_severity,
            }
            for head_definition in DECISION_HEAD_DEFINITIONS:
                head_score = head_score_index[head_definition.name]
                row[head_definition.name] = head_score.score
                row[f"{head_definition.name}_status"] = head_score.status
            rows.append(row)

    rows.sort(
        key=lambda row: (
            row["entity_type"],
            domain_order[str(row["domain_slug"])],
            row["domain_rank_v1"] is None,
            row["domain_rank_v1"] if row["domain_rank_v1"] is not None else 10**9,
            str(row["entity_label"]).lower(),
        )
    )
    return rows
