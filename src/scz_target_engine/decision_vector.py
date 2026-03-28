from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

from scz_target_engine.scoring import RankedEntity, parse_optional_float

if TYPE_CHECKING:
    from scz_target_engine.ledger import TargetLedger


AVAILABLE_STATUS = "available"
PARTIAL_STATUS = "partial"
MISSING_INPUTS_STATUS = "missing_inputs"
NOT_APPLICABLE_STATUS = "not_applicable"

FAILURE_SCOPE_PENALTIES = {
    "target": 0.35,
    "target_class": 0.30,
    "unresolved": 0.25,
    "population": 0.20,
    "endpoint": 0.15,
    "molecule": 0.10,
}

FAILURE_EVIDENCE_PENALTIES = {
    "strong": 0.10,
    "moderate": 0.05,
    "provisional": 0.0,
}

DIRECTIONALITY_BASE_SCORES = {
    "high": 0.90,
    "medium": 0.75,
    "low": 0.60,
}

PR7_LEDGER_INPUTS = {
    "failure_burden_score": (
        "structural_failure_history.failure_event_count",
        "structural_failure_history.failure_scopes",
        "structural_failure_history.events[].evidence_strength",
    ),
    "directionality_confidence": (
        "directionality_hypothesis.status",
        "directionality_hypothesis.confidence",
        "directionality_hypothesis.desired_perturbation_direction",
        "directionality_hypothesis.modality_hypothesis",
        "directionality_hypothesis.contradiction_conditions",
        "directionality_hypothesis.falsification_conditions",
        "directionality_hypothesis.open_risks",
    ),
    "subgroup_resolution_score": (
        "subgroup_domain_relevance.clinical_domains",
        "subgroup_domain_relevance.clinical_populations",
        "subgroup_domain_relevance.mono_or_adjunct_contexts",
        "subgroup_domain_relevance.psychencode_deg_top_cell_types",
        "subgroup_domain_relevance.psychencode_grn_top_cell_types",
        "structural_failure_history.failure_scopes",
        "structural_failure_history.failure_taxonomies",
    ),
}

DOMAIN_RELEVANCE = {
    "acute_positive_symptoms": frozenset({"acute_positive_symptoms"}),
    "relapse_prevention": frozenset({"relapse_prevention"}),
    "treatment_resistant_schizophrenia": frozenset(
        {
            "treatment_resistant_schizophrenia",
            "clozapine_resistant_schizophrenia",
        }
    ),
    "clozapine_resistant_schizophrenia": frozenset({"clozapine_resistant_schizophrenia"}),
    "negative_symptoms": frozenset({"negative_symptoms"}),
    "cognition": frozenset({"cognition"}),
    "chr_transition_prevention": frozenset({"chr_transition_prevention"}),
    "functioning_durable_recovery_relevance": frozenset(
        {"functioning_durable_recovery_relevance"}
    ),
}


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
    projected_head_scores: tuple[tuple[str, float | None], ...]


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
            "Higher means lower known clinical failure burden: no known structural "
            "failure baggage scores highest, while repeated or severe unresolved "
            "history scores lower."
        ),
        entity_inputs={},
    ),
    DecisionHeadDefinition(
        name="directionality_confidence",
        label="Directionality confidence",
        semantics=(
            "Confidence that the intervention direction is curated and coherent for "
            "the intended schizophrenia use case."
        ),
        entity_inputs={},
    ),
    DecisionHeadDefinition(
        name="subgroup_resolution_score",
        label="Subgroup resolution",
        semantics=(
            "Clarity around the population, illness stage, symptom subgroup, or "
            "cell-state context most likely to benefit."
        ),
        entity_inputs={},
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


def clamp_score(value: float) -> float:
    return round(max(0.0, min(1.0, value)), 6)


def iter_relevant_program_events(
    target_ledger: TargetLedger,
    domain_slug: str | None,
) -> list[dict[str, object]]:
    events = list(target_ledger.structural_failure_history["events"])
    if domain_slug is None:
        return events
    relevant_domains = DOMAIN_RELEVANCE.get(domain_slug, frozenset({domain_slug}))
    return [
        event
        for event in events
        if str(event.get("where", {}).get("domain", "")) in relevant_domains
    ]


def compute_failure_burden_score(
    target_ledger: TargetLedger,
    domain_slug: str | None = None,
) -> float:
    relevant_events = [
        event
        for event in iter_relevant_program_events(target_ledger, domain_slug)
        if event.get("failure_scope") != "nonfailure"
    ]
    failure_event_count = len(relevant_events)
    if failure_event_count == 0:
        return 1.0

    scope_penalty = max(
        (
            FAILURE_SCOPE_PENALTIES.get(str(scope), FAILURE_SCOPE_PENALTIES["unresolved"])
            for scope in (event.get("failure_scope") for event in relevant_events)
        ),
        default=0.0,
    )
    evidence_penalty = max(
        (
            FAILURE_EVIDENCE_PENALTIES.get(
                str(event.get("evidence_strength")),
                FAILURE_EVIDENCE_PENALTIES["provisional"],
            )
            for event in relevant_events
        ),
        default=0.0,
    )
    count_penalty = min(0.45, 0.15 * failure_event_count)
    return clamp_score(1.0 - count_penalty - scope_penalty - evidence_penalty)


def compute_directionality_confidence_score(target_ledger: TargetLedger) -> float:
    directionality = target_ledger.directionality_hypothesis
    if directionality["status"] != "curated":
        return 0.25

    score = DIRECTIONALITY_BASE_SCORES.get(
        str(directionality["confidence"]),
        DIRECTIONALITY_BASE_SCORES["low"],
    )
    if directionality["desired_perturbation_direction"] == "undetermined":
        score -= 0.10
    if directionality["modality_hypothesis"] == "undetermined":
        score -= 0.10
    if directionality["contradiction_conditions"]:
        score -= 0.10
    if directionality["falsification_conditions"]:
        score -= 0.05
    score -= min(0.10, 0.05 * len(directionality["open_risks"]))
    return clamp_score(score)


def compute_subgroup_resolution_score(
    target_ledger: TargetLedger,
    domain_slug: str | None = None,
) -> float:
    subgroup = target_ledger.subgroup_domain_relevance
    relevant_events = iter_relevant_program_events(target_ledger, domain_slug)
    score = 0.0

    clinical_domains = sorted(
        {
            str(event.get("where", {}).get("domain", ""))
            for event in relevant_events
            if event.get("where", {}).get("domain")
        }
    )
    clinical_domain_count = len(clinical_domains if domain_slug is not None else subgroup["clinical_domains"])
    if clinical_domain_count == 1:
        score += 0.25
    elif clinical_domain_count > 1:
        score += 0.15

    clinical_populations = sorted(
        {
            str(event.get("where", {}).get("population", ""))
            for event in relevant_events
            if event.get("where", {}).get("population")
        }
    )
    clinical_population_count = len(
        clinical_populations if domain_slug is not None else subgroup["clinical_populations"]
    )
    if clinical_population_count == 1:
        score += 0.25
    elif clinical_population_count > 1:
        score += 0.15

    mono_or_adjunct_contexts = sorted(
        {
            str(event.get("mono_or_adjunct", ""))
            for event in relevant_events
            if event.get("mono_or_adjunct")
        }
    )
    if (
        mono_or_adjunct_contexts
        if domain_slug is not None
        else subgroup["mono_or_adjunct_contexts"]
    ):
        score += 0.10
    if subgroup["psychencode_deg_top_cell_types"]:
        score += 0.15
    if subgroup["psychencode_grn_top_cell_types"]:
        score += 0.15
    if any(event.get("failure_scope") == "population" for event in relevant_events):
        score -= 0.15
    if any(
        event.get("failure_reason_taxonomy") == "heterogeneity_or_subgroup_dilution"
        for event in relevant_events
    ):
        score -= 0.15

    return clamp_score(score)


def project_head_score_for_domain(
    head_name: str,
    head_score: DecisionHeadScore,
    target_ledger: TargetLedger | None,
    domain_slug: str,
) -> float | None:
    if head_score.score is None:
        return None
    if target_ledger is None:
        return head_score.score
    if head_name == "failure_burden_score":
        return compute_failure_burden_score(target_ledger, domain_slug=domain_slug)
    if head_name == "subgroup_resolution_score":
        return compute_subgroup_resolution_score(target_ledger, domain_slug=domain_slug)
    return head_score.score


def build_ledger_backed_head_score(
    entity: RankedEntity,
    definition: DecisionHeadDefinition,
    target_ledger: TargetLedger | None,
) -> DecisionHeadScore:
    if entity.entity_type != "gene":
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
    if target_ledger is None:
        return DecisionHeadScore(
            head_name=definition.name,
            label=definition.label,
            score=None,
            status=MISSING_INPUTS_STATUS,
            semantics=definition.semantics,
            used_inputs=(),
            missing_inputs=("target_ledger",),
            coverage_weight_fraction=0.0,
            pending_reason=None,
        )

    if definition.name == "failure_burden_score":
        score = compute_failure_burden_score(target_ledger)
    elif definition.name == "directionality_confidence":
        score = compute_directionality_confidence_score(target_ledger)
    else:
        score = compute_subgroup_resolution_score(target_ledger)

    return DecisionHeadScore(
        head_name=definition.name,
        label=definition.label,
        score=score,
        status=AVAILABLE_STATUS,
        semantics=definition.semantics,
        used_inputs=PR7_LEDGER_INPUTS[definition.name],
        missing_inputs=(),
        coverage_weight_fraction=1.0,
        pending_reason=None,
    )


def build_decision_head_score(
    entity: RankedEntity,
    definition: DecisionHeadDefinition,
    target_ledger: TargetLedger | None = None,
) -> DecisionHeadScore:
    if definition.name in PR7_LEDGER_INPUTS:
        return build_ledger_backed_head_score(entity, definition, target_ledger)

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
    target_ledger: TargetLedger | None = None,
) -> DomainHeadScore:
    weighted_values: list[tuple[float, float]] = []
    present_weight = 0.0
    available_head_count = 0
    pending_head_names: list[str] = []
    missing_head_names: list[str] = []
    projected_head_scores: list[tuple[str, float | None]] = []

    for head_name, weight in definition.decision_head_weights:
        head_score = head_scores[head_name]
        projected_score = project_head_score_for_domain(
            head_name,
            head_score,
            target_ledger,
            definition.slug,
        )
        projected_head_scores.append((head_name, projected_score))
        if projected_score is not None:
            weighted_values.append((projected_score, weight))
            present_weight += weight
            available_head_count += 1
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
        projected_head_scores=tuple(projected_head_scores),
    )


def build_decision_vector(
    entity: RankedEntity,
    target_ledger: TargetLedger | None = None,
) -> DecisionVectorV1:
    head_scores = tuple(
        build_decision_head_score(entity, definition, target_ledger)
        for definition in DECISION_HEAD_DEFINITIONS
    )
    head_score_index = {score.head_name: score for score in head_scores}
    domain_head_scores = tuple(
        build_domain_head_score(head_score_index, definition, target_ledger)
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


def build_decision_vectors(
    entities: list[RankedEntity],
    ledger_index: Mapping[str, TargetLedger] | None = None,
) -> list[DecisionVectorV1]:
    return [
        build_decision_vector(
            entity,
            target_ledger=None if ledger_index is None else ledger_index.get(entity.entity_id),
        )
        for entity in entities
    ]


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
        "projected_head_scores": {
            head_name: projected_score
            for head_name, projected_score in score.projected_head_scores
        },
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
            projected_head_score_index = dict(domain_score.projected_head_scores)
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
                row[head_definition.name] = projected_head_score_index.get(
                    head_definition.name,
                    head_score.score,
                )
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
