from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from scz_target_engine.program_memory.analogs import (
    AnalogSearchResult,
    InterventionProposal,
    ProgramMemoryAnalog,
    UncertaintyFlag,
    retrieve_program_memory_analogs,
)
from scz_target_engine.program_memory.loaders import load_program_memory_dataset
from scz_target_engine.program_memory.models import (
    ProgramMemoryDataset,
    ProgramMemoryDirectionalityHypothesis,
)


FAILURE_SCOPE_BY_TAXONOMY = {
    "not_applicable_nonfailure": "nonfailure",
    "unresolved": "unresolved",
    "molecule_failure": "molecule",
    "target_class_failure": "target_class",
    "endpoint_mismatch": "endpoint",
    "population_mismatch": "population",
    "dosing_or_exposure_issue": "molecule",
    "heterogeneity_or_subgroup_dilution": "population",
    "probable_target_invalidity": "target",
}


@dataclass(frozen=True)
class CounterfactualReason:
    relation: str
    event_id: str
    failure_scope: str
    explanation: str


@dataclass(frozen=True)
class ReplayRiskAssessment:
    proposal: InterventionProposal
    status: str
    summary: str
    analog_search: AnalogSearchResult
    supporting_reasons: tuple[CounterfactualReason, ...]
    offsetting_reasons: tuple[CounterfactualReason, ...]
    uncertainty_reasons: tuple[CounterfactualReason, ...]
    uncertainty_flags: tuple[UncertaintyFlag, ...]
    falsification_conditions: tuple[str, ...]


def assess_counterfactual_replay_risk(
    dataset_or_path: ProgramMemoryDataset | Path,
    proposal: InterventionProposal,
    *,
    limit: int | None = None,
) -> ReplayRiskAssessment:
    dataset = _coerce_dataset(dataset_or_path)
    analog_search = retrieve_program_memory_analogs(
        dataset_or_path,
        proposal,
        limit=limit,
    )
    supporting_reasons: list[CounterfactualReason] = []
    offsetting_reasons: list[CounterfactualReason] = []
    uncertainty_reasons: list[CounterfactualReason] = []

    for analog in analog_search.all_matched_analogs:
        if analog.is_nonfailure:
            if _is_domain_aligned(analog, proposal):
                offsetting_reasons.append(_build_nonfailure_reason(analog))
            continue

        reason = _classify_failure_reason(analog, proposal)
        if reason.relation == "supports_replay":
            supporting_reasons.append(reason)
        else:
            uncertainty_reasons.append(reason)

    status = _determine_status(
        analog_search=analog_search,
        supporting_reasons=supporting_reasons,
        offsetting_reasons=offsetting_reasons,
    )
    uncertainty_flags = list(analog_search.uncertainty_flags)
    if status == "replay_inconclusive":
        uncertainty_flags.append(
            UncertaintyFlag(
                code="replay_inconclusive",
                explanation=(
                    "Checked-in history contains biologically relevant analogs, but "
                    "the current failure scopes do not defend a clean replay claim."
                ),
            )
        )
    falsification_conditions = _build_falsification_conditions(
        dataset,
        proposal,
        status=status,
    )
    return ReplayRiskAssessment(
        proposal=proposal,
        status=status,
        summary=_build_summary(
            status=status,
            analog_search=analog_search,
            supporting_reasons=supporting_reasons,
            offsetting_reasons=offsetting_reasons,
            uncertainty_reasons=uncertainty_reasons,
        ),
        analog_search=analog_search,
        supporting_reasons=tuple(supporting_reasons),
        offsetting_reasons=tuple(offsetting_reasons),
        uncertainty_reasons=tuple(uncertainty_reasons),
        uncertainty_flags=tuple(_dedupe_uncertainty_flags(uncertainty_flags)),
        falsification_conditions=falsification_conditions,
    )


def _coerce_dataset(dataset_or_path: ProgramMemoryDataset | Path) -> ProgramMemoryDataset:
    if isinstance(dataset_or_path, ProgramMemoryDataset):
        return dataset_or_path
    return load_program_memory_dataset(Path(dataset_or_path))


def _classify_failure_reason(
    analog: ProgramMemoryAnalog,
    proposal: InterventionProposal,
) -> CounterfactualReason:
    failure_scope = FAILURE_SCOPE_BY_TAXONOMY.get(
        analog.failure_reason_taxonomy,
        "unresolved",
    )
    if not _is_domain_aligned(analog, proposal):
        return CounterfactualReason(
            relation="raises_uncertainty",
            event_id=analog.event_id,
            failure_scope=failure_scope,
            explanation=(
                f"{analog.event_id} is in {analog.domain}, which differs from the "
                "proposal domain, so it is not a clean replay analog."
            ),
        )

    if failure_scope == "unresolved":
        return CounterfactualReason(
            relation="raises_uncertainty",
            event_id=analog.event_id,
            failure_scope=failure_scope,
            explanation=(
                f"{analog.event_id} is recorded as an unresolved miss, so it cannot "
                "by itself establish that the proposal replays a defended prior "
                "failure scope."
            ),
        )

    if failure_scope == "molecule":
        if analog.has_match("molecule"):
            return CounterfactualReason(
                relation="supports_replay",
                event_id=analog.event_id,
                failure_scope=failure_scope,
                explanation=(
                    f"{analog.event_id} is already adjudicated as a molecule-level "
                    "failure for the same asset."
                ),
            )
        return CounterfactualReason(
            relation="raises_uncertainty",
            event_id=analog.event_id,
            failure_scope=failure_scope,
            explanation=(
                f"{analog.event_id} is a molecule-level failure, which does not "
                "transfer to a new asset by default."
            ),
        )

    if failure_scope == "target_class" and analog.has_match("target_class"):
        return CounterfactualReason(
            relation="supports_replay",
            event_id=analog.event_id,
            failure_scope=failure_scope,
            explanation=(
                f"{analog.event_id} already places baggage on the same checked-in "
                f"target class {analog.target_class}."
            ),
        )

    if failure_scope == "target" and analog.has_match("target_symbol"):
        return CounterfactualReason(
            relation="supports_replay",
            event_id=analog.event_id,
            failure_scope=failure_scope,
            explanation=(
                f"{analog.event_id} argues that the same target was already failing at "
                "the target scope in an aligned domain."
            ),
        )

    if failure_scope == "population":
        if analog.has_match("population", "exact_match"):
            return CounterfactualReason(
                relation="supports_replay",
                event_id=analog.event_id,
                failure_scope=failure_scope,
                explanation=(
                    f"{analog.event_id} records a population-scope miss in the same "
                    "checked-in population context."
                ),
            )
        return CounterfactualReason(
            relation="raises_uncertainty",
            event_id=analog.event_id,
            failure_scope=failure_scope,
            explanation=(
                f"{analog.event_id} is a population-scope miss, but the proposal does "
                "not fully match that checked-in population."
            ),
        )

    if failure_scope == "endpoint":
        return CounterfactualReason(
            relation="raises_uncertainty",
            event_id=analog.event_id,
            failure_scope=failure_scope,
            explanation=(
                f"{analog.event_id} is stored as an endpoint mismatch, so it is not "
                "yet a mechanism-level replay claim."
            ),
        )

    return CounterfactualReason(
        relation="raises_uncertainty",
        event_id=analog.event_id,
        failure_scope=failure_scope,
        explanation=(
            f"{analog.event_id} remains relevant history, but the current repository "
            "does not defend it as a direct replay claim."
        ),
    )


def _build_nonfailure_reason(analog: ProgramMemoryAnalog) -> CounterfactualReason:
    qualifier = ""
    if any(flag.code == "composite_mechanism_analog" for flag in analog.uncertainty_flags):
        qualifier = " It is not a perfectly clean single-target counterexample."
    return CounterfactualReason(
        relation="argues_against_replay",
        event_id=analog.event_id,
        failure_scope="nonfailure",
        explanation=(
            f"{analog.event_id} is a checked-in nonfailure anchor in the same "
            f"biological neighborhood ({analog.target_class}).{qualifier}"
        ),
    )


def _determine_status(
    *,
    analog_search: AnalogSearchResult,
    supporting_reasons: list[CounterfactualReason],
    offsetting_reasons: list[CounterfactualReason],
) -> str:
    if analog_search.summary.matched_event_count == 0:
        return "insufficient_history"
    if supporting_reasons and not offsetting_reasons:
        return "replay_supported"
    if supporting_reasons and offsetting_reasons:
        return "replay_inconclusive"
    if offsetting_reasons:
        return "replay_not_supported"
    return "replay_inconclusive"


def _build_summary(
    *,
    status: str,
    analog_search: AnalogSearchResult,
    supporting_reasons: list[CounterfactualReason],
    offsetting_reasons: list[CounterfactualReason],
    uncertainty_reasons: list[CounterfactualReason],
) -> str:
    if status == "insufficient_history":
        return (
            "No checked-in analogs matched this proposal, so the repository cannot "
            "yet call it a replay."
        )
    if status == "replay_supported":
        return (
            f"Checked-in history supports a replay concern via "
            f"{len(supporting_reasons)} aligned failure analog(s) and no aligned "
            "nonfailure anchors."
        )
    if status == "replay_not_supported":
        return (
            f"Checked-in history does not support calling this proposal a replay: "
            f"{len(offsetting_reasons)} nonfailure anchor(s) offset the currently "
            f"available failure analogs ({len(uncertainty_reasons)} remain cautionary)."
        )
    return (
        f"Replay remains inconclusive: {analog_search.summary.matched_event_count} "
        "checked-in analog(s) exist, but the current failure scopes do not cleanly "
        "establish or dismiss replay risk."
    )


def _build_falsification_conditions(
    dataset: ProgramMemoryDataset,
    proposal: InterventionProposal,
    *,
    status: str,
) -> tuple[str, ...]:
    hypothesis = _find_directionality_hypothesis(dataset, proposal)
    conditions: list[str] = []
    if hypothesis is not None:
        conditions.extend(hypothesis.falsification_conditions)
    if status == "replay_supported":
        conditions.append(
            "A clinically aligned checked-in success in the same claimed failure "
            "scope would weaken this replay claim."
        )
    else:
        conditions.append(
            "Repeated adequately engaged failures in the same target and domain "
            "context would strengthen the replay claim."
        )
    return tuple(_dedupe_preserve_order(conditions))


def _find_directionality_hypothesis(
    dataset: ProgramMemoryDataset,
    proposal: InterventionProposal,
) -> ProgramMemoryDirectionalityHypothesis | None:
    proposal_entity_id = proposal.entity_id.strip()
    proposal_symbol = proposal.target_symbol.strip().upper()
    for hypothesis in sorted(
        dataset.directionality_hypotheses,
        key=lambda item: (item.sort_order, item.entity_label.lower(), item.hypothesis_id),
    ):
        if proposal_entity_id and hypothesis.entity_id == proposal_entity_id:
            return hypothesis
        if proposal_symbol and hypothesis.entity_label.upper() == proposal_symbol:
            return hypothesis
    return None


def _is_domain_aligned(analog: ProgramMemoryAnalog, proposal: InterventionProposal) -> bool:
    if not proposal.domain.strip():
        return True
    return analog.has_match("domain", "exact_match")


def _dedupe_uncertainty_flags(
    flags: list[UncertaintyFlag],
) -> list[UncertaintyFlag]:
    seen: set[str] = set()
    deduped: list[UncertaintyFlag] = []
    for flag in flags:
        if flag.code in seen:
            continue
        seen.add(flag.code)
        deduped.append(flag)
    return deduped


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        cleaned = value.strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        deduped.append(cleaned)
    return deduped
