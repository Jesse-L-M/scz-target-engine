from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import tomllib

from scz_target_engine.decision_vector import DOMAIN_HEAD_DEFINITIONS


@dataclass(frozen=True)
class PolicyAdjustmentWeights:
    low_coverage_penalty: float
    missing_head_penalty: float
    partial_head_penalty: float
    warning_penalty_per_warning: float
    directionality_open_risk_penalty: float
    directionality_contradiction_penalty: float
    directionality_falsification_penalty: float
    replay_supported_penalty: float
    replay_inconclusive_penalty: float
    replay_not_supported_bonus: float
    replay_supporting_reason_penalty: float
    replay_offsetting_reason_bonus: float
    replay_uncertainty_reason_penalty: float
    replay_uncertainty_flag_penalty: float


@dataclass(frozen=True)
class PolicyDefinition:
    policy_id: str
    label: str
    description: str
    source_file: str
    domain_weights: tuple[tuple[str, float], ...]
    adjustment_weights: PolicyAdjustmentWeights

    @property
    def total_domain_weight(self) -> float:
        return sum(weight for _, weight in self.domain_weights)

    @property
    def primary_domain_slug(self) -> str:
        return max(
            self.domain_weights,
            key=lambda item: item[1],
        )[0]


def load_policy_definitions(policy_dir: Path) -> tuple[PolicyDefinition, ...]:
    resolved_dir = policy_dir.resolve()
    if not resolved_dir.exists():
        raise FileNotFoundError(f"policy config directory does not exist: {resolved_dir}")

    policy_files = sorted(resolved_dir.glob("*.toml"))
    if not policy_files:
        raise ValueError(f"no policy config files found in {resolved_dir}")

    valid_domain_slugs = {definition.slug for definition in DOMAIN_HEAD_DEFINITIONS}
    policies = tuple(
        _load_policy_definition(
            path,
            valid_domain_slugs=valid_domain_slugs,
            repo_root=resolved_dir.parents[1],
        )
        for path in policy_files
    )
    seen_policy_ids: set[str] = set()
    for policy in policies:
        if policy.policy_id in seen_policy_ids:
            raise ValueError(f"duplicate policy_id {policy.policy_id!r} in {resolved_dir}")
        seen_policy_ids.add(policy.policy_id)
    return policies


def serialize_policy_definition(policy: PolicyDefinition) -> dict[str, object]:
    return {
        "policy_id": policy.policy_id,
        "label": policy.label,
        "description": policy.description,
        "source_file": policy.source_file,
        "primary_domain_slug": policy.primary_domain_slug,
        "domain_weights": [
            {
                "domain_slug": domain_slug,
                "weight": weight,
            }
            for domain_slug, weight in policy.domain_weights
        ],
        "adjustment_weights": {
            "low_coverage_penalty": policy.adjustment_weights.low_coverage_penalty,
            "missing_head_penalty": policy.adjustment_weights.missing_head_penalty,
            "partial_head_penalty": policy.adjustment_weights.partial_head_penalty,
            "warning_penalty_per_warning": (
                policy.adjustment_weights.warning_penalty_per_warning
            ),
            "directionality_open_risk_penalty": (
                policy.adjustment_weights.directionality_open_risk_penalty
            ),
            "directionality_contradiction_penalty": (
                policy.adjustment_weights.directionality_contradiction_penalty
            ),
            "directionality_falsification_penalty": (
                policy.adjustment_weights.directionality_falsification_penalty
            ),
            "replay_supported_penalty": policy.adjustment_weights.replay_supported_penalty,
            "replay_inconclusive_penalty": (
                policy.adjustment_weights.replay_inconclusive_penalty
            ),
            "replay_not_supported_bonus": (
                policy.adjustment_weights.replay_not_supported_bonus
            ),
            "replay_supporting_reason_penalty": (
                policy.adjustment_weights.replay_supporting_reason_penalty
            ),
            "replay_offsetting_reason_bonus": (
                policy.adjustment_weights.replay_offsetting_reason_bonus
            ),
            "replay_uncertainty_reason_penalty": (
                policy.adjustment_weights.replay_uncertainty_reason_penalty
            ),
            "replay_uncertainty_flag_penalty": (
                policy.adjustment_weights.replay_uncertainty_flag_penalty
            ),
        },
    }


def _load_policy_definition(
    path: Path,
    *,
    valid_domain_slugs: set[str],
    repo_root: Path,
) -> PolicyDefinition:
    with path.open("rb") as handle:
        raw = tomllib.load(handle)

    domain_weight_payload = _require_mapping(raw.get("domain_weights"), f"{path}.domain_weights")
    domain_weights: list[tuple[str, float]] = []
    for domain_slug, weight_value in domain_weight_payload.items():
        if domain_slug not in valid_domain_slugs:
            available = ", ".join(sorted(valid_domain_slugs))
            raise ValueError(
                f"{path}.domain_weights contains unknown domain {domain_slug!r}; "
                f"available domains: {available}"
            )
        weight = float(weight_value)
        if weight <= 0:
            raise ValueError(f"{path}.domain_weights.{domain_slug} must be positive")
        domain_weights.append((domain_slug, weight))
    if not domain_weights:
        raise ValueError(f"{path}.domain_weights must not be empty")

    total_weight = sum(weight for _, weight in domain_weights)
    if abs(total_weight - 1.0) > 1e-6:
        raise ValueError(f"{path}.domain_weights must sum to 1.0")

    adjustment_payload = _require_mapping(raw.get("adjustments"), f"{path}.adjustments")
    adjustment_weights = PolicyAdjustmentWeights(
        low_coverage_penalty=float(adjustment_payload["low_coverage_penalty"]),
        missing_head_penalty=float(adjustment_payload["missing_head_penalty"]),
        partial_head_penalty=float(adjustment_payload["partial_head_penalty"]),
        warning_penalty_per_warning=float(
            adjustment_payload["warning_penalty_per_warning"]
        ),
        directionality_open_risk_penalty=float(
            adjustment_payload["directionality_open_risk_penalty"]
        ),
        directionality_contradiction_penalty=float(
            adjustment_payload["directionality_contradiction_penalty"]
        ),
        directionality_falsification_penalty=float(
            adjustment_payload["directionality_falsification_penalty"]
        ),
        replay_supported_penalty=float(adjustment_payload["replay_supported_penalty"]),
        replay_inconclusive_penalty=float(
            adjustment_payload["replay_inconclusive_penalty"]
        ),
        replay_not_supported_bonus=float(
            adjustment_payload["replay_not_supported_bonus"]
        ),
        replay_supporting_reason_penalty=float(
            adjustment_payload["replay_supporting_reason_penalty"]
        ),
        replay_offsetting_reason_bonus=float(
            adjustment_payload["replay_offsetting_reason_bonus"]
        ),
        replay_uncertainty_reason_penalty=float(
            adjustment_payload["replay_uncertainty_reason_penalty"]
        ),
        replay_uncertainty_flag_penalty=float(
            adjustment_payload["replay_uncertainty_flag_penalty"]
        ),
    )

    try:
        source_file = str(path.resolve().relative_to(repo_root.resolve()))
    except ValueError:
        source_file = str(path.resolve())

    return PolicyDefinition(
        policy_id=_require_text(raw.get("policy_id"), f"{path}.policy_id"),
        label=_require_text(raw.get("label"), f"{path}.label"),
        description=_require_text(raw.get("description"), f"{path}.description"),
        source_file=source_file,
        domain_weights=tuple(domain_weights),
        adjustment_weights=adjustment_weights,
    )


def _require_mapping(value: object, field_name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a TOML table")
    return dict(value)


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()
