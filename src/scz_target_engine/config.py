from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import tomllib


@dataclass(frozen=True)
class BuildSettings:
    gene_input_file: str
    module_input_file: str
    warning_input_file: str
    output_dir: str
    top_n: int


@dataclass(frozen=True)
class StabilitySettings:
    perturbation_fraction: float
    heuristic_stability_threshold: float
    top10_ejection_limit: float

    @property
    def decision_grade_threshold(self) -> float:
        return self.heuristic_stability_threshold


@dataclass(frozen=True)
class EngineConfig:
    build: BuildSettings
    stability: StabilitySettings
    gene_layers: dict[str, float]
    module_layers: dict[str, float]
    config_path: Path


def load_stability_settings(raw: dict[str, object]) -> StabilitySettings:
    preferred_threshold = raw.get("heuristic_stability_threshold")
    deprecated_threshold = raw.get("decision_grade_threshold")
    if preferred_threshold is None and deprecated_threshold is None:
        raise KeyError(
            "stability.heuristic_stability_threshold is required "
            "(deprecated alias: stability.decision_grade_threshold)"
        )
    if (
        preferred_threshold is not None
        and deprecated_threshold is not None
        and float(preferred_threshold) != float(deprecated_threshold)
    ):
        raise ValueError(
            "stability.heuristic_stability_threshold and deprecated "
            "stability.decision_grade_threshold must match when both are set"
        )

    threshold = preferred_threshold
    if threshold is None:
        threshold = deprecated_threshold

    return StabilitySettings(
        perturbation_fraction=float(raw["perturbation_fraction"]),
        heuristic_stability_threshold=float(threshold),
        top10_ejection_limit=float(raw["top10_ejection_limit"]),
    )


def load_config(path: str | Path) -> EngineConfig:
    config_path = Path(path).resolve()
    with config_path.open("rb") as handle:
        raw = tomllib.load(handle)

    return EngineConfig(
        build=BuildSettings(**raw["build"]),
        stability=load_stability_settings(raw["stability"]),
        gene_layers={key: float(value) for key, value in raw["gene_layers"].items()},
        module_layers={
            key: float(value) for key, value in raw["module_layers"].items()
        },
        config_path=config_path,
    )
