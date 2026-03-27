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
    decision_grade_threshold: float
    top10_ejection_limit: float


@dataclass(frozen=True)
class EngineConfig:
    build: BuildSettings
    stability: StabilitySettings
    gene_layers: dict[str, float]
    module_layers: dict[str, float]
    config_path: Path


def load_config(path: str | Path) -> EngineConfig:
    config_path = Path(path).resolve()
    with config_path.open("rb") as handle:
        raw = tomllib.load(handle)

    return EngineConfig(
        build=BuildSettings(**raw["build"]),
        stability=StabilitySettings(**raw["stability"]),
        gene_layers={key: float(value) for key, value in raw["gene_layers"].items()},
        module_layers={
            key: float(value) for key, value in raw["module_layers"].items()
        },
        config_path=config_path,
    )
