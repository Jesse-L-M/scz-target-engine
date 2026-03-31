from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import hashlib
import json
from math import isclose
import os
from pathlib import Path
import re
from typing import Any

from scz_target_engine.io import read_json, write_json


REPO_ROOT = Path(__file__).resolve().parents[3]

PROSPECTIVE_PREDICTION_REGISTRATION_ARTIFACT_NAME = (
    "prospective_prediction_registration"
)
PROSPECTIVE_FORECAST_OUTCOME_LOG_ARTIFACT_NAME = "prospective_forecast_outcome_log"
PROSPECTIVE_SCHEMA_VERSION = "v1"
DEFAULT_PROSPECTIVE_REGISTRY_DIR = REPO_ROOT / "data" / "prospective_registry"
DEFAULT_PROSPECTIVE_REGISTRATIONS_DIR = (
    DEFAULT_PROSPECTIVE_REGISTRY_DIR / "registrations"
)
DEFAULT_PROSPECTIVE_OUTCOMES_DIR = DEFAULT_PROSPECTIVE_REGISTRY_DIR / "outcomes"
PROSPECTIVE_FORECAST_TYPE = "reviewed_packet_disposition"
PROSPECTIVE_SCORING_TARGET = "multiclass_single_label"
VALID_RECONCILIATION_STATUSES = (
    "pending",
    "resolved",
    "conflicted",
    "out_of_window",
)


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value.strip()


def _require_mapping(value: object, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be an object")
    return value


def _require_list(value: object, field_name: str) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    return value


def _require_string_list(
    value: object,
    field_name: str,
    *,
    min_length: int = 1,
) -> tuple[str, ...]:
    items = tuple(_require_text(item, f"{field_name}[]") for item in _require_list(value, field_name))
    if len(items) < min_length:
        raise ValueError(f"{field_name} must contain at least {min_length} value(s)")
    return items


def _require_iso_date(value: object, field_name: str) -> str:
    text = _require_text(value, field_name)
    try:
        date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO date") from exc
    return text


def _normalize_datetime_text(text: str) -> str:
    return text[:-1] + "+00:00" if text.endswith("Z") else text


def _require_iso_datetime(value: object, field_name: str) -> str:
    text = _require_text(value, field_name)
    try:
        parsed = datetime.fromisoformat(_normalize_datetime_text(text))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO datetime") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must include an explicit timezone offset")
    return text


def _require_float(value: object, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be a number")
    return float(value)


def _require_sha256(value: object, field_name: str) -> str:
    digest = _require_text(value, field_name).lower()
    if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
        raise ValueError(f"{field_name} must be a lowercase hex sha256 digest")
    return digest


def _resolve_path(reference: str, *, base_dir: Path) -> Path:
    path = Path(reference)
    if path.is_absolute():
        return path.resolve()
    return (base_dir / path).resolve()


def _compute_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _iter_json_files(directory: Path) -> tuple[Path, ...]:
    resolved_dir = directory.resolve()
    if not resolved_dir.exists():
        return ()
    return tuple(
        sorted(
            path
            for path in resolved_dir.iterdir()
            if path.is_file() and path.suffix == ".json"
        )
    )


def _canonical_json_sha256(payload: object) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
    ).hexdigest()


def _require_registration_output_is_new(output_path: Path) -> None:
    if output_path.exists():
        raise ValueError(
            "prospective registrations are immutable; output_file already exists: "
            f"{output_path}"
        )


def _require_registration_id_is_unique(
    registration_id: str,
    *,
    registrations_dir: Path,
    output_path: Path,
) -> None:
    for path in _iter_json_files(registrations_dir):
        if path.resolve() == output_path.resolve():
            continue
        payload = read_json(path)
        if not isinstance(payload, dict):
            continue
        if payload.get("schema_name") != PROSPECTIVE_PREDICTION_REGISTRATION_ARTIFACT_NAME:
            continue
        existing_registration_id = payload.get("registration_id")
        if isinstance(existing_registration_id, str) and existing_registration_id.strip() == registration_id:
            raise ValueError(
                "registration_id must be unique within the registrations directory; "
                f"{registration_id} already exists in {path}"
            )


def _outcome_window_bounds(
    registration: ProspectivePredictionRegistration,
) -> tuple[date, date]:
    outcome_window = _require_mapping(
        registration.frozen_forecast_payload.get("outcome_window"),
        "registration.frozen_forecast_payload.outcome_window",
    )
    opens_on = date.fromisoformat(
        _require_iso_date(
            outcome_window.get("opens_on"),
            "registration.frozen_forecast_payload.outcome_window.opens_on",
        )
    )
    closes_on = date.fromisoformat(
        _require_iso_date(
            outcome_window.get("closes_on"),
            "registration.frozen_forecast_payload.outcome_window.closes_on",
        )
    )
    return opens_on, closes_on


def _outcome_record_is_within_window(
    record: "ProspectiveForecastOutcomeRecord",
    *,
    registration: ProspectivePredictionRegistration,
) -> bool:
    opens_on, closes_on = _outcome_window_bounds(registration)
    observed_at = date.fromisoformat(record.observed_at)
    return opens_on <= observed_at <= closes_on


def _sort_outcome_records(
    records: list["ProspectiveForecastOutcomeRecord"],
) -> tuple["ProspectiveForecastOutcomeRecord", ...]:
    return tuple(
        sorted(
            records,
            key=lambda record: (record.observed_at, record.outcome_record_id),
        )
    )


def _resolve_packet(packet_payload: dict[str, object], *, packet_id: str) -> tuple[int, dict[str, object]]:
    packets = _require_list(packet_payload.get("packets"), "hypothesis_packets_v1.packets")
    matches: list[tuple[int, dict[str, object]]] = []
    for index, item in enumerate(packets):
        packet = _require_mapping(item, f"hypothesis_packets_v1.packets[{index}]")
        candidate_id = _require_text(
            packet.get("packet_id"),
            f"hypothesis_packets_v1.packets[{index}].packet_id",
        )
        if candidate_id == packet_id:
            matches.append((index, packet))
    if not matches:
        raise ValueError(f"unknown packet_id in hypothesis packet artifact: {packet_id}")
    if len(matches) > 1:
        raise ValueError(f"packet_id must be unique inside hypothesis packet artifact: {packet_id}")
    return matches[0]


def _resolve_packet_pointer(
    packet_payload: dict[str, object],
    *,
    packet_pointer: str,
) -> tuple[int, dict[str, object]]:
    pointer = _require_text(packet_pointer, "packet_artifact.packet_pointer")
    match = re.fullmatch(r"/packets/(\d+)", pointer)
    if match is None:
        raise ValueError("packet_artifact.packet_pointer must use /packets/<index>")
    packet_index = int(match.group(1))
    packets = _require_list(packet_payload.get("packets"), "hypothesis_packets_v1.packets")
    if packet_index >= len(packets):
        raise ValueError(
            "packet_artifact.packet_pointer points past the end of the hypothesis packet artifact"
        )
    packet = _require_mapping(
        packets[packet_index],
        f"hypothesis_packets_v1.packets[{packet_index}]",
    )
    return packet_index, packet


def _default_registration_id(packet_id: str, registered_at: str) -> str:
    slug_source = f"{packet_id}-{registered_at}"
    slug = re.sub(r"[^a-z0-9]+", "_", slug_source.lower()).strip("_")
    return f"forecast_{slug}"


def _normalize_option_probabilities(
    option_probabilities: dict[str, float],
    *,
    options: tuple[str, ...],
    field_name: str,
) -> dict[str, float]:
    missing = [option for option in options if option not in option_probabilities]
    extra = [option for option in option_probabilities if option not in options]
    if missing or extra:
        messages: list[str] = []
        if missing:
            messages.append("missing " + ", ".join(missing))
        if extra:
            messages.append("unexpected " + ", ".join(sorted(extra)))
        raise ValueError(f"{field_name} must match the packet decision options exactly: {'; '.join(messages)}")

    normalized = {
        option: _require_float(option_probabilities[option], f"{field_name}.{option}")
        for option in options
    }
    for option, probability in normalized.items():
        if probability < 0.0 or probability > 1.0:
            raise ValueError(f"{field_name}.{option} must be between 0 and 1 inclusive")
    probability_sum = sum(normalized.values())
    if not isclose(probability_sum, 1.0, rel_tol=0.0, abs_tol=1e-9):
        raise ValueError(f"{field_name} must sum to 1.0")
    return normalized


def _coerce_option_probabilities(
    option_probabilities: object,
    *,
    options: tuple[str, ...],
    field_name: str,
) -> dict[str, float]:
    mapping = _require_mapping(option_probabilities, field_name)
    return _normalize_option_probabilities(
        {
            _require_text(option, f"{field_name}.<key>"): _require_float(
                value,
                f"{field_name}.{option}",
            )
            for option, value in mapping.items()
        },
        options=options,
        field_name=field_name,
    )


def _build_packet_scope(packet: dict[str, object]) -> dict[str, str]:
    return {
        "entity_id": _require_text(packet.get("entity_id"), "packet.entity_id"),
        "entity_label": _require_text(packet.get("entity_label"), "packet.entity_label"),
        "entity_type": _require_text(packet.get("entity_type"), "packet.entity_type"),
        "policy_id": _require_text(packet.get("policy_id"), "packet.policy_id"),
        "policy_label": _require_text(packet.get("policy_label"), "packet.policy_label"),
        "priority_domain": _require_text(
            packet.get("priority_domain"),
            "packet.priority_domain",
        ),
    }


def _validate_packet_scope(
    packet_scope: dict[str, object],
    *,
    frozen_packet_payload: dict[str, object],
) -> dict[str, str]:
    validated_scope = {
        key: _require_text(packet_scope.get(key), f"packet_scope.{key}")
        for key in (
            "entity_id",
            "entity_label",
            "entity_type",
            "policy_id",
            "policy_label",
            "priority_domain",
        )
    }
    for key, expected_value in _build_packet_scope(frozen_packet_payload).items():
        if validated_scope[key] != expected_value:
            raise ValueError(f"packet_scope.{key} must match frozen_packet_payload.{key}")
    return validated_scope


def _validate_frozen_forecast_payload(
    payload: dict[str, object],
    *,
    frozen_packet_payload: dict[str, object],
) -> dict[str, object]:
    forecast_type = _require_text(
        payload.get("forecast_type"),
        "frozen_forecast_payload.forecast_type",
    )
    if forecast_type != PROSPECTIVE_FORECAST_TYPE:
        raise ValueError(
            "frozen_forecast_payload.forecast_type must remain reviewed_packet_disposition"
        )
    scoring_target = _require_text(
        payload.get("scoring_target"),
        "frozen_forecast_payload.scoring_target",
    )
    if scoring_target != PROSPECTIVE_SCORING_TARGET:
        raise ValueError(
            "frozen_forecast_payload.scoring_target must remain multiclass_single_label"
        )

    decision_focus = _require_mapping(
        frozen_packet_payload.get("decision_focus"),
        "frozen_packet_payload.decision_focus",
    )
    options = _require_string_list(
        decision_focus.get("decision_options"),
        "frozen_packet_payload.decision_focus.decision_options",
    )
    outcome_options = _require_string_list(
        payload.get("outcome_options"),
        "frozen_forecast_payload.outcome_options",
    )
    if outcome_options != options:
        raise ValueError(
            "frozen_forecast_payload.outcome_options must match the reviewed packet decision options"
        )

    option_probabilities = _coerce_option_probabilities(
        payload.get("option_probabilities"),
        options=options,
        field_name="frozen_forecast_payload.option_probabilities",
    )
    predicted_outcome = _require_text(
        payload.get("predicted_outcome"),
        "frozen_forecast_payload.predicted_outcome",
    )
    if predicted_outcome not in options:
        raise ValueError(
            "frozen_forecast_payload.predicted_outcome must be one of the packet decision options"
        )

    max_probability = max(option_probabilities.values())
    winning_options = sorted(
        option for option, probability in option_probabilities.items() if isclose(probability, max_probability, rel_tol=0.0, abs_tol=1e-12)
    )
    if len(winning_options) != 1:
        raise ValueError(
            "frozen_forecast_payload.option_probabilities must expose a single highest-probability outcome"
        )
    if predicted_outcome != winning_options[0]:
        raise ValueError(
            "frozen_forecast_payload.predicted_outcome must equal the highest-probability option"
        )

    outcome_window = _require_mapping(
        payload.get("outcome_window"),
        "frozen_forecast_payload.outcome_window",
    )
    opens_on = _require_iso_date(
        outcome_window.get("opens_on"),
        "frozen_forecast_payload.outcome_window.opens_on",
    )
    closes_on = _require_iso_date(
        outcome_window.get("closes_on"),
        "frozen_forecast_payload.outcome_window.closes_on",
    )
    if date.fromisoformat(opens_on) > date.fromisoformat(closes_on):
        raise ValueError(
            "frozen_forecast_payload.outcome_window.opens_on must not be after closes_on"
        )

    rationale = _require_string_list(
        payload.get("rationale"),
        "frozen_forecast_payload.rationale",
    )

    return {
        "forecast_type": forecast_type,
        "scoring_target": scoring_target,
        "outcome_options": list(options),
        "option_probabilities": {
            option: option_probabilities[option] for option in options
        },
        "predicted_outcome": predicted_outcome,
        "outcome_window": {
            "opens_on": opens_on,
            "closes_on": closes_on,
        },
        "rationale": list(rationale),
    }


@dataclass(frozen=True)
class PacketArtifactReference:
    schema_name: str
    schema_version: str
    artifact_path: str
    artifact_sha256: str
    packet_id: str
    packet_pointer: str

    def __post_init__(self) -> None:
        if self.schema_name != "hypothesis_packets_v1":
            raise ValueError("schema_name must remain hypothesis_packets_v1")
        if self.schema_version != "v1":
            raise ValueError("schema_version must remain v1 for hypothesis_packets_v1")
        _require_text(self.artifact_path, "artifact_path")
        _require_sha256(self.artifact_sha256, "artifact_sha256")
        _require_text(self.packet_id, "packet_id")
        _require_text(self.packet_pointer, "packet_pointer")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_name": self.schema_name,
            "schema_version": self.schema_version,
            "artifact_path": self.artifact_path,
            "artifact_sha256": self.artifact_sha256,
            "packet_id": self.packet_id,
            "packet_pointer": self.packet_pointer,
        }


@dataclass(frozen=True)
class ProspectivePredictionRegistration:
    schema_name: str
    schema_version: str
    registration_id: str
    registered_at: str
    registered_by: str
    packet_artifact: PacketArtifactReference
    packet_scope: dict[str, str]
    frozen_packet_payload: dict[str, object]
    frozen_packet_payload_sha256: str
    frozen_forecast_payload: dict[str, object]
    frozen_forecast_payload_sha256: str
    notes: str = ""

    def __post_init__(self) -> None:
        if self.schema_name != PROSPECTIVE_PREDICTION_REGISTRATION_ARTIFACT_NAME:
            raise ValueError(
                "schema_name must remain prospective_prediction_registration"
            )
        if self.schema_version != PROSPECTIVE_SCHEMA_VERSION:
            raise ValueError("schema_version must remain v1 for prospective registrations")
        _require_text(self.registration_id, "registration_id")
        _require_iso_datetime(self.registered_at, "registered_at")
        _require_text(self.registered_by, "registered_by")
        _require_sha256(
            self.frozen_packet_payload_sha256,
            "frozen_packet_payload_sha256",
        )
        _require_sha256(
            self.frozen_forecast_payload_sha256,
            "frozen_forecast_payload_sha256",
        )

    def to_dict(self) -> dict[str, object]:
        payload = {
            "schema_name": self.schema_name,
            "schema_version": self.schema_version,
            "registration_id": self.registration_id,
            "registered_at": self.registered_at,
            "registered_by": self.registered_by,
            "packet_artifact": self.packet_artifact.to_dict(),
            "packet_scope": dict(self.packet_scope),
            "frozen_packet_payload": self.frozen_packet_payload,
            "frozen_packet_payload_sha256": self.frozen_packet_payload_sha256,
            "frozen_forecast_payload": self.frozen_forecast_payload,
            "frozen_forecast_payload_sha256": self.frozen_forecast_payload_sha256,
        }
        if self.notes:
            payload["notes"] = self.notes
        return payload


@dataclass(frozen=True)
class OutcomeEvidenceReference:
    source_path: str
    source_sha256: str
    source_pointer: str
    summary: str

    def __post_init__(self) -> None:
        _require_text(self.source_path, "source_path")
        _require_sha256(self.source_sha256, "source_sha256")
        _require_text(self.summary, "summary")

    def to_dict(self) -> dict[str, object]:
        payload = {
            "source_path": self.source_path,
            "source_sha256": self.source_sha256,
            "summary": self.summary,
        }
        if self.source_pointer:
            payload["source_pointer"] = self.source_pointer
        return payload


@dataclass(frozen=True)
class ProspectiveForecastOutcomeRecord:
    outcome_record_id: str
    registration_artifact_path: str
    registration_artifact_sha256: str
    registration_id: str
    observed_at: str
    observed_outcome: str
    source_evidence: OutcomeEvidenceReference
    notes: str = ""

    def __post_init__(self) -> None:
        _require_text(self.outcome_record_id, "outcome_record_id")
        _require_text(self.registration_artifact_path, "registration_artifact_path")
        _require_sha256(
            self.registration_artifact_sha256,
            "registration_artifact_sha256",
        )
        _require_text(self.registration_id, "registration_id")
        _require_iso_date(self.observed_at, "observed_at")
        _require_text(self.observed_outcome, "observed_outcome")

    def to_dict(self) -> dict[str, object]:
        payload = {
            "outcome_record_id": self.outcome_record_id,
            "registration_artifact_path": self.registration_artifact_path,
            "registration_artifact_sha256": self.registration_artifact_sha256,
            "registration_id": self.registration_id,
            "observed_at": self.observed_at,
            "observed_outcome": self.observed_outcome,
            "source_evidence": self.source_evidence.to_dict(),
        }
        if self.notes:
            payload["notes"] = self.notes
        return payload


@dataclass(frozen=True)
class ProspectiveForecastOutcomeLog:
    schema_name: str
    schema_version: str
    outcome_log_id: str
    logged_at: str
    logged_by: str
    outcome_records: tuple[ProspectiveForecastOutcomeRecord, ...]
    notes: str = ""

    def __post_init__(self) -> None:
        if self.schema_name != PROSPECTIVE_FORECAST_OUTCOME_LOG_ARTIFACT_NAME:
            raise ValueError("schema_name must remain prospective_forecast_outcome_log")
        if self.schema_version != PROSPECTIVE_SCHEMA_VERSION:
            raise ValueError("schema_version must remain v1 for prospective outcome logs")
        _require_text(self.outcome_log_id, "outcome_log_id")
        _require_iso_datetime(self.logged_at, "logged_at")
        _require_text(self.logged_by, "logged_by")
        if not self.outcome_records:
            raise ValueError("outcome_records must contain at least one record")

    def to_dict(self) -> dict[str, object]:
        payload = {
            "schema_name": self.schema_name,
            "schema_version": self.schema_version,
            "outcome_log_id": self.outcome_log_id,
            "logged_at": self.logged_at,
            "logged_by": self.logged_by,
            "outcome_records": [record.to_dict() for record in self.outcome_records],
        }
        if self.notes:
            payload["notes"] = self.notes
        return payload


@dataclass(frozen=True)
class ProspectiveForecastReconciliation:
    registration_id: str
    packet_id: str
    resolution_status: str
    predicted_outcome: str
    option_probabilities: dict[str, float]
    observed_outcome: str = ""
    observed_at: str = ""
    outcome_record_ids: tuple[str, ...] = ()
    conflict_outcomes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_text(self.registration_id, "registration_id")
        _require_text(self.packet_id, "packet_id")
        if self.resolution_status not in VALID_RECONCILIATION_STATUSES:
            raise ValueError(
                "resolution_status must be one of pending, resolved, conflicted"
            )

    def to_dict(self) -> dict[str, object]:
        payload = {
            "registration_id": self.registration_id,
            "packet_id": self.packet_id,
            "resolution_status": self.resolution_status,
            "predicted_outcome": self.predicted_outcome,
            "option_probabilities": dict(self.option_probabilities),
            "outcome_record_ids": list(self.outcome_record_ids),
        }
        if self.observed_outcome:
            payload["observed_outcome"] = self.observed_outcome
        if self.observed_at:
            payload["observed_at"] = self.observed_at
        if self.conflict_outcomes:
            payload["conflict_outcomes"] = list(self.conflict_outcomes)
        return payload


@dataclass(frozen=True)
class ProspectiveForecastScoringRecord:
    registration_id: str
    packet_id: str
    predicted_outcome: str
    observed_outcome: str
    option_probabilities: dict[str, float]
    observed_at: str

    def to_dict(self) -> dict[str, object]:
        return {
            "registration_id": self.registration_id,
            "packet_id": self.packet_id,
            "predicted_outcome": self.predicted_outcome,
            "observed_outcome": self.observed_outcome,
            "option_probabilities": dict(self.option_probabilities),
            "observed_at": self.observed_at,
        }


def build_prospective_prediction_registration_payload(
    hypothesis_payload: dict[str, object],
    *,
    hypothesis_artifact_ref: str,
    hypothesis_artifact_sha256: str,
    hypothesis_schema_version: str,
    packet_id: str,
    registered_at: str,
    registered_by: str,
    predicted_outcome: str,
    option_probabilities: dict[str, float],
    outcome_window_closes_on: str,
    outcome_window_opens_on: str | None = None,
    rationale: list[str] | tuple[str, ...] | None = None,
    registration_id: str | None = None,
    notes: str = "",
) -> dict[str, object]:
    registered_at_text = _require_iso_datetime(registered_at, "registered_at")
    opens_on = (
        _require_iso_date(outcome_window_opens_on, "outcome_window_opens_on")
        if outcome_window_opens_on is not None
        else registered_at_text[:10]
    )
    closes_on = _require_iso_date(
        outcome_window_closes_on,
        "outcome_window_closes_on",
    )
    if date.fromisoformat(opens_on) > date.fromisoformat(closes_on):
        raise ValueError("outcome_window_opens_on must not be after outcome_window_closes_on")

    packet_index, packet = _resolve_packet(
        hypothesis_payload,
        packet_id=_require_text(packet_id, "packet_id"),
    )
    decision_focus = _require_mapping(packet.get("decision_focus"), "packet.decision_focus")
    options = _require_string_list(
        decision_focus.get("decision_options"),
        "packet.decision_focus.decision_options",
    )
    normalized_probabilities = _normalize_option_probabilities(
        option_probabilities,
        options=options,
        field_name="option_probabilities",
    )
    predicted = _require_text(predicted_outcome, "predicted_outcome")
    max_probability = max(normalized_probabilities.values())
    winning_options = [
        option
        for option, probability in normalized_probabilities.items()
        if isclose(probability, max_probability, rel_tol=0.0, abs_tol=1e-12)
    ]
    if len(winning_options) != 1:
        raise ValueError("option_probabilities must expose a single highest-probability outcome")
    if predicted != winning_options[0]:
        raise ValueError("predicted_outcome must equal the highest-probability option")
    rationale_items = tuple(
        _require_text(item, "rationale[]") for item in (rationale or ())
    )
    if not rationale_items:
        raise ValueError("rationale must contain at least one entry")

    resolved_registration_id = (
        _require_text(registration_id, "registration_id")
        if registration_id is not None
        else _default_registration_id(packet_id, registered_at_text)
    )

    frozen_packet_payload = dict(packet)
    frozen_forecast_payload = {
        "forecast_type": PROSPECTIVE_FORECAST_TYPE,
        "scoring_target": PROSPECTIVE_SCORING_TARGET,
        "outcome_options": list(options),
        "option_probabilities": {
            option: normalized_probabilities[option] for option in options
        },
        "predicted_outcome": predicted,
        "outcome_window": {
            "opens_on": opens_on,
            "closes_on": closes_on,
        },
        "rationale": list(rationale_items),
    }
    registration = ProspectivePredictionRegistration(
        schema_name=PROSPECTIVE_PREDICTION_REGISTRATION_ARTIFACT_NAME,
        schema_version=PROSPECTIVE_SCHEMA_VERSION,
        registration_id=resolved_registration_id,
        registered_at=registered_at_text,
        registered_by=_require_text(registered_by, "registered_by"),
        packet_artifact=PacketArtifactReference(
            schema_name="hypothesis_packets_v1",
            schema_version=_require_text(
                hypothesis_schema_version,
                "hypothesis_schema_version",
            ),
            artifact_path=_require_text(
                hypothesis_artifact_ref,
                "hypothesis_artifact_ref",
            ),
            artifact_sha256=_require_sha256(
                hypothesis_artifact_sha256,
                "hypothesis_artifact_sha256",
            ),
            packet_id=_require_text(packet_id, "packet_id"),
            packet_pointer=f"/packets/{packet_index}",
        ),
        packet_scope=_build_packet_scope(frozen_packet_payload),
        frozen_packet_payload=frozen_packet_payload,
        frozen_packet_payload_sha256=_canonical_json_sha256(frozen_packet_payload),
        frozen_forecast_payload=frozen_forecast_payload,
        frozen_forecast_payload_sha256=_canonical_json_sha256(
            frozen_forecast_payload
        ),
        notes=str(notes).strip(),
    )
    return registration.to_dict()


def materialize_prospective_prediction_registration(
    hypothesis_artifact_file: Path,
    *,
    packet_id: str,
    output_file: Path,
    registered_at: str,
    registered_by: str,
    predicted_outcome: str,
    option_probabilities: dict[str, float],
    outcome_window_closes_on: str,
    outcome_window_opens_on: str | None = None,
    rationale: list[str] | tuple[str, ...] | None = None,
    registration_id: str | None = None,
    notes: str = "",
) -> dict[str, object]:
    from scz_target_engine.artifacts.validators import load_artifact

    resolved_hypothesis_path = hypothesis_artifact_file.resolve()
    resolved_output_path = output_file.resolve()
    _require_registration_output_is_new(resolved_output_path)
    artifact = load_artifact(
        resolved_hypothesis_path,
        artifact_name="hypothesis_packets_v1",
    )
    payload = build_prospective_prediction_registration_payload(
        dict(artifact.payload),
        hypothesis_artifact_ref=os.path.relpath(
            resolved_hypothesis_path,
            resolved_output_path.parent,
        ),
        hypothesis_artifact_sha256=_compute_sha256(resolved_hypothesis_path),
        hypothesis_schema_version=artifact.schema_version,
        packet_id=packet_id,
        registered_at=registered_at,
        registered_by=registered_by,
        predicted_outcome=predicted_outcome,
        option_probabilities=option_probabilities,
        outcome_window_closes_on=outcome_window_closes_on,
        outcome_window_opens_on=outcome_window_opens_on,
        rationale=rationale,
        registration_id=registration_id,
        notes=notes,
    )
    validate_prospective_prediction_registration_payload(
        payload,
        artifact_path=resolved_output_path,
    )
    _require_registration_id_is_unique(
        str(payload["registration_id"]),
        registrations_dir=resolved_output_path.parent,
        output_path=resolved_output_path,
    )
    write_json(resolved_output_path, payload)
    return payload


def validate_prospective_prediction_registration_payload(
    payload: dict[str, object],
    *,
    artifact_path: Path,
) -> ProspectivePredictionRegistration:
    from scz_target_engine.artifacts.validators import load_artifact

    schema_name = _require_text(payload.get("schema_name"), "schema_name")
    schema_version = _require_text(payload.get("schema_version"), "schema_version")
    packet_artifact_payload = _require_mapping(
        payload.get("packet_artifact"),
        "packet_artifact",
    )
    packet_artifact = PacketArtifactReference(
        schema_name=_require_text(
            packet_artifact_payload.get("schema_name"),
            "packet_artifact.schema_name",
        ),
        schema_version=_require_text(
            packet_artifact_payload.get("schema_version"),
            "packet_artifact.schema_version",
        ),
        artifact_path=_require_text(
            packet_artifact_payload.get("artifact_path"),
            "packet_artifact.artifact_path",
        ),
        artifact_sha256=_require_sha256(
            packet_artifact_payload.get("artifact_sha256"),
            "packet_artifact.artifact_sha256",
        ),
        packet_id=_require_text(
            packet_artifact_payload.get("packet_id"),
            "packet_artifact.packet_id",
        ),
        packet_pointer=_require_text(
            packet_artifact_payload.get("packet_pointer"),
            "packet_artifact.packet_pointer",
        ),
    )
    resolved_packet_artifact_path = _resolve_path(
        packet_artifact.artifact_path,
        base_dir=artifact_path.resolve().parent,
    )
    if not resolved_packet_artifact_path.is_file():
        raise ValueError(
            f"packet_artifact.artifact_path points to a missing file: {packet_artifact.artifact_path}"
        )
    actual_packet_artifact_sha256 = _compute_sha256(resolved_packet_artifact_path)
    if actual_packet_artifact_sha256 != packet_artifact.artifact_sha256:
        raise ValueError("packet_artifact.artifact_sha256 does not match the referenced artifact")

    hypothesis_artifact = load_artifact(
        resolved_packet_artifact_path,
        artifact_name="hypothesis_packets_v1",
    )
    if hypothesis_artifact.schema_version != packet_artifact.schema_version:
        raise ValueError("packet_artifact.schema_version must match the referenced artifact")
    hypothesis_payload = dict(hypothesis_artifact.payload)
    packet_index, referenced_packet = _resolve_packet_pointer(
        hypothesis_payload,
        packet_pointer=packet_artifact.packet_pointer,
    )
    referenced_packet_id = _require_text(
        referenced_packet.get("packet_id"),
        f"hypothesis_packets_v1.packets[{packet_index}].packet_id",
    )
    if referenced_packet_id != packet_artifact.packet_id:
        raise ValueError("packet_artifact.packet_id must match the packet_pointer target")

    frozen_packet_payload = _require_mapping(
        payload.get("frozen_packet_payload"),
        "frozen_packet_payload",
    )
    if dict(referenced_packet) != dict(frozen_packet_payload):
        raise ValueError(
            "frozen_packet_payload must exactly match the referenced reviewed packet payload"
        )
    frozen_packet_payload_sha256 = _require_sha256(
        payload.get("frozen_packet_payload_sha256"),
        "frozen_packet_payload_sha256",
    )
    if frozen_packet_payload_sha256 != _canonical_json_sha256(frozen_packet_payload):
        raise ValueError("frozen_packet_payload_sha256 must hash the exact frozen packet payload")

    packet_scope = _validate_packet_scope(
        _require_mapping(payload.get("packet_scope"), "packet_scope"),
        frozen_packet_payload=frozen_packet_payload,
    )
    frozen_forecast_payload = _validate_frozen_forecast_payload(
        _require_mapping(
            payload.get("frozen_forecast_payload"),
            "frozen_forecast_payload",
        ),
        frozen_packet_payload=frozen_packet_payload,
    )
    frozen_forecast_payload_sha256 = _require_sha256(
        payload.get("frozen_forecast_payload_sha256"),
        "frozen_forecast_payload_sha256",
    )
    if frozen_forecast_payload_sha256 != _canonical_json_sha256(
        frozen_forecast_payload
    ):
        raise ValueError(
            "frozen_forecast_payload_sha256 must hash the exact frozen forecast payload"
        )

    return ProspectivePredictionRegistration(
        schema_name=schema_name,
        schema_version=schema_version,
        registration_id=_require_text(payload.get("registration_id"), "registration_id"),
        registered_at=_require_iso_datetime(payload.get("registered_at"), "registered_at"),
        registered_by=_require_text(payload.get("registered_by"), "registered_by"),
        packet_artifact=packet_artifact,
        packet_scope=packet_scope,
        frozen_packet_payload=dict(frozen_packet_payload),
        frozen_packet_payload_sha256=frozen_packet_payload_sha256,
        frozen_forecast_payload=frozen_forecast_payload,
        frozen_forecast_payload_sha256=frozen_forecast_payload_sha256,
        notes=str(payload.get("notes", "")).strip(),
    )


def read_prospective_prediction_registration(
    path: Path,
) -> ProspectivePredictionRegistration:
    return validate_prospective_prediction_registration_payload(
        _require_mapping(
            read_json(path.resolve()),
            PROSPECTIVE_PREDICTION_REGISTRATION_ARTIFACT_NAME,
        ),
        artifact_path=path.resolve(),
    )


def validate_prospective_forecast_outcome_log_payload(
    payload: dict[str, object],
    *,
    artifact_path: Path,
) -> ProspectiveForecastOutcomeLog:
    outcome_records_payload = _require_list(payload.get("outcome_records"), "outcome_records")
    seen_record_ids: set[str] = set()
    records: list[ProspectiveForecastOutcomeRecord] = []

    for index, item in enumerate(outcome_records_payload):
        record_payload = _require_mapping(item, f"outcome_records[{index}]")
        registration_artifact_path = _require_text(
            record_payload.get("registration_artifact_path"),
            f"outcome_records[{index}].registration_artifact_path",
        )
        registration_path = _resolve_path(
            registration_artifact_path,
            base_dir=artifact_path.resolve().parent,
        )
        if not registration_path.is_file():
            raise ValueError(
                f"outcome_records[{index}].registration_artifact_path points to a missing file"
            )
        registration_artifact_sha256 = _require_sha256(
            record_payload.get("registration_artifact_sha256"),
            f"outcome_records[{index}].registration_artifact_sha256",
        )
        if _compute_sha256(registration_path) != registration_artifact_sha256:
            raise ValueError(
                f"outcome_records[{index}].registration_artifact_sha256 does not match the referenced registration"
            )
        registration = read_prospective_prediction_registration(registration_path)
        registration_id = _require_text(
            record_payload.get("registration_id"),
            f"outcome_records[{index}].registration_id",
        )
        if registration.registration_id != registration_id:
            raise ValueError(
                f"outcome_records[{index}].registration_id must match the referenced registration artifact"
            )
        observed_outcome = _require_text(
            record_payload.get("observed_outcome"),
            f"outcome_records[{index}].observed_outcome",
        )
        allowed_outcomes = tuple(
            str(option)
            for option in registration.frozen_forecast_payload["outcome_options"]
        )
        if observed_outcome not in allowed_outcomes:
            raise ValueError(
                f"outcome_records[{index}].observed_outcome must match one of the registered outcome options"
            )

        source_evidence_payload = _require_mapping(
            record_payload.get("source_evidence"),
            f"outcome_records[{index}].source_evidence",
        )
        source_path_text = _require_text(
            source_evidence_payload.get("source_path"),
            f"outcome_records[{index}].source_evidence.source_path",
        )
        source_path = _resolve_path(
            source_path_text,
            base_dir=artifact_path.resolve().parent,
        )
        if not source_path.is_file():
            raise ValueError(
                f"outcome_records[{index}].source_evidence.source_path points to a missing file"
            )
        source_sha256 = _require_sha256(
            source_evidence_payload.get("source_sha256"),
            f"outcome_records[{index}].source_evidence.source_sha256",
        )
        if _compute_sha256(source_path) != source_sha256:
            raise ValueError(
                f"outcome_records[{index}].source_evidence.source_sha256 does not match the referenced source file"
            )

        record = ProspectiveForecastOutcomeRecord(
            outcome_record_id=_require_text(
                record_payload.get("outcome_record_id"),
                f"outcome_records[{index}].outcome_record_id",
            ),
            registration_artifact_path=registration_artifact_path,
            registration_artifact_sha256=registration_artifact_sha256,
            registration_id=registration_id,
            observed_at=_require_iso_date(
                record_payload.get("observed_at"),
                f"outcome_records[{index}].observed_at",
            ),
            observed_outcome=observed_outcome,
            source_evidence=OutcomeEvidenceReference(
                source_path=source_path_text,
                source_sha256=source_sha256,
                source_pointer=str(source_evidence_payload.get("source_pointer", "")).strip(),
                summary=_require_text(
                    source_evidence_payload.get("summary"),
                    f"outcome_records[{index}].source_evidence.summary",
                ),
            ),
            notes=str(record_payload.get("notes", "")).strip(),
        )
        if record.outcome_record_id in seen_record_ids:
            raise ValueError("outcome_records must not repeat outcome_record_id")
        seen_record_ids.add(record.outcome_record_id)
        records.append(record)

    return ProspectiveForecastOutcomeLog(
        schema_name=_require_text(payload.get("schema_name"), "schema_name"),
        schema_version=_require_text(payload.get("schema_version"), "schema_version"),
        outcome_log_id=_require_text(payload.get("outcome_log_id"), "outcome_log_id"),
        logged_at=_require_iso_datetime(payload.get("logged_at"), "logged_at"),
        logged_by=_require_text(payload.get("logged_by"), "logged_by"),
        outcome_records=tuple(records),
        notes=str(payload.get("notes", "")).strip(),
    )


def read_prospective_forecast_outcome_log(
    path: Path,
) -> ProspectiveForecastOutcomeLog:
    return validate_prospective_forecast_outcome_log_payload(
        _require_mapping(
            read_json(path.resolve()),
            PROSPECTIVE_FORECAST_OUTCOME_LOG_ARTIFACT_NAME,
        ),
        artifact_path=path.resolve(),
    )


def _load_json_artifacts(
    directory: Path,
    *,
    reader: Any,
) -> tuple[object, ...]:
    resolved_dir = directory.resolve()
    if not resolved_dir.exists():
        return ()
    return tuple(reader(path) for path in _iter_json_files(resolved_dir))


def load_prospective_prediction_registrations(
    registrations_dir: Path | None = None,
) -> tuple[ProspectivePredictionRegistration, ...]:
    directory = (
        DEFAULT_PROSPECTIVE_REGISTRATIONS_DIR
        if registrations_dir is None
        else registrations_dir.resolve()
    )
    registrations = tuple(
        _load_json_artifacts(directory, reader=read_prospective_prediction_registration)
    )
    registration_ids = [registration.registration_id for registration in registrations]
    if len(registration_ids) != len(set(registration_ids)):
        raise ValueError("prospective registrations must not repeat registration_id")
    return registrations


def load_prospective_forecast_outcome_logs(
    outcomes_dir: Path | None = None,
) -> tuple[ProspectiveForecastOutcomeLog, ...]:
    directory = (
        DEFAULT_PROSPECTIVE_OUTCOMES_DIR
        if outcomes_dir is None
        else outcomes_dir.resolve()
    )
    logs = tuple(
        _load_json_artifacts(directory, reader=read_prospective_forecast_outcome_log)
    )
    outcome_log_ids = [log.outcome_log_id for log in logs]
    if len(outcome_log_ids) != len(set(outcome_log_ids)):
        raise ValueError("prospective outcome logs must not repeat outcome_log_id")
    outcome_record_ids = [
        record.outcome_record_id
        for log in logs
        for record in log.outcome_records
    ]
    if len(outcome_record_ids) != len(set(outcome_record_ids)):
        raise ValueError(
            "prospective outcome logs must not repeat outcome_record_id across files"
        )
    return logs


def reconcile_prospective_forecasts(
    *,
    registrations_dir: Path | None = None,
    outcomes_dir: Path | None = None,
) -> tuple[ProspectiveForecastReconciliation, ...]:
    registrations = load_prospective_prediction_registrations(
        registrations_dir=registrations_dir
    )
    outcome_logs = load_prospective_forecast_outcome_logs(outcomes_dir=outcomes_dir)
    registration_by_id = {
        registration.registration_id: registration for registration in registrations
    }

    grouped_records: dict[str, list[ProspectiveForecastOutcomeRecord]] = {}
    for log in outcome_logs:
        for record in log.outcome_records:
            if record.registration_id not in registration_by_id:
                raise ValueError(
                    "prospective outcome logs reference an unknown registration_id: "
                    f"{record.registration_id}"
                )
            grouped_records.setdefault(record.registration_id, []).append(record)

    reconciliations: list[ProspectiveForecastReconciliation] = []
    for registration in registrations:
        forecast_payload = registration.frozen_forecast_payload
        option_probabilities = {
            _require_text(option, "option"): _require_float(probability, "probability")
            for option, probability in _require_mapping(
                forecast_payload.get("option_probabilities"),
                "registration.frozen_forecast_payload.option_probabilities",
            ).items()
        }
        records = grouped_records.get(registration.registration_id, [])
        if not records:
            reconciliations.append(
                ProspectiveForecastReconciliation(
                    registration_id=registration.registration_id,
                    packet_id=registration.packet_artifact.packet_id,
                    resolution_status="pending",
                    predicted_outcome=_require_text(
                        forecast_payload.get("predicted_outcome"),
                        "registration.frozen_forecast_payload.predicted_outcome",
                    ),
                    option_probabilities=option_probabilities,
                )
            )
            continue
        sorted_records = _sort_outcome_records(records)
        in_window_records = tuple(
            record
            for record in sorted_records
            if _outcome_record_is_within_window(
                record,
                registration=registration,
            )
        )
        valid_outcomes = tuple(
            sorted({record.observed_outcome for record in in_window_records})
        )

        if not in_window_records:
            reconciliations.append(
                ProspectiveForecastReconciliation(
                    registration_id=registration.registration_id,
                    packet_id=registration.packet_artifact.packet_id,
                    resolution_status="out_of_window",
                    predicted_outcome=_require_text(
                        forecast_payload.get("predicted_outcome"),
                        "registration.frozen_forecast_payload.predicted_outcome",
                    ),
                    option_probabilities=option_probabilities,
                    observed_outcome=sorted_records[0].observed_outcome,
                    observed_at=sorted_records[0].observed_at,
                    outcome_record_ids=tuple(
                        record.outcome_record_id for record in sorted_records
                    ),
                )
            )
            continue

        if len(valid_outcomes) == 1:
            reconciliations.append(
                ProspectiveForecastReconciliation(
                    registration_id=registration.registration_id,
                    packet_id=registration.packet_artifact.packet_id,
                    resolution_status="resolved",
                    predicted_outcome=_require_text(
                        forecast_payload.get("predicted_outcome"),
                        "registration.frozen_forecast_payload.predicted_outcome",
                    ),
                    option_probabilities=option_probabilities,
                    observed_outcome=valid_outcomes[0],
                    observed_at=in_window_records[0].observed_at,
                    outcome_record_ids=tuple(
                        record.outcome_record_id for record in in_window_records
                    ),
                )
            )
            continue

        reconciliations.append(
            ProspectiveForecastReconciliation(
                registration_id=registration.registration_id,
                packet_id=registration.packet_artifact.packet_id,
                resolution_status="conflicted",
                predicted_outcome=_require_text(
                    forecast_payload.get("predicted_outcome"),
                    "registration.frozen_forecast_payload.predicted_outcome",
                ),
                option_probabilities=option_probabilities,
                outcome_record_ids=tuple(
                    record.outcome_record_id for record in in_window_records
                ),
                conflict_outcomes=valid_outcomes,
            )
        )
    return tuple(reconciliations)


def build_prospective_scoring_records(
    *,
    registrations_dir: Path | None = None,
    outcomes_dir: Path | None = None,
) -> tuple[ProspectiveForecastScoringRecord, ...]:
    scoring_records: list[ProspectiveForecastScoringRecord] = []
    for reconciliation in reconcile_prospective_forecasts(
        registrations_dir=registrations_dir,
        outcomes_dir=outcomes_dir,
    ):
        if reconciliation.resolution_status != "resolved":
            continue
        scoring_records.append(
            ProspectiveForecastScoringRecord(
                registration_id=reconciliation.registration_id,
                packet_id=reconciliation.packet_id,
                predicted_outcome=reconciliation.predicted_outcome,
                observed_outcome=reconciliation.observed_outcome,
                option_probabilities=dict(reconciliation.option_probabilities),
                observed_at=reconciliation.observed_at,
            )
        )
    return tuple(scoring_records)


__all__ = [
    "DEFAULT_PROSPECTIVE_OUTCOMES_DIR",
    "DEFAULT_PROSPECTIVE_REGISTRATIONS_DIR",
    "DEFAULT_PROSPECTIVE_REGISTRY_DIR",
    "PROSPECTIVE_FORECAST_OUTCOME_LOG_ARTIFACT_NAME",
    "PROSPECTIVE_PREDICTION_REGISTRATION_ARTIFACT_NAME",
    "PROSPECTIVE_SCHEMA_VERSION",
    "ProspectiveForecastOutcomeLog",
    "ProspectiveForecastOutcomeRecord",
    "ProspectiveForecastReconciliation",
    "ProspectiveForecastScoringRecord",
    "ProspectivePredictionRegistration",
    "build_prospective_prediction_registration_payload",
    "build_prospective_scoring_records",
    "load_prospective_forecast_outcome_logs",
    "load_prospective_prediction_registrations",
    "materialize_prospective_prediction_registration",
    "read_prospective_forecast_outcome_log",
    "read_prospective_prediction_registration",
    "reconcile_prospective_forecasts",
    "validate_prospective_forecast_outcome_log_payload",
    "validate_prospective_prediction_registration_payload",
]
