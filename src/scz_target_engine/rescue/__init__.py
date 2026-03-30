"""Rescue task registry and contract surfaces."""

from scz_target_engine.rescue.contracts import (
    RESCUE_TASK_CONTRACT_ARTIFACT_NAME,
    RescueArtifactContract,
    RescueLeakageBoundary,
    RescueTaskContract,
    read_rescue_task_contract,
)
from scz_target_engine.rescue.governance import (
    RESCUE_DATASET_CARD_ARTIFACT_NAME,
    RESCUE_FREEZE_MANIFEST_ARTIFACT_NAME,
    RESCUE_RAW_TO_FROZEN_LINEAGE_ARTIFACT_NAME,
    RESCUE_SPLIT_MANIFEST_ARTIFACT_NAME,
    RESCUE_TASK_CARD_ARTIFACT_NAME,
    RescueDatasetCard,
    RescueFreezeManifest,
    RescueGovernanceBundle,
    RescueRawToFrozenLineage,
    RescueSplitManifest,
    RescueTaskCard,
    read_rescue_dataset_card,
    read_rescue_freeze_manifest,
    read_rescue_raw_to_frozen_lineage,
    read_rescue_split_manifest,
    read_rescue_task_card,
    validate_rescue_governance_bundle,
)

__all__ = [
    "DEFAULT_RESCUE_TASK_REGISTRY_PATH",
    "RESCUE_DATASET_CARD_ARTIFACT_NAME",
    "RESCUE_FREEZE_MANIFEST_ARTIFACT_NAME",
    "RESCUE_RAW_TO_FROZEN_LINEAGE_ARTIFACT_NAME",
    "RESCUE_SPLIT_MANIFEST_ARTIFACT_NAME",
    "RESCUE_TASK_CONTRACT_ARTIFACT_NAME",
    "RESCUE_TASK_CARD_ARTIFACT_NAME",
    "RescueArtifactContract",
    "RescueDatasetCard",
    "RescueFreezeManifest",
    "RescueGovernanceBundle",
    "RescueLeakageBoundary",
    "RescueRawToFrozenLineage",
    "RescueSplitManifest",
    "RescueSuiteContract",
    "RescueTaskContract",
    "RescueTaskCard",
    "RescueTaskRegistration",
    "read_rescue_dataset_card",
    "read_rescue_freeze_manifest",
    "read_rescue_raw_to_frozen_lineage",
    "read_rescue_split_manifest",
    "load_rescue_suite_contracts",
    "load_rescue_task_contracts",
    "load_rescue_task_registrations",
    "read_rescue_task_contract",
    "read_rescue_task_card",
    "resolve_rescue_suite_contract",
    "resolve_rescue_task_contract",
    "validate_rescue_governance_bundle",
]


def __getattr__(name: str) -> object:
    if name in {
        "DEFAULT_RESCUE_TASK_REGISTRY_PATH",
        "RescueSuiteContract",
        "RescueTaskRegistration",
        "load_rescue_suite_contracts",
        "load_rescue_task_contracts",
        "load_rescue_task_registrations",
        "resolve_rescue_suite_contract",
        "resolve_rescue_task_contract",
    }:
        from scz_target_engine.rescue.registry import (
            DEFAULT_RESCUE_TASK_REGISTRY_PATH,
            RescueSuiteContract,
            RescueTaskRegistration,
            load_rescue_suite_contracts,
            load_rescue_task_contracts,
            load_rescue_task_registrations,
            resolve_rescue_suite_contract,
            resolve_rescue_task_contract,
        )

        return {
            "DEFAULT_RESCUE_TASK_REGISTRY_PATH": DEFAULT_RESCUE_TASK_REGISTRY_PATH,
            "RescueSuiteContract": RescueSuiteContract,
            "RescueTaskRegistration": RescueTaskRegistration,
            "load_rescue_suite_contracts": load_rescue_suite_contracts,
            "load_rescue_task_contracts": load_rescue_task_contracts,
            "load_rescue_task_registrations": load_rescue_task_registrations,
            "resolve_rescue_suite_contract": resolve_rescue_suite_contract,
            "resolve_rescue_task_contract": resolve_rescue_task_contract,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
