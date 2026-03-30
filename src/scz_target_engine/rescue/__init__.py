"""Rescue task registry and contract surfaces."""

from scz_target_engine.rescue.contracts import (
    RESCUE_TASK_CONTRACT_ARTIFACT_NAME,
    RescueArtifactContract,
    RescueLeakageBoundary,
    RescueTaskContract,
    read_rescue_task_contract,
)

__all__ = [
    "DEFAULT_RESCUE_TASK_REGISTRY_PATH",
    "RESCUE_TASK_CONTRACT_ARTIFACT_NAME",
    "RescueArtifactContract",
    "RescueLeakageBoundary",
    "RescueSuiteContract",
    "RescueTaskContract",
    "RescueTaskRegistration",
    "load_rescue_suite_contracts",
    "load_rescue_task_contracts",
    "load_rescue_task_registrations",
    "read_rescue_task_contract",
    "resolve_rescue_suite_contract",
    "resolve_rescue_task_contract",
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
