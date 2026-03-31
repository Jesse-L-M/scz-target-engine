from __future__ import annotations

from scz_target_engine.rescue.models.base import (
    RescueModelDefinition,
    RescueModelPlugin,
)
from scz_target_engine.rescue.models.npc import (
    NPC_SIGNATURE_REVERSAL_MODEL_PLUGINS,
    NPC_SIGNATURE_REVERSAL_TASK_ID,
)


_PLUGIN_INDEX: dict[str, tuple[RescueModelPlugin, ...]] = {
    NPC_SIGNATURE_REVERSAL_TASK_ID: NPC_SIGNATURE_REVERSAL_MODEL_PLUGINS,
}


def list_rescue_model_plugins(
    task_id: str,
    *,
    include_candidates: bool = False,
) -> tuple[RescueModelPlugin, ...]:
    plugins = _PLUGIN_INDEX.get(task_id, ())
    if include_candidates:
        return plugins
    return tuple(
        plugin
        for plugin in plugins
        if plugin.definition.stage == "shipped"
    )


def list_rescue_model_definitions(
    task_id: str,
    *,
    include_candidates: bool = False,
) -> tuple[RescueModelDefinition, ...]:
    return tuple(
        plugin.definition
        for plugin in list_rescue_model_plugins(
            task_id,
            include_candidates=include_candidates,
        )
    )


def resolve_rescue_model_plugin(
    task_id: str,
    model_id: str,
    *,
    include_candidates: bool = False,
) -> RescueModelPlugin:
    for plugin in list_rescue_model_plugins(
        task_id,
        include_candidates=include_candidates,
    ):
        if plugin.definition.model_id == model_id:
            return plugin
    valid_ids = ", ".join(
        plugin.definition.model_id
        for plugin in list_rescue_model_plugins(
            task_id,
            include_candidates=include_candidates,
        )
    )
    raise KeyError(
        f"unknown rescue model plugin for {task_id}: {model_id}; "
        f"expected one of {valid_ids}"
    )


__all__ = [
    "list_rescue_model_definitions",
    "list_rescue_model_plugins",
    "resolve_rescue_model_plugin",
]
