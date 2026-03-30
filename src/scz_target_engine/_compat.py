"""Helpers for bridging legacy flat modules into namespaced packages."""

from __future__ import annotations

from importlib import import_module
from typing import Any


def reexport_module(namespace: dict[str, Any], legacy_module_name: str) -> None:
    """Populate a wrapper module namespace from a legacy module."""

    legacy_module = import_module(legacy_module_name)
    exported_names = [
        name
        for name in dir(legacy_module)
        if not name.startswith("__")
    ]
    namespace["__all__"] = exported_names
    for name in exported_names:
        namespace[name] = getattr(legacy_module, name)
