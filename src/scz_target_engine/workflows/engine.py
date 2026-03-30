"""Namespaced wrapper for the legacy engine workflow."""

from scz_target_engine._compat import reexport_module

reexport_module(globals(), "scz_target_engine.engine")
