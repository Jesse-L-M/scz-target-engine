"""Namespaced wrapper for legacy file IO helpers."""

from scz_target_engine._compat import reexport_module

reexport_module(globals(), "scz_target_engine.io")
