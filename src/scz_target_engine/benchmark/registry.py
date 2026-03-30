"""Namespaced wrapper for the legacy benchmark task registry module."""

from scz_target_engine._compat import reexport_module

reexport_module(globals(), "scz_target_engine.benchmark_registry")
