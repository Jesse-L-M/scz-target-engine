"""Namespaced wrapper for legacy benchmark snapshot builders."""

from scz_target_engine._compat import reexport_module

reexport_module(globals(), "scz_target_engine.benchmark_snapshots")
