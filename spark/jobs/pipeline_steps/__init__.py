"""Reusable, named Bronze->Silver transformation steps.

Python owns control flow (the fixed per-pipeline_type skeleton in
silver_transform.py); this package owns the named, parametrized building
blocks that skeleton calls into, and that a dataset's config can reference by
name for its own dataset-specific "extra_steps". Importing this package
registers every step in `registry.STEP_REGISTRY` as a side effect.
"""

from __future__ import annotations

from . import structured  # noqa: F401  (populates STEP_REGISTRY on import)
from .context import PipelineContext
from .registry import STEP_REGISTRY, get_step, step

__all__ = ["PipelineContext", "STEP_REGISTRY", "get_step", "step"]
