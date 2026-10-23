"""Name -> step function registry.

A config references a step by its string name (e.g. "pivot_long_to_wide");
this is what resolves that name back to the actual implementation in
structured.py (or, later, unstructured.py). Keeping this separate from
structured.py avoids a circular import between the step implementations and
whatever needs to look one up.
"""

from __future__ import annotations

from typing import Callable

STEP_REGISTRY: dict[str, Callable] = {}


def step(name: str) -> Callable:
    """Decorator: register a step function under `name`."""

    def decorator(fn: Callable) -> Callable:
        if name in STEP_REGISTRY:
            raise ValueError(f"Step {name!r} is already registered (by {STEP_REGISTRY[name].__module__})")
        STEP_REGISTRY[name] = fn
        return fn

    return decorator


def get_step(name: str) -> Callable:
    try:
        return STEP_REGISTRY[name]
    except KeyError:
        raise KeyError(f"Unknown pipeline step {name!r}. Available: {sorted(STEP_REGISTRY)}") from None
