# dataloom_engine/_optional.py

"""
Helpers for optional dependencies.
The engine core has no third-party requirements; the demo implementations
that need one import it lazily through these helpers, so that a missing
dependency fails fast with an actionable message.
"""

from types import ModuleType


def require_numpy(feature: str) -> ModuleType:
    """
    Returns the numpy module, or raises an informative ImportError if the
    optional dependency is not installed.
    """
    try:
        import numpy
    except ImportError as exc:
        raise ImportError(
            f"{feature} requires numpy, which is an optional dependency of "
            'dataloom-engine. Install it with: pip install "dataloom-engine[numpy]"'
        ) from exc
    return numpy
