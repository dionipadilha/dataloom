# dataloom_engine/types.py

"""
Defines the fundamental types and states of the DataLoom system.
These definitions are used across several modules to keep state
handling consistent.
"""

from enum import Enum


class LoomState(Enum):
    """Represents the current lifecycle state of the Loom machine."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
