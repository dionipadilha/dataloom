# dataloom_engine/exceptions.py

"""
Custom exceptions for DataLoom.
Lets consumers catch library-specific errors without relying on
generic Python exceptions.
"""

from typing import Any, Optional


class LoomError(Exception):
    """Base exception for every DataLoom error."""

    pass


class ConfigurationError(LoomError):
    """Raised when LoomConfig validation fails."""

    pass


class WeaverError(LoomError):
    """
    Raised when a Weaver fails to handle a batch.

    Carries the failure context so hooks.on_error can implement retry,
    quarantine or dead-letter logic without parsing error strings:

    Attributes:
        batch: The batch that failed, exactly as yielded by the Source.
        stage: Where the failure happened: "process" (Processor.process)
            or "send" (Sink.send). None when the error was constructed
            without context.
    """

    def __init__(self, message: str, batch: Any = None, stage: Optional[str] = None):
        super().__init__(message)
        self.batch = batch
        self.stage = stage
