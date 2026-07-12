# dataloom_engine/exceptions.py

"""
Custom exceptions for DataLoom.
Lets consumers catch library-specific errors without relying on
generic Python exceptions.
"""


class LoomError(Exception):
    """Base exception for every DataLoom error."""

    pass


class ConfigurationError(LoomError):
    """Raised when LoomConfig validation fails."""

    pass


class WeaverError(LoomError):
    """Raised when a Weaver fails to process a batch."""

    pass
