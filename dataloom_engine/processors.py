# dataloom_engine/processors.py

"""
Data processing contracts.
Defines how batches are transformed before being sent to the Sink.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict

from dataloom_engine._optional import require_numpy


class Processor(ABC):
    """Base interface for data transformation."""

    @abstractmethod
    def process(self, batch: Any) -> Dict[str, Any]:
        """
        Processes a batch of data.

        Args:
            batch: The raw data yielded by the Source. The engine imposes
                no type — built-in sources yield NumPy arrays sized
                according to LoomConfig.

        Returns:
            Dict[str, Any]: Dictionary with the processed results.
        """
        pass


class StatisticsProcessor(Processor):
    """
    Reference implementation that computes basic statistics.
    Useful for tests and initial validation.

    Requires the optional numpy dependency:
        pip install "dataloom-engine[numpy]"
    """

    def __init__(self) -> None:
        # Fail fast at construction if the optional dependency is missing
        self._np = require_numpy("StatisticsProcessor")

    def process(self, batch: Any) -> Dict[str, Any]:
        np = self._np
        return {
            "mean": float(np.mean(batch)),
            "std": float(np.std(batch)),
            "min": float(np.min(batch)),
            "max": float(np.max(batch)),
        }
