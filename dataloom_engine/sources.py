# dataloom_engine/sources.py

"""
Data sources module.
Defines the interface for generating or fetching data batches to be
processed by the Loom.
"""

import time
from abc import ABC, abstractmethod
from typing import Any, Iterator

from dataloom_engine._optional import require_numpy
from dataloom_engine.config import LoomConfig


class Source(ABC):
    """
    Abstract base class for data sources.
    A Source must be iterable, yielding batches of data.
    """

    @abstractmethod
    def __iter__(self) -> Iterator[Any]:
        """
        Yields data batches.
        """
        pass


class RandomNumPySource(Source):
    """
    Standard source that generates random NumPy arrays.
    Simulates a continuous data stream.

    Requires the optional numpy dependency:
        pip install "dataloom-engine[numpy]"
    """

    def __init__(self, config: LoomConfig, limit: int = -1):
        """
        Args:
            config: Configuration object containing batch_size and interval_seconds.
            limit: Maximum number of batches to generate. -1 for infinite.
        """
        # Fail fast at construction if the optional dependency is missing
        self._np = require_numpy("RandomNumPySource")
        self.config = config
        self.limit = limit

    def __iter__(self) -> Iterator[Any]:
        count = 0
        while self.limit == -1 or count < self.limit:
            # Simulate generic production delay
            time.sleep(self.config.interval_seconds)

            # Generate batch
            batch = self._np.random.rand(self.config.batch_size)
            yield batch

            count += 1
