# dataloom_engine/config.py

"""
Orchestrator configuration management.
Centralizes operational parameters such as directories and batch sizes.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

from dataloom_engine.exceptions import ConfigurationError


@dataclass
class LoomConfig:
    """
    DataLoom's main configuration object.

    Args:
        output_dir (Path): Base directory where the built-in Sinks save data.
            Strings are converted to Path automatically.
        batch_size (int): Number of items produced per processing cycle.
        interval_seconds (float): Time interval between task generations.
        queue_maxsize (Optional[int]): Maximum capacity of the task queue
            (backpressure). None uses the Loom default (num_weavers * 4);
            0 means an unbounded queue.

    Raises:
        ConfigurationError: if any parameter is invalid.
    """

    output_dir: Union[str, Path]
    batch_size: int = 10
    interval_seconds: float = 1.0
    queue_maxsize: Optional[int] = None

    def __post_init__(self) -> None:
        self.output_dir = Path(self.output_dir)

        if self.batch_size <= 0:
            raise ConfigurationError(
                f"batch_size must be greater than zero (got: {self.batch_size})."
            )
        if self.interval_seconds < 0:
            raise ConfigurationError(
                f"interval_seconds cannot be negative (got: {self.interval_seconds})."
            )
        if self.queue_maxsize is not None and self.queue_maxsize < 0:
            raise ConfigurationError(
                f"queue_maxsize cannot be negative (got: {self.queue_maxsize})."
            )
