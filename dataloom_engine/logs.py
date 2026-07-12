# dataloom_engine/logs.py

"""
Logging configuration utilities for DataLoom.
Provides a static namespace for quick setup.
"""

import logging


class LoomLogs:
    """
    Utility namespace for DataLoom log management.
    Not meant to be instantiated — use it statically.
    """

    @staticmethod
    def setup(level: int = logging.INFO) -> None:
        """
        Configures basic logging to stdout.

        Usage:
            LoomLogs.setup(level=logging.DEBUG)
        """
        logging.basicConfig(
            level=level,
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        )
