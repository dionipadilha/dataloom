# dataloom_engine/hooks.py

"""
Observability extension points (Hooks).
Lets you inject monitoring logic without coupling it to the engine core.
"""

from typing import Any, Dict


class LoomHooks:
    """
    Base class for lifecycle hooks.
    Every method is optional and a no-op by default: override only the
    points you care about. (Deliberately not an ABC — there is no method
    you are required to implement.)
    """

    def on_start(self) -> None:
        """Called right before the Weavers are started."""
        pass

    def on_stop(self) -> None:
        """Called after the orchestrator shuts down gracefully."""
        pass

    def on_error(self, error: Exception) -> None:
        """
        Called when an exception occurs in the Loom's main loop or while
        processing a batch inside a Weaver (WeaverError).

        Note: it may be invoked from multiple Weaver threads at the same
        time — implementations must be thread-safe.
        """
        pass

    def on_batch_processed(self, result: Dict[str, Any], duration_seconds: float) -> None:
        """
        Called after each batch is processed and delivered to the Sink.

        Args:
            result: The dictionary produced by the Processor for the batch.
            duration_seconds: Time spent in process() + send(), measured
                with a monotonic clock.

        Note: like on_error, this is invoked from the Weaver threads —
        implementations must be thread-safe and fast, since they run on
        the pipeline's hot path.
        """
        pass
