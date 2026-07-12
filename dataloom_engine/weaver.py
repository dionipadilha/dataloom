# dataloom_engine/weaver.py

"""
The worker definition (Weaver).
Dedicated thread that consumes tasks from the queue, processes them and
sends the results to the Sink. This module is internal and should not
be imported directly by users.
"""

import logging
import queue
import threading
import time
from typing import Any, Callable, Dict, Optional

from dataloom_engine.exceptions import WeaverError
from dataloom_engine.processors import Processor
from dataloom_engine.sinks import Sink

logger = logging.getLogger(__name__)

# Internal sentinel: instructs the Weaver to exit. The Loom enqueues one
# sentinel per Weaver after the data, guaranteeing the queue is fully
# drained before the threads die.
STOP_SENTINEL: Any = object()


class Weaver(threading.Thread):
    """
    Execution agent that runs in a separate thread.
    Orchestrates the flow: Queue -> Processor -> Sink.

    Processing errors do not kill the thread: they are wrapped in
    WeaverError, logged and reported via the on_error callback (if any).
    """

    def __init__(
        self,
        task_queue: queue.Queue,
        processor: Processor,
        sink: Sink,
        on_error: Optional[Callable[[Exception], None]] = None,
        on_batch_processed: Optional[Callable[[Dict[str, Any], float], None]] = None,
    ):
        super().__init__(daemon=True)
        self.task_queue = task_queue
        self.processor = processor
        self.sink = sink
        self.on_error = on_error
        self.on_batch_processed = on_batch_processed

    def run(self) -> None:
        while True:
            batch = self.task_queue.get()
            try:
                if batch is STOP_SENTINEL:
                    return
                self._process_batch(batch)
            finally:
                # Marks the queue item as handled (success or failure)
                self.task_queue.task_done()

    def _process_batch(self, batch: Any) -> None:
        try:
            started = time.monotonic()
            result = self.processor.process(batch)
            self.sink.send(result)
            duration = time.monotonic() - started
        except Exception as exc:
            logger.exception("Weaver failed to process a batch; the batch was dropped.")
            if self.on_error is not None:
                error = WeaverError(f"Failed to process batch: {exc}")
                error.__cause__ = exc
                try:
                    self.on_error(error)
                except Exception:
                    logger.exception("The on_error callback raised an exception.")
            return

        # The metric is only emitted on success; failures go through on_error
        if self.on_batch_processed is not None:
            try:
                self.on_batch_processed(result, duration)
            except Exception:
                logger.exception("The on_batch_processed callback raised an exception.")
