# dataloom_engine/loom.py

"""
The Loom module is the heart of the orchestration engine.
Defines the main class responsible for managing the lifecycle of the
worker threads (Weavers) and the distribution of tasks.
"""

import queue
import threading
from typing import TYPE_CHECKING, Optional

from dataloom_engine.config import LoomConfig
from dataloom_engine.hooks import LoomHooks
from dataloom_engine.processors import Processor
from dataloom_engine.sinks import Sink
from dataloom_engine.types import LoomState
from dataloom_engine.weaver import STOP_SENTINEL, Weaver

if TYPE_CHECKING:
    from dataloom_engine.sources import Source


class Loom:
    """
    DataLoom's main orchestrator.

    Usage example:
        config = LoomConfig(...)
        loom = Loom(config, processor, sink)
        loom.start()

    It can also be used as a context manager, guaranteeing stop()
    even if the block raises or is interrupted:
        with Loom(config, processor, sink) as loom:
            loom.start()
    """

    def __init__(
        self,
        config: LoomConfig,
        processor: Processor,
        sink: Sink,
        source: Optional["Source"] = None,
        hooks: Optional[LoomHooks] = None,
        num_weavers: int = 2,
    ):
        self.config = config
        self.processor = processor
        self.sink = sink

        # Default dependency injection if not provided
        if source is None:
            # Late import to avoid a circular dependency at module import time
            from dataloom_engine.sources import RandomNumPySource

            source = RandomNumPySource(config)
        self.source: "Source" = source

        # Fresh hooks instance when not provided (avoids global state)
        self.hooks = hooks or LoomHooks()
        self.num_weavers = num_weavers

        self.state = LoomState.PENDING

        # Bounded queue (backpressure): if the Weavers can't keep up with
        # the Source, the producer waits instead of growing memory.
        # queue_maxsize=0 in the config disables the limit.
        if config.queue_maxsize is not None:
            maxsize = config.queue_maxsize
        else:
            maxsize = num_weavers * 4
        self.task_queue: queue.Queue = queue.Queue(maxsize=maxsize)

        self.stop_event = threading.Event()
        self.weavers: list[Weaver] = []
        self._stop_lock = threading.Lock()
        self._stopped = False

    def __enter__(self) -> "Loom":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        # Guarantees full cleanup when leaving the with block, including on
        # exceptions (such as KeyboardInterrupt) that escape start().
        # stop() is idempotent, so this is free if it already ran.
        self.stop()

    def start(self) -> None:
        """
        Starts the Weavers and begins the task production loop.
        This method blocks until an error occurs or the loom is stopped.
        """
        self.state = LoomState.RUNNING
        self.hooks.on_start()

        # Wake up the weavers
        for _ in range(self.num_weavers):
            weaver = Weaver(
                self.task_queue,
                self.processor,
                self.sink,
                on_error=self.hooks.on_error,
                on_batch_processed=self.hooks.on_batch_processed,
            )
            weaver.start()
            self.weavers.append(weaver)

        try:
            # Consume from the Source
            for batch in self.source:
                if self.stop_event.is_set():
                    break
                self._enqueue(batch)
        except Exception as e:
            self.state = LoomState.FAILED
            self.hooks.on_error(e)
            raise
        finally:
            self.stop()

    def _enqueue(self, batch) -> None:
        """
        Puts a batch on the queue without blocking indefinitely: the
        timeout lets us react to a stop() triggered from another thread.
        """
        while not self.stop_event.is_set():
            try:
                self.task_queue.put(batch, timeout=0.1)
                return
            except queue.Full:
                continue

    def stop(self) -> None:
        """
        Signals every component to stop and waits for cleanup.
        Safe to call multiple times or from finally blocks.

        Items already queued are processed before shutdown: each Weaver
        drains the queue until it finds its stop sentinel.
        """
        with self._stop_lock:
            if self._stopped:
                return
            self._stopped = True

        self.stop_event.set()

        # One sentinel per Weaver: each thread drains the queue and exits
        # upon consuming its own. This replaces the queue join(), which
        # could block forever if a Weaver died before emptying it.
        for _ in self.weavers:
            self.task_queue.put(STOP_SENTINEL)
        for weaver in self.weavers:
            weaver.join()

        # Never overwrite a FAILED state set by start() on error
        if self.state is LoomState.RUNNING:
            self.state = LoomState.COMPLETED

        try:
            self.sink.close()
        except Exception as e:
            # An error in close() must not mask other errors, but we report it
            self.hooks.on_error(e)

        self.hooks.on_stop()
