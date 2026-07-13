# dataloom_engine/loom.py

"""
The Loom module is the heart of the orchestration engine.
Defines the main class responsible for managing the lifecycle of the
worker threads (Weavers) and the distribution of tasks.
"""

import logging
import queue
import threading
import time
from typing import TYPE_CHECKING, Optional

from dataloom_engine.config import LoomConfig
from dataloom_engine.exceptions import ConfigurationError, LoomError
from dataloom_engine.hooks import LoomHooks
from dataloom_engine.processors import Processor
from dataloom_engine.sinks import Sink
from dataloom_engine.types import LoomState
from dataloom_engine.weaver import STOP_SENTINEL, Weaver

if TYPE_CHECKING:
    from dataloom_engine.sources import Source

logger = logging.getLogger(__name__)


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
        if num_weavers < 1:
            raise ConfigurationError(f"num_weavers must be at least 1 (got: {num_weavers}).")

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
        # Distinguishes COMPLETED (source exhausted) from STOPPED (interrupted)
        self._source_exhausted = False

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

        A Loom instance is single-use: calling start() again after it has
        run (or after stop()) raises LoomError.
        """
        # Restarting a stopped instance would spawn Weavers that never
        # receive a stop sentinel (stop() is idempotent), leaking threads.
        if self.state is not LoomState.PENDING or self._stopped:
            raise LoomError(
                "This Loom has already been started or stopped; "
                "create a new instance to run another pipeline."
            )

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
            else:
                # The for/else only runs when the loop ended without a
                # break: the source was exhausted naturally, so stop()
                # may report COMPLETED instead of STOPPED.
                self._source_exhausted = True
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

    def stop(self, timeout: Optional[float] = None) -> None:
        """
        Signals every component to stop and waits for cleanup.
        Safe to call multiple times or from finally blocks.

        Items already queued are processed before shutdown: each Weaver
        drains the queue until it finds its stop sentinel.

        Args:
            timeout: Maximum time in seconds to wait for the Weavers to
                finish. None (default) waits indefinitely. When the
                deadline passes, still-running Weavers are reported via
                hooks.on_error (as LoomError), the sink is closed anyway
                and stop() returns; the leftover daemon threads do not
                block process exit.
        """
        with self._stop_lock:
            if self._stopped:
                return
            self._stopped = True

        self.stop_event.set()

        deadline = None if timeout is None else time.monotonic() + timeout

        # One sentinel per Weaver: each thread drains the queue and exits
        # upon consuming its own. This replaces the queue join(), which
        # could block forever if a Weaver died before emptying it.
        for _ in self.weavers:
            if not self._put_sentinel(deadline):
                break

        stuck = []
        for weaver in self.weavers:
            if deadline is None:
                weaver.join()
            else:
                weaver.join(timeout=max(deadline - time.monotonic(), 0))
            if weaver.is_alive():
                stuck.append(weaver)

        if stuck:
            logger.warning(
                "%d weaver(s) still running after the stop timeout; they are "
                "daemon threads and will not block process exit.",
                len(stuck),
            )
            try:
                self.hooks.on_error(
                    LoomError(f"{len(stuck)} weaver(s) did not finish within the stop timeout.")
                )
            except Exception:
                logger.exception("The on_error callback raised an exception.")

        # Natural exhaustion of the source becomes COMPLETED; an external
        # stop or interruption becomes STOPPED. A FAILED state set by
        # start() on a source error is never overwritten.
        if self.state is LoomState.RUNNING:
            self.state = LoomState.COMPLETED if self._source_exhausted else LoomState.STOPPED

        try:
            self.sink.close()
        except Exception as e:
            # An error in close() must not mask other errors, but we report it
            self.hooks.on_error(e)

        self.hooks.on_stop()

    def _put_sentinel(self, deadline: Optional[float]) -> bool:
        """
        Enqueues one stop sentinel, giving up when the deadline passes or
        when no Weaver is alive to drain a full queue — a plain blocking
        put() would hang stop() forever in that scenario.
        """
        while True:
            if deadline is None:
                wait = 0.1
            else:
                wait = min(0.1, max(deadline - time.monotonic(), 0.0))
            try:
                self.task_queue.put(STOP_SENTINEL, timeout=wait)
                return True
            except queue.Full:
                if deadline is not None and time.monotonic() >= deadline:
                    return False
                if not any(weaver.is_alive() for weaver in self.weavers):
                    return False
