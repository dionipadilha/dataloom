# dataloom_engine/sinks.py

"""
Data output contracts (Sinks).
Defines how and where processed results are deposited.
"""

import csv
import json
import logging
import queue
import threading
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from dataloom_engine.exceptions import LoomError

logger = logging.getLogger(__name__)


class Sink(ABC):
    """Base interface for data destinations."""

    @abstractmethod
    def send(self, result: Dict[str, Any]) -> None:
        """
        Sends the result to its final destination.
        Implementations must guarantee thread-safety when touching shared resources.
        """
        pass

    def close(self) -> None:  # noqa: B027 -- deliberate no-op: close is optional
        """
        Lifecycle method called when the Loom shuts down.
        Useful for closing connections, flushing buffers or stopping background threads.
        """
        pass


class JsonFileSink(Sink):
    """
    Default sink that appends results to a local JSON-lines file.
    Uses a threading.Lock to keep concurrent writes consistent.
    """

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        # The lock ensures only one Weaver writes to the file at a time
        self._lock = threading.Lock()

    def send(self, result: Dict[str, Any]) -> None:
        filename = self.output_dir / "results.json"

        with self._lock:
            with open(filename, "a") as f:
                json.dump(result, f)
                f.write("\n")


class CsvFileSink(Sink):
    """
    Sink that writes results to a local CSV file.

    The header comes from the keys of the first result received.
    In subsequent results, extra keys are ignored and missing keys are
    left empty. Uses a threading.Lock to keep concurrent writes
    consistent.
    """

    def __init__(self, output_dir: Path, filename: str = "results.csv"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._path = self.output_dir / filename
        self._lock = threading.Lock()
        self._fieldnames: Optional[list] = None

    def send(self, result: Dict[str, Any]) -> None:
        with self._lock:
            fieldnames = self._fieldnames
            write_header = fieldnames is None
            if fieldnames is None:
                fieldnames = list(result.keys())
                self._fieldnames = fieldnames
            with open(self._path, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                if write_header:
                    writer.writeheader()
                writer.writerow(result)


class CallbackSink(Sink):
    """
    Sink that delegates every result to a user-provided callable.
    Lets you plug the pipeline into any destination (external queue,
    database, metrics) without subclassing Sink.

    Note: the callable is invoked from the Weaver threads — it must be
    thread-safe.
    """

    def __init__(
        self,
        callback: Callable[[Dict[str, Any]], None],
        on_close: Optional[Callable[[], None]] = None,
    ):
        self.callback = callback
        self.on_close = on_close

    def send(self, result: Dict[str, Any]) -> None:
        self.callback(result)

    def close(self) -> None:
        if self.on_close is not None:
            self.on_close()


class ThreadedBufferedSink(Sink):
    """
    Decorator that adds an in-memory buffer and asynchronous writing
    to any existing Sink.

    The worker consumes the buffer until it finds the stop sentinel,
    which guarantees every item sent before close() is delivered to the
    target sink, with no race window between signaling and draining.
    """

    # Internal sentinel instructing the worker to exit after draining the queue
    _STOP: Any = object()

    def __init__(self, target_sink: Sink, buffer_size: int = 1000):
        self.target = target_sink
        self.queue: queue.Queue = queue.Queue(maxsize=buffer_size)
        self._closed = False
        self._close_lock = threading.Lock()
        self.worker_thread = threading.Thread(target=self._worker, daemon=True)
        self.worker_thread.start()

    def send(self, result: Dict[str, Any]) -> None:
        if self._closed:
            raise LoomError("ThreadedBufferedSink is already closed; send() is not allowed.")
        self.queue.put(result)

    def _worker(self) -> None:
        while True:
            item = self.queue.get()
            try:
                if item is self._STOP:
                    return
                self.target.send(item)
            except Exception:
                # The worker must survive target sink failures, otherwise
                # the queue stops draining and close() hangs.
                logger.exception("Target sink failed to receive an item; item dropped.")
            finally:
                self.queue.task_done()

    def close(self) -> None:
        # Idempotent: only the first call performs the shutdown
        with self._close_lock:
            if self._closed:
                return
            self._closed = True

        # The sentinel goes in behind any pending items: the worker
        # drains everything before exiting
        self.queue.put(self._STOP)
        self.worker_thread.join()

        # Propagate the close
        self.target.close()
