# tests/test_core.py

# This file covers:
# - The StatisticsProcessor (math logic).
# - The JsonFileSink (file creation and JSON content).
# - The full Weaver flow (threading and queue), in a controlled way.

import json
import queue
import threading

import numpy as np

from dataloom_engine import JsonFileSink, LoomConfig, Processor, Sink

# Importing internal classes explicitly for testing
from dataloom_engine.exceptions import WeaverError
from dataloom_engine.processors import StatisticsProcessor
from dataloom_engine.sources import RandomNumPySource
from dataloom_engine.weaver import STOP_SENTINEL, Weaver

# --- Mocks and helpers ---


class MockSink(Sink):
    """In-memory sink to avoid disk I/O during Weaver tests."""

    def __init__(self):
        self.results = []
        self._lock = threading.Lock()

    def send(self, result):
        with self._lock:
            self.results.append(result)


class SimpleProcessor(Processor):
    """Deterministic processor for tests."""

    def process(self, batch):
        return {"sum": float(np.sum(batch))}


# --- Unit tests ---


def test_statistics_processor_logic():
    """Verifies the math of the reference processor."""
    processor = StatisticsProcessor()
    # Simple batch: [1, 2, 3]
    batch = np.array([1.0, 2.0, 3.0])

    result = processor.process(batch)

    assert result["min"] == 1.0
    assert result["max"] == 3.0
    assert result["mean"] == 2.0
    # Standard deviation of [1, 2, 3] is ~0.816
    assert abs(result["std"] - 0.81649) < 0.0001


def test_json_sink_writes_file(tmp_path):
    """
    Verifies that JsonFileSink creates the file and writes valid JSON.
    Uses 'tmp_path' (pytest fixture) for isolated temporary directories.
    """
    sink = JsonFileSink(output_dir=tmp_path)
    data = {"id": 1, "status": "ok"}

    sink.send(data)

    expected_file = tmp_path / "results.json"
    assert expected_file.exists()

    content = expected_file.read_text()
    loaded_json = json.loads(content)
    assert loaded_json == data


def test_json_sink_accepts_custom_filename(tmp_path):
    """The output filename is configurable (parity with CsvFileSink)."""
    sink = JsonFileSink(output_dir=tmp_path, filename="custom.jsonl")
    sink.send({"id": 7})

    assert (tmp_path / "custom.jsonl").exists()
    assert not (tmp_path / "results.json").exists()
    assert json.loads((tmp_path / "custom.jsonl").read_text()) == {"id": 7}


def test_random_numpy_source_respects_limit_and_batch_size():
    """The demo source yields exactly `limit` batches of `batch_size` values in [0, 1)."""
    config = LoomConfig(output_dir=".", batch_size=5, interval_seconds=0)
    source = RandomNumPySource(config, limit=3)

    batches = list(source)

    assert len(batches) == 3
    for batch in batches:
        assert len(batch) == 5
        assert float(batch.min()) >= 0.0
        assert float(batch.max()) < 1.0


# --- Integration tests (Weaver/flow) ---


def test_weaver_consumes_queue():
    """
    Tests that a Weaver consumes items from the queue and deposits them
    into the Sink. Simulates the lifecycle without starting a full Loom.
    """
    task_queue = queue.Queue()
    mock_sink = MockSink()

    # Inject 3 tasks into the queue, followed by the stop sentinel
    task_queue.put(np.array([1, 1]))
    task_queue.put(np.array([2, 2]))
    task_queue.put(np.array([3, 3]))
    task_queue.put(STOP_SENTINEL)

    # Create and start the Weaver
    weaver = Weaver(
        task_queue=task_queue,
        processor=SimpleProcessor(),
        sink=mock_sink,
    )
    weaver.start()

    # The Weaver drains the queue and exits upon consuming the sentinel
    weaver.join(timeout=2)
    assert not weaver.is_alive()

    # Verify
    assert len(mock_sink.results) == 3
    # Order may vary across threads, so we sum everything to check integrity
    total_sum = sum(r["sum"] for r in mock_sink.results)
    assert total_sum == (2.0 + 4.0 + 6.0)  # [1,1]=2, [2,2]=4, [3,3]=6


def test_weaver_handles_processor_error():
    """
    Guarantees the Weaver survives processing errors: the thread stays
    alive, subsequent items are processed, the queue is cleaned up
    (task_done) and the error is reported via the on_error callback.
    """

    class BrokenProcessor(Processor):
        def process(self, batch):
            if batch[0] == 2:
                raise ValueError("Boom!")
            return {"sum": float(np.sum(batch))}

    task_queue = queue.Queue()
    mock_sink = MockSink()
    errors = []
    errors_lock = threading.Lock()

    def on_error(exc):
        with errors_lock:
            errors.append(exc)

    # The middle item breaks the processor; the others must pass
    task_queue.put(np.array([1]))
    task_queue.put(np.array([2]))
    task_queue.put(np.array([3]))
    task_queue.put(STOP_SENTINEL)

    weaver = Weaver(
        task_queue=task_queue,
        processor=BrokenProcessor(),
        sink=mock_sink,
        on_error=on_error,
    )
    weaver.start()

    # If the Weaver's finally block doesn't call task_done(), this join hangs forever.
    task_queue.join()
    weaver.join(timeout=2)
    assert not weaver.is_alive()

    # The valid items were processed despite the error in the middle
    assert sorted(r["sum"] for r in mock_sink.results) == [1.0, 3.0]

    # The error was typed and reported
    assert len(errors) == 1
    assert isinstance(errors[0], WeaverError)
    assert isinstance(errors[0].__cause__, ValueError)
