# tests/test_loom.py

import threading
from typing import Any, Iterator

import numpy as np
import pytest

from dataloom_engine import Loom, LoomConfig, LoomHooks, LoomState, Processor, Sink
from dataloom_engine.exceptions import ConfigurationError, LoomError, WeaverError
from dataloom_engine.sources import Source

# --- Mocks ---


class InMemorySink(Sink):
    def __init__(self):
        self.results = []
        self._lock = threading.Lock()

    def send(self, result):
        with self._lock:
            self.results.append(result)


class PassthroughProcessor(Processor):
    def process(self, batch):
        return {"data": batch[0]}


class FiniteSource(Source):
    """Yields a fixed list of batches."""

    def __init__(self, data: list):
        self.data = data

    def __iter__(self) -> Iterator[Any]:
        for item in self.data:
            yield np.array([item])


# --- Tests ---


def test_loom_uses_custom_source():
    """
    Verifies that Loom correctly consumes data from a custom Source,
    processes it via Weavers, and deposits into Sink.
    """
    # Setup
    config = LoomConfig(
        output_dir=".",  # Ignored by InMemorySink
        batch_size=1,
        interval_seconds=0,
    )

    source_data = [10, 20, 30]
    source = FiniteSource(source_data)
    sink = InMemorySink()
    processor = PassthroughProcessor()

    loom = Loom(config=config, processor=processor, sink=sink, source=source, num_weavers=2)

    # start() blocks until the finite source is exhausted, then stops itself
    loom.start()

    # Verify results
    assert len(sink.results) == 3
    values = sorted([r["data"] for r in sink.results])
    assert values == [10, 20, 30]
    assert loom.state.name == "COMPLETED"


class RecordingHooks(LoomHooks):
    """Hooks that record every call for inspection in tests."""

    def __init__(self):
        self._lock = threading.Lock()
        self.started = False
        self.stopped = False
        self.errors = []
        self.batches = []

    def on_start(self):
        self.started = True

    def on_stop(self):
        self.stopped = True

    def on_error(self, error):
        with self._lock:
            self.errors.append(error)

    def on_batch_processed(self, result, duration_seconds):
        with self._lock:
            self.batches.append((result, duration_seconds))


class ExplodingSource(Source):
    """Source that fails after delivering a few items."""

    def __iter__(self) -> Iterator[Any]:
        yield np.array([1])
        raise RuntimeError("source exploded")


def _make_loom(source, hooks=None, sink=None):
    config = LoomConfig(output_dir=".", batch_size=1, interval_seconds=0)
    return Loom(
        config=config,
        processor=PassthroughProcessor(),
        sink=sink or InMemorySink(),
        source=source,
        hooks=hooks,
        num_weavers=2,
    )


def test_loom_failed_state_survives_stop():
    """An error in the source must leave the state as FAILED (not COMPLETED)."""
    hooks = RecordingHooks()
    loom = _make_loom(ExplodingSource(), hooks=hooks)

    with pytest.raises(RuntimeError, match="source exploded"):
        loom.start()

    assert loom.state is LoomState.FAILED
    assert any(isinstance(e, RuntimeError) for e in hooks.errors)
    assert hooks.stopped  # stop() still performs the full cleanup


def test_loom_stop_is_idempotent():
    """stop() can be called multiple times without hanging or re-running hooks."""
    hooks = RecordingHooks()
    loom = _make_loom(FiniteSource([1, 2]), hooks=hooks)
    loom.start()  # the internal finally already calls stop()

    loom.stop()
    loom.stop()

    assert loom.state is LoomState.COMPLETED
    assert hooks.stopped


def test_loom_stop_drains_pending_items():
    """
    stop() with items still queued must process them and shut down,
    without deadlocking (regression for the old task_queue.join() stop).
    """
    sink = InMemorySink()
    loom = _make_loom(FiniteSource([1, 2, 3, 4, 5]), sink=sink)

    # Enqueue items directly, before start(), simulating a queue with backlog
    for pending_value in [10, 20, 30]:
        loom.task_queue.put(np.array([pending_value]))

    runner = threading.Thread(target=loom.start, daemon=True)
    runner.start()
    runner.join(timeout=5)

    assert not runner.is_alive(), "Loom.start() did not finish: possible deadlock"
    assert loom.state is LoomState.COMPLETED
    # Pre-queued items + source items were all processed
    assert len(sink.results) == 8


def test_loom_weaver_error_reported_via_hooks():
    """An error inside the Processor reaches the hooks as WeaverError and doesn't hang the Loom."""

    class BrokenProcessor(Processor):
        def process(self, batch):
            raise ValueError("Boom!")

    hooks = RecordingHooks()
    config = LoomConfig(output_dir=".", batch_size=1, interval_seconds=0)
    loom = Loom(
        config=config,
        processor=BrokenProcessor(),
        sink=InMemorySink(),
        source=FiniteSource([1, 2, 3]),
        hooks=hooks,
        num_weavers=2,
    )

    loom.start()  # must not raise or hang

    assert loom.state is LoomState.COMPLETED
    assert len(hooks.errors) == 3
    assert all(isinstance(e, WeaverError) for e in hooks.errors)


def test_loom_bounded_queue_processes_everything():
    """
    With a small queue (backpressure), a source larger than the queue
    must be fully processed, with no loss and no deadlock.
    """
    config = LoomConfig(
        output_dir=".",
        batch_size=1,
        interval_seconds=0,
        queue_maxsize=2,  # much smaller than the source volume
    )
    sink = InMemorySink()
    loom = Loom(
        config=config,
        processor=PassthroughProcessor(),
        sink=sink,
        source=FiniteSource(list(range(50))),
        num_weavers=2,
    )

    runner = threading.Thread(target=loom.start, daemon=True)
    runner.start()
    runner.join(timeout=10)

    assert not runner.is_alive(), "Loom hung with a bounded queue"
    assert loom.task_queue.maxsize == 2
    assert sorted(r["data"] for r in sink.results) == list(range(50))


def test_loom_default_queue_size_scales_with_weavers():
    loom = _make_loom(FiniteSource([1]))
    assert loom.task_queue.maxsize == loom.num_weavers * 4


def test_loom_reports_batch_metrics_via_hooks():
    """Each successfully processed batch fires on_batch_processed with result and duration."""
    hooks = RecordingHooks()
    loom = _make_loom(FiniteSource([10, 20, 30]), hooks=hooks)
    loom.start()

    assert len(hooks.batches) == 3
    values = sorted(result["data"] for result, _ in hooks.batches)
    assert values == [10, 20, 30]
    assert all(duration >= 0 for _, duration in hooks.batches)


def test_loom_no_batch_metrics_on_failure():
    """A failing batch emits no success metric — only on_error."""

    class BrokenProcessor(Processor):
        def process(self, batch):
            raise ValueError("Boom!")

    hooks = RecordingHooks()
    config = LoomConfig(output_dir=".", batch_size=1, interval_seconds=0)
    loom = Loom(
        config=config,
        processor=BrokenProcessor(),
        sink=InMemorySink(),
        source=FiniteSource([1, 2]),
        hooks=hooks,
        num_weavers=2,
    )
    loom.start()

    assert hooks.batches == []
    assert len(hooks.errors) == 2


def test_loom_survives_broken_metrics_hook():
    """An exception inside on_batch_processed must not bring down the Weaver."""

    class BrokenMetricsHooks(RecordingHooks):
        def on_batch_processed(self, result, duration_seconds):
            raise RuntimeError("metrics backend offline")

    hooks = BrokenMetricsHooks()
    sink = InMemorySink()
    loom = _make_loom(FiniteSource([1, 2, 3]), hooks=hooks, sink=sink)
    loom.start()

    # Every item was processed despite the broken hook
    assert len(sink.results) == 3
    assert loom.state is LoomState.COMPLETED


@pytest.mark.parametrize("num_weavers", [0, -1])
def test_loom_rejects_non_positive_num_weavers(num_weavers):
    """
    num_weavers < 1 must fail fast: 0 weavers would silently create an
    unbounded queue (0 * 4 = 0 = no limit) and complete without
    processing anything.
    """
    config = LoomConfig(output_dir=".", batch_size=1, interval_seconds=0)
    with pytest.raises(ConfigurationError):
        Loom(
            config=config,
            processor=PassthroughProcessor(),
            sink=InMemorySink(),
            source=FiniteSource([1]),
            num_weavers=num_weavers,
        )


def test_loom_start_cannot_be_reused():
    """
    A Loom instance is single-use: a second start() must raise instead of
    spawning Weavers that never receive a stop sentinel (thread leak with
    the state stuck in RUNNING).
    """

    class CountingHooks(LoomHooks):
        def __init__(self):
            self.start_calls = 0

        def on_start(self):
            self.start_calls += 1

    hooks = CountingHooks()
    loom = _make_loom(FiniteSource([1]), hooks=hooks)
    loom.start()
    weavers_after_first = len(loom.weavers)

    with pytest.raises(LoomError):
        loom.start()

    assert len(loom.weavers) == weavers_after_first  # no leaked threads
    assert loom.state is LoomState.COMPLETED  # not stuck in RUNNING
    assert hooks.start_calls == 1  # the rejected start never fired hooks


def test_loom_start_after_early_stop_raises():
    """stop() before start() must also make start() unusable (same leak scenario)."""
    loom = _make_loom(FiniteSource([1]))
    loom.stop()

    with pytest.raises(LoomError):
        loom.start()

    assert loom.weavers == []


def test_loom_as_context_manager():
    """The with block yields the Loom itself and guarantees stop() on exit."""
    hooks = RecordingHooks()
    sink = InMemorySink()

    with _make_loom(FiniteSource([1, 2, 3]), hooks=hooks, sink=sink) as loom:
        assert isinstance(loom, Loom)
        loom.start()

    assert loom.state is LoomState.COMPLETED
    assert hooks.stopped
    assert len(sink.results) == 3


def test_loom_context_manager_stops_on_exception():
    """An exception inside the with block must not escape without cleanup: stop() still runs."""
    hooks = RecordingHooks()
    loom = _make_loom(FiniteSource([1]), hooks=hooks)

    with pytest.raises(RuntimeError, match="user code failed"):
        with loom:
            # The weavers never even started: __exit__ must shut down without hanging
            raise RuntimeError("user code failed")

    assert hooks.stopped
    # start() never ran, so the state must not pretend completion
    assert loom.state is LoomState.PENDING


def test_loom_context_manager_after_start_is_noop():
    """__exit__'s stop() after a completed start() doesn't hang or repeat hooks."""
    hooks = RecordingHooks()

    with _make_loom(FiniteSource([1, 2]), hooks=hooks) as loom:
        loom.start()  # start() already calls stop() internally in its finally

    assert loom.state is LoomState.COMPLETED
    assert hooks.stopped
