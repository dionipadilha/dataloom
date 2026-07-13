# benchmarks/throughput.py

"""
Reproducible benchmarks for DataLoom.

Two questions, answered honestly:

1. Speedup — how much faster does an I/O-bound pipeline run with N
   Weavers compared to sequential execution? This is DataLoom's core
   use case: each item spends most of its time waiting (network, disk),
   so threads overlap those waits.

2. Overhead — how many items per second can the engine move when the
   Processor does nothing? This measures the cost of the queue + thread
   machinery itself, and is the price you pay for the orchestration.
   If your per-item work is far cheaper than this, a plain loop is the
   better tool (see "Why not just ThreadPoolExecutor?" in the README).

Zero dependencies — runs anywhere the engine runs:

    python benchmarks/throughput.py

Numbers vary with hardware and load; treat the output as an order of
magnitude, not a contract.
"""

import threading
import time
from typing import Any, Dict, Iterator

from dataloom_engine import CallbackSink, Loom, LoomConfig, Processor
from dataloom_engine.sources import Source

IO_LATENCY_SECONDS = 0.010  # simulated per-item I/O wait
IO_ITEMS = 200
IO_WEAVERS = 8

OVERHEAD_ITEMS = 50_000
OVERHEAD_WEAVERS = 4


class RangeSource(Source):
    def __init__(self, count: int):
        self.count = count

    def __iter__(self) -> Iterator[int]:
        yield from range(self.count)


class SimulatedIOProcessor(Processor):
    """Stands in for an HTTP call, DB query or disk read."""

    def process(self, batch: Any) -> Dict[str, Any]:
        time.sleep(IO_LATENCY_SECONDS)
        return {"item": batch}


class NoOpProcessor(Processor):
    """Does nothing: isolates the engine's own overhead."""

    def process(self, batch: Any) -> Dict[str, Any]:
        return {"item": batch}


class CountingSink(CallbackSink):
    def __init__(self) -> None:
        self.count = 0
        self._lock = threading.Lock()
        super().__init__(self._collect)

    def _collect(self, result: Dict[str, Any]) -> None:
        with self._lock:
            self.count += 1


def run_loom(processor: Processor, items: int, weavers: int) -> float:
    """Runs a full pipeline and returns the elapsed wall-clock seconds."""
    sink = CountingSink()
    config = LoomConfig(interval_seconds=0)
    started = time.monotonic()
    with Loom(
        config=config,
        processor=processor,
        sink=sink,
        source=RangeSource(items),
        num_weavers=weavers,
    ) as loom:
        loom.start()
    elapsed = time.monotonic() - started
    assert sink.count == items, f"expected {items} results, got {sink.count}"
    return elapsed


def bench_io_speedup() -> None:
    print(f"1) I/O-bound speedup — {IO_ITEMS} items x {IO_LATENCY_SECONDS * 1000:.0f}ms each")

    started = time.monotonic()
    for _ in range(IO_ITEMS):
        time.sleep(IO_LATENCY_SECONDS)
    sequential = time.monotonic() - started

    pipeline = run_loom(SimulatedIOProcessor(), IO_ITEMS, IO_WEAVERS)

    print(f"   sequential loop:        {sequential:6.2f}s")
    print(f"   loom ({IO_WEAVERS} weavers):       {pipeline:6.2f}s")
    print(f"   speedup:                {sequential / pipeline:6.1f}x")
    print()


def bench_engine_overhead() -> None:
    print(f"2) Engine overhead — {OVERHEAD_ITEMS} no-op items")

    started = time.monotonic()
    count = 0
    for _ in range(OVERHEAD_ITEMS):
        count += 1
    plain_loop = time.monotonic() - started

    pipeline = run_loom(NoOpProcessor(), OVERHEAD_ITEMS, OVERHEAD_WEAVERS)
    per_item_us = pipeline / OVERHEAD_ITEMS * 1_000_000

    print(f"   plain for-loop:         {plain_loop:6.2f}s (baseline: no engine at all)")
    print(f"   loom ({OVERHEAD_WEAVERS} weavers):       {pipeline:6.2f}s")
    print(f"   throughput:             {OVERHEAD_ITEMS / pipeline:,.0f} items/s")
    print(f"   engine cost per item:   {per_item_us:6.1f}us")
    print()
    print("   Rule of thumb: if your per-item work costs less than the")
    print("   engine cost per item, use a plain loop instead.")


def main() -> None:
    print("DataLoom throughput benchmarks")
    print("=" * 50)
    bench_io_speedup()
    bench_engine_overhead()


if __name__ == "__main__":
    main()
