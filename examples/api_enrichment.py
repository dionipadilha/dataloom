# examples/api_enrichment.py

"""
Enriching records through an external API — DataLoom's flagship use case.

Scenario: you have thousands of records and need to call a third-party
API for each one (scoring, geocoding, LLM classification...). Each call
spends most of its time waiting on the network, so running them through
parallel Weavers gives a near-linear speedup, while backpressure keeps
memory flat and per-batch metrics tell you throughput and latency.

This example simulates the API with a sleep so it runs offline — swap
`fake_scoring_api` for a real HTTP call and it becomes production code.

Run it (no extra dependencies needed):
    python examples/api_enrichment.py
"""

import threading
import time
from typing import Any, Dict, Iterator

from dataloom_engine import CallbackSink, Loom, LoomConfig, LoomHooks, Processor
from dataloom_engine.sources import Source

API_LATENCY_SECONDS = 0.2
NUM_WEAVERS = 8

CUSTOMERS = [
    {"id": i, "name": f"customer-{i:03d}", "city": city}
    for i, city in enumerate(
        ["Curitiba", "Lisboa", "Porto Alegre", "Recife", "Salvador", "Manaus"] * 5
    )
]


class CustomerSource(Source):
    """Yields one customer record at a time (could be a DB cursor or a file)."""

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        yield from CUSTOMERS


def fake_scoring_api(customer: Dict[str, Any]) -> Dict[str, Any]:
    """Stands in for a real HTTP call — replace with requests/httpx in real use."""
    time.sleep(API_LATENCY_SECONDS)  # simulated network latency
    score = sum(ord(ch) for ch in customer["name"]) % 100  # deterministic fake score
    return {"score": score, "risk": "high" if score > 70 else "normal"}


class EnrichProcessor(Processor):
    """Calls the API for each record and merges the response into it."""

    def process(self, batch: Any) -> Dict[str, Any]:
        api_response = fake_scoring_api(batch)
        return {**batch, **api_response}


class ThroughputHooks(LoomHooks):
    """Collects per-batch metrics — in real use, feed these to your metrics backend."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.batches = 0
        self.total_seconds = 0.0

    def on_batch_processed(self, result: Dict[str, Any], duration_seconds: float) -> None:
        with self._lock:
            self.batches += 1
            self.total_seconds += duration_seconds


def main() -> None:
    enriched: list = []
    collect_lock = threading.Lock()

    def collect(result: Dict[str, Any]) -> None:
        with collect_lock:
            enriched.append(result)

    config = LoomConfig(output_dir=".", interval_seconds=0)  # output_dir unused here
    hooks = ThroughputHooks()

    started = time.monotonic()
    with Loom(
        config=config,
        processor=EnrichProcessor(),
        sink=CallbackSink(collect),
        source=CustomerSource(),
        hooks=hooks,
        num_weavers=NUM_WEAVERS,
    ) as loom:
        loom.start()
    elapsed = time.monotonic() - started

    sequential_estimate = len(CUSTOMERS) * API_LATENCY_SECONDS
    print(
        f"Enriched {len(enriched)} records in {elapsed:.1f}s "
        f"(sequential would take ~{sequential_estimate:.1f}s "
        f"-> ~{sequential_estimate / elapsed:.1f}x speedup with {NUM_WEAVERS} weavers)"
    )
    print(f"Average API latency observed: {hooks.total_seconds / hooks.batches * 1000:.0f}ms")
    print(f"Sample result: {enriched[0]}")
    print(f"Final state: {loom.state.name}")


if __name__ == "__main__":
    main()
