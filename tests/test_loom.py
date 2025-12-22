# tests/test_loom.py

import threading
import time
import queue
from typing import Iterator, Any
import numpy as np
import pytest

from dataloom import Loom, LoomConfig, Processor, Sink
from dataloom.sources import Source

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
         output_dir=".", # Ignored by InMemorySink
         batch_size=1,
         interval_seconds=0
    )
    
    source_data = [10, 20, 30]
    source = FiniteSource(source_data)
    sink = InMemorySink()
    processor = PassthroughProcessor()
    
    loom = Loom(
        config=config,
        processor=processor,
        sink=sink,
        source=source,
        num_weavers=2
    )
    
    # Run Loom (start blocks until source is exhausted or error)
    # But wait, Loom.start() blocks until source is exhausted AND then calls stop().
    # Since our source is finite, it should finish naturally.
    if hasattr(loom, 'start'):
        loom.start()
        
    # Verify results
    assert len(sink.results) == 3
    values = sorted([r["data"] for r in sink.results])
    assert values == [10, 20, 30]
    assert loom.state.name == "COMPLETED"
