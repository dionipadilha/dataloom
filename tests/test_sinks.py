# tests/test_sinks.py

import pytest
import threading
import time
from dataloom.sinks import Sink, ThreadedBufferedSink

class MockSink(Sink):
    def __init__(self):
        self.results = []
        self.closed_called = False
        self._lock = threading.Lock()
        
    def send(self, result):
        with self._lock:
            self.results.append(result)
            
    def close(self):
        self.closed_called = True

def test_threaded_sink_delivers_all_items():
    """
    Verifies that the ThreadedBufferedSink:
    1. Buffers items.
    2. Writes them to the target sink asynchronously.
    3. Flushes everything on close().
    4. Calls target.close().
    """
    target = MockSink()
    # Buffer size small to ensure no overflow issues, or large... functionality is same.
    buffered_sink = ThreadedBufferedSink(target, buffer_size=100)
    
    # Send data
    items_to_send = [{"id": i} for i in range(50)]
    for item in items_to_send:
        buffered_sink.send(item)
        
    # At this point, items might be in queue or processed.
    # calling close() should guarantee they are flushed.
    buffered_sink.close()
    
    assert len(target.results) == 50
    assert target.closed_called is True
    
    # Verify content
    target_ids = sorted([r["id"] for r in target.results])
    assert target_ids == list(range(50))
