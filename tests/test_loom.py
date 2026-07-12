# tests/test_loom.py

import threading
import time
import queue
from typing import Iterator, Any
import numpy as np
import pytest

from dataloom import Loom, LoomConfig, LoomHooks, LoomState, Processor, Sink
from dataloom.exceptions import WeaverError
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


class RecordingHooks(LoomHooks):
    """Hooks que registram todas as chamadas para inspeção nos testes."""

    def __init__(self):
        self._lock = threading.Lock()
        self.started = False
        self.stopped = False
        self.errors = []

    def on_start(self):
        self.started = True

    def on_stop(self):
        self.stopped = True

    def on_error(self, error):
        with self._lock:
            self.errors.append(error)


class ExplodingSource(Source):
    """Source que falha após entregar alguns itens."""

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
    """Erro no source deve deixar o estado FAILED (não COMPLETED)."""
    hooks = RecordingHooks()
    loom = _make_loom(ExplodingSource(), hooks=hooks)

    with pytest.raises(RuntimeError, match="source exploded"):
        loom.start()

    assert loom.state is LoomState.FAILED
    assert any(isinstance(e, RuntimeError) for e in hooks.errors)
    assert hooks.stopped  # stop() ainda executa a limpeza completa


def test_loom_stop_is_idempotent():
    """stop() pode ser chamado múltiplas vezes sem travar ou re-executar hooks."""
    hooks = RecordingHooks()
    loom = _make_loom(FiniteSource([1, 2]), hooks=hooks)
    loom.start()  # finally interno já chama stop()

    loom.stop()
    loom.stop()

    assert loom.state is LoomState.COMPLETED
    assert hooks.stopped


def test_loom_stop_drains_pending_items():
    """
    stop() com itens ainda na fila deve processá-los e encerrar,
    sem deadlock (regressão do task_queue.join() no stop antigo).
    """
    sink = InMemorySink()
    loom = _make_loom(FiniteSource([1, 2, 3, 4, 5]), sink=sink)

    # Enfileira itens diretamente, antes do start(), simulando fila com backlog
    for pending_value in [10, 20, 30]:
        loom.task_queue.put(np.array([pending_value]))

    runner = threading.Thread(target=loom.start, daemon=True)
    runner.start()
    runner.join(timeout=5)

    assert not runner.is_alive(), "Loom.start() não terminou: possível deadlock"
    assert loom.state is LoomState.COMPLETED
    # Itens pré-enfileirados + itens do source foram todos processados
    assert len(sink.results) == 8


def test_loom_weaver_error_reported_via_hooks():
    """Erro dentro do Processor chega aos hooks como WeaverError e não trava o Loom."""

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

    loom.start()  # não deve levantar nem travar

    assert loom.state is LoomState.COMPLETED
    assert len(hooks.errors) == 3
    assert all(isinstance(e, WeaverError) for e in hooks.errors)
