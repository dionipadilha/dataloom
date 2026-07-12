# tests/test_loom.py

import threading
from typing import Any, Iterator

import numpy as np
import pytest

from dataloom_engine import Loom, LoomConfig, LoomHooks, LoomState, Processor, Sink
from dataloom_engine.exceptions import WeaverError
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

    # Run Loom (start blocks until source is exhausted or error)
    # But wait, Loom.start() blocks until source is exhausted AND then calls stop().
    # Since our source is finite, it should finish naturally.
    if hasattr(loom, "start"):
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


def test_loom_bounded_queue_processes_everything():
    """
    Com fila pequena (backpressure), um source maior que a fila deve
    ser processado por inteiro, sem perda nem deadlock.
    """
    config = LoomConfig(
        output_dir=".",
        batch_size=1,
        interval_seconds=0,
        queue_maxsize=2,  # bem menor que o volume do source
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

    assert not runner.is_alive(), "Loom travou com fila limitada"
    assert loom.task_queue.maxsize == 2
    assert sorted(r["data"] for r in sink.results) == list(range(50))


def test_loom_default_queue_size_scales_with_weavers():
    loom = _make_loom(FiniteSource([1]))
    assert loom.task_queue.maxsize == loom.num_weavers * 4


def test_loom_reports_batch_metrics_via_hooks():
    """Cada lote processado com sucesso dispara on_batch_processed com resultado e duração."""
    hooks = RecordingHooks()
    loom = _make_loom(FiniteSource([10, 20, 30]), hooks=hooks)
    loom.start()

    assert len(hooks.batches) == 3
    values = sorted(result["data"] for result, _ in hooks.batches)
    assert values == [10, 20, 30]
    assert all(duration >= 0 for _, duration in hooks.batches)


def test_loom_no_batch_metrics_on_failure():
    """Lote que falha não emite métrica de sucesso — só on_error."""

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
    """Exceção dentro do on_batch_processed não pode derrubar o Weaver."""

    class BrokenMetricsHooks(RecordingHooks):
        def on_batch_processed(self, result, duration_seconds):
            raise RuntimeError("metrics backend offline")

    hooks = BrokenMetricsHooks()
    sink = InMemorySink()
    loom = _make_loom(FiniteSource([1, 2, 3]), hooks=hooks, sink=sink)
    loom.start()

    # Todos os itens foram processados apesar do hook quebrado
    assert len(sink.results) == 3
    assert loom.state is LoomState.COMPLETED


def test_loom_as_context_manager():
    """O bloco with entrega o próprio Loom e garante stop() na saída."""
    hooks = RecordingHooks()
    sink = InMemorySink()

    with _make_loom(FiniteSource([1, 2, 3]), hooks=hooks, sink=sink) as loom:
        assert isinstance(loom, Loom)
        loom.start()

    assert loom.state is LoomState.COMPLETED
    assert hooks.stopped
    assert len(sink.results) == 3


def test_loom_context_manager_stops_on_exception():
    """Exceção dentro do bloco with não pode vazar sem limpeza: stop() roda mesmo assim."""
    hooks = RecordingHooks()
    loom = _make_loom(FiniteSource([1]), hooks=hooks)

    with pytest.raises(RuntimeError, match="user code failed"):
        with loom:
            # Weavers ainda nem iniciaram: o __exit__ deve encerrar sem travar
            raise RuntimeError("user code failed")

    assert hooks.stopped
    # start() nunca rodou, então o estado não deve fingir conclusão
    assert loom.state is LoomState.PENDING


def test_loom_context_manager_after_start_is_noop():
    """stop() do __exit__ após um start() completo não trava nem repete hooks."""
    hooks = RecordingHooks()

    with _make_loom(FiniteSource([1, 2]), hooks=hooks) as loom:
        loom.start()  # start() já chama stop() internamente no finally

    assert loom.state is LoomState.COMPLETED
    assert hooks.stopped
