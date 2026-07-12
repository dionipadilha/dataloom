# tests/test_sinks.py

import csv
import pytest
import threading
import time
from dataloom.exceptions import LoomError
from dataloom.sinks import CallbackSink, CsvFileSink, Sink, ThreadedBufferedSink

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


def test_threaded_sink_rejects_send_after_close():
    """send() após close() deve falhar explicitamente, não perder dados em silêncio."""
    target = MockSink()
    buffered_sink = ThreadedBufferedSink(target)
    buffered_sink.close()

    with pytest.raises(LoomError):
        buffered_sink.send({"id": 1})


def test_threaded_sink_close_is_idempotent():
    """close() repetido não deve travar nem fechar o alvo duas vezes."""
    target = MockSink()
    buffered_sink = ThreadedBufferedSink(target)

    buffered_sink.close()
    buffered_sink.close()

    assert target.closed_called is True


def test_threaded_sink_survives_target_errors():
    """Falha no sink alvo não pode matar o worker: itens seguintes continuam fluindo."""

    class FlakySink(Sink):
        def __init__(self):
            self.results = []
            self._lock = threading.Lock()

        def send(self, result):
            if result["id"] == 1:
                raise IOError("disco cheio!")
            with self._lock:
                self.results.append(result)

    target = FlakySink()
    buffered_sink = ThreadedBufferedSink(target)

    for i in range(3):
        buffered_sink.send({"id": i})
    buffered_sink.close()

    assert sorted(r["id"] for r in target.results) == [0, 2]


def test_csv_sink_writes_header_and_rows(tmp_path):
    """O CSV recebe cabeçalho na primeira escrita e uma linha por resultado."""
    sink = CsvFileSink(output_dir=tmp_path)
    sink.send({"id": 1, "status": "ok"})
    sink.send({"id": 2, "status": "fail"})

    with open(tmp_path / "results.csv", newline="") as f:
        rows = list(csv.DictReader(f))

    assert rows == [
        {"id": "1", "status": "ok"},
        {"id": "2", "status": "fail"},
    ]


def test_csv_sink_handles_key_variations(tmp_path):
    """Chaves extras são ignoradas, ausentes ficam vazias; cabeçalho vem do primeiro resultado."""
    sink = CsvFileSink(output_dir=tmp_path)
    sink.send({"a": 1, "b": 2})
    sink.send({"a": 3, "b": 4, "extra": 99})  # extra ignorada
    sink.send({"a": 5})  # b ausente fica vazia

    with open(tmp_path / "results.csv", newline="") as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames == ["a", "b"]
        rows = list(reader)

    assert rows[1] == {"a": "3", "b": "4"}
    assert rows[2] == {"a": "5", "b": ""}


def test_csv_sink_concurrent_writes(tmp_path):
    """Escritas de múltiplas threads não podem corromper ou perder linhas."""
    sink = CsvFileSink(output_dir=tmp_path)

    def write_batch(offset):
        for i in range(25):
            sink.send({"id": offset + i})

    threads = [threading.Thread(target=write_batch, args=(n * 25,)) for n in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    with open(tmp_path / "results.csv", newline="") as f:
        ids = sorted(int(row["id"]) for row in csv.DictReader(f))

    assert ids == list(range(100))


def test_callback_sink_delegates_send_and_close():
    received = []
    closed = []

    sink = CallbackSink(received.append, on_close=lambda: closed.append(True))
    sink.send({"id": 1})
    sink.send({"id": 2})
    sink.close()

    assert received == [{"id": 1}, {"id": 2}]
    assert closed == [True]


def test_callback_sink_close_without_handler_is_noop():
    sink = CallbackSink(lambda result: None)
    sink.close()  # não deve levantar


def test_threaded_sink_no_data_loss_on_racy_close():
    """
    Regressão da corrida do worker antigo (stop_event + queue.empty()):
    fechar imediatamente após enviar não pode perder itens.
    """
    for _ in range(20):  # repete para dar chance à corrida
        target = MockSink()
        buffered_sink = ThreadedBufferedSink(target)
        for i in range(10):
            buffered_sink.send({"id": i})
        buffered_sink.close()  # sem esperar o worker

        assert len(target.results) == 10
