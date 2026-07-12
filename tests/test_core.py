# tests/test_core.py

# Este arquivo cobre:
# - O StatisticsProcessor (lógica matemática).
# - O JsonFileSink (criação de arquivos e conteúdo JSON).
# - O fluxo completo do Weaver (Threading e Fila), mas de forma controlada.

import json
import queue
import tempfile
import threading
from pathlib import Path

import numpy as np
import pytest

from dataloom import JsonFileSink, LoomConfig, Processor, Sink

# Importando classes internas explicitamente para teste
from dataloom.exceptions import WeaverError
from dataloom.processors import StatisticsProcessor
from dataloom.weaver import Weaver, STOP_SENTINEL

# --- Mocks e Helpers ---


class MockSink(Sink):
    """Sink em memória para evitar I/O em disco durante testes de Weaver."""

    def __init__(self):
        self.results = []
        self._lock = threading.Lock()

    def send(self, result):
        with self._lock:
            self.results.append(result)


class SimpleProcessor(Processor):
    """Processador determinístico para testes."""

    def process(self, batch):
        return {"sum": float(np.sum(batch))}


# --- Testes Unitários ---


def test_statistics_processor_logic():
    """Verifica se a matemática do processador padrão está correta."""
    processor = StatisticsProcessor()
    # Cria um batch simples: [1, 2, 3]
    batch = np.array([1.0, 2.0, 3.0])

    result = processor.process(batch)

    assert result["min"] == 1.0
    assert result["max"] == 3.0
    assert result["mean"] == 2.0
    # Desvio padrão de [1, 2, 3] é ~0.816
    assert abs(result["std"] - 0.81649) < 0.0001


def test_json_sink_writes_file(tmp_path):
    """
    Verifica se o JsonFileSink cria o arquivo e escreve JSON válido.
    Usa 'tmp_path' (fixture do pytest) para criar pastas temporárias isoladas.
    """
    sink = JsonFileSink(output_dir=tmp_path)
    data = {"id": 1, "status": "ok"}

    sink.send(data)

    expected_file = tmp_path / "results.json"
    assert expected_file.exists()

    content = expected_file.read_text()
    loaded_json = json.loads(content)
    assert loaded_json == data


# --- Testes de Integração (Weaver/Fluxo) ---


def test_weaver_consumes_queue():
    """
    Testa se um Weaver consome itens da fila e deposita no Sink.
    Este teste simula o ciclo de vida sem iniciar o Loom inteiro.
    """
    task_queue = queue.Queue()
    mock_sink = MockSink()

    # Injeta 3 tarefas na fila, seguidas do sentinela de parada
    task_queue.put(np.array([1, 1]))
    task_queue.put(np.array([2, 2]))
    task_queue.put(np.array([3, 3]))
    task_queue.put(STOP_SENTINEL)

    # Cria e inicia o Weaver
    weaver = Weaver(
        task_queue=task_queue,
        processor=SimpleProcessor(),
        sink=mock_sink,
    )
    weaver.start()

    # O Weaver drena a fila e encerra ao consumir o sentinela
    weaver.join(timeout=2)
    assert not weaver.is_alive()

    # Verificações
    assert len(mock_sink.results) == 3
    # A ordem pode variar em multithread, então somamos tudo para verificar integridade
    total_sum = sum(r["sum"] for r in mock_sink.results)
    assert total_sum == (2.0 + 4.0 + 6.0)  # [1,1]=2, [2,2]=4, [3,3]=6


def test_weaver_handles_processor_error():
    """
    Garante que o Weaver sobrevive a erros de processamento: a thread
    continua viva, os itens seguintes são processados, a fila é limpa
    (task_done) e o erro é reportado via callback on_error.
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

    # O item do meio quebra o processador; os demais devem passar
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

    # Se o finally block do Weaver não chamar task_done(), este join vai travar para sempre.
    task_queue.join()
    weaver.join(timeout=2)
    assert not weaver.is_alive()

    # Os itens válidos foram processados apesar do erro no meio
    assert sorted(r["sum"] for r in mock_sink.results) == [1.0, 3.0]

    # O erro foi tipado e reportado
    assert len(errors) == 1
    assert isinstance(errors[0], WeaverError)
    assert isinstance(errors[0].__cause__, ValueError)
