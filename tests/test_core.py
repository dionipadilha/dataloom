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
from dataloom.processors import StatisticsProcessor
from dataloom.weaver import Weaver

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
    stop_event = threading.Event()

    # Injeta 3 tarefas na fila
    task_queue.put(np.array([1, 1]))
    task_queue.put(np.array([2, 2]))
    task_queue.put(np.array([3, 3]))

    # Cria e inicia o Weaver
    weaver = Weaver(
        task_queue=task_queue,
        processor=SimpleProcessor(),
        sink=mock_sink,
        stop_event=stop_event,
    )
    weaver.start()

    # Aguarda a fila esvaziar (com timeout para não travar o teste se falhar)
    task_queue.join()

    # Para o Weaver
    stop_event.set()
    weaver.join(timeout=2)  # Aguarda a thread morrer

    # Verificações
    assert len(mock_sink.results) == 3
    # A ordem pode variar em multithread, então somamos tudo para verificar integridade
    total_sum = sum(r["sum"] for r in mock_sink.results)
    assert total_sum == (2.0 + 4.0 + 6.0)  # [1,1]=2, [2,2]=4, [3,3]=6


def test_weaver_handles_processor_error():
    """
    Garante que o Weaver não morre silenciosamente e limpa a fila mesmo em erro.
    (Nota: Atualmente, o Weaver deixa a exceção subir no Loom.Hooks,
     mas a queue.task_done() DEVE ser chamada).
    """

    class BrokenProcessor(Processor):
        def process(self, batch):
            raise ValueError("Boom!")

    task_queue = queue.Queue()
    stop_event = threading.Event()

    task_queue.put(np.array([1]))

    weaver = Weaver(
        task_queue=task_queue,
        processor=BrokenProcessor(),
        sink=MockSink(),
        stop_event=stop_event,
    )

    # Não vamos dar start() thread aqui para capturar a exceção mais fácil,
    # vamos chamar o corpo do loop logicamente se possível, ou usar um wrapper.
    # Mas para teste de integração simples, vamos verificar se a fila destrava.

    weaver.start()

    # Se o finally block do Weaver não chamar task_done(), este join vai travar para sempre.
    # O teste passar significa que o task_done() foi chamado corretamente.
    try:
        task_queue.join()
    except KeyboardInterrupt:
        pytest.fail("Weaver travou a fila ao encontrar erro!")
    finally:
        stop_event.set()
        weaver.join()
