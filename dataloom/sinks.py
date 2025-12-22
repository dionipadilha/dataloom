# dataloom/sinks.py

"""
Contratos de saída de dados (Sinks).
Define como e onde os resultados processados são depositados.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any
from pathlib import Path
import json
import threading
import queue


class Sink(ABC):
    """Interface base para destinos de dados."""

    @abstractmethod
    def send(self, result: Dict[str, Any]) -> None:
        """
        Envia o resultado para o destino final.
        Implementações devem garantir thread-safety se acessarem recursos compartilhados.
        """
        pass

    def close(self) -> None:
        """
        Método de ciclo de vida chamado quando o Loom encerra.
        Útil para fechar conexões, flushear buffers ou parar threads de background.
        """
        pass


class JsonFileSink(Sink):
    """
    Sink padrão que escreve resultados em um arquivo JSON local.
    Utiliza threading.Lock para garantir integridade na escrita concorrente.
    """

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        # Lock garante que apenas um Weaver escreva no arquivo por vez
        self._lock = threading.Lock()

    def send(self, result: Dict[str, Any]) -> None:
        filename = self.output_dir / "results.json"

        with self._lock:
            with open(filename, "a") as f:
                json.dump(result, f)
                f.write("\n")


class ThreadedBufferedSink(Sink):
    """
    Decorator que adiciona um buffer em memória e escrita assíncrona
    para qualquer Sink existente.
    """

    def __init__(self, target_sink: Sink, buffer_size: int = 1000):
        self.target = target_sink
        self.queue: queue.Queue = queue.Queue(maxsize=buffer_size)
        self.stop_event = threading.Event()
        self.worker_thread = threading.Thread(target=self._worker, daemon=True)
        self.worker_thread.start()

    def send(self, result: Dict[str, Any]) -> None:
        self.queue.put(result)

    def _worker(self) -> None:
        while not self.stop_event.is_set() or not self.queue.empty():
            try:
                item = self.queue.get(timeout=0.1)
                self.target.send(item)
                self.queue.task_done()
            except queue.Empty:
                continue

    def close(self) -> None:
        # Sinaliza parada
        self.stop_event.set()
        # Aguarda thread terminar (ela vai esvaziar a fila antes)
        self.worker_thread.join()
        # Propaga o fechamento
        self.target.close()
