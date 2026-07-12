# dataloom/sinks.py

"""
Contratos de saída de dados (Sinks).
Define como e onde os resultados processados são depositados.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any
from pathlib import Path
import json
import logging
import threading
import queue

from dataloom.exceptions import LoomError

logger = logging.getLogger(__name__)


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

    O worker consome a fila até encontrar o sentinela de parada, o que
    garante que todos os itens enviados antes do close() sejam entregues
    ao sink alvo, sem janelas de corrida entre sinalização e drenagem.
    """

    # Sentinela interno que instrui o worker a encerrar após drenar a fila
    _STOP: Any = object()

    def __init__(self, target_sink: Sink, buffer_size: int = 1000):
        self.target = target_sink
        self.queue: queue.Queue = queue.Queue(maxsize=buffer_size)
        self._closed = False
        self._close_lock = threading.Lock()
        self.worker_thread = threading.Thread(target=self._worker, daemon=True)
        self.worker_thread.start()

    def send(self, result: Dict[str, Any]) -> None:
        if self._closed:
            raise LoomError("ThreadedBufferedSink já foi fechado; send() não é permitido.")
        self.queue.put(result)

    def _worker(self) -> None:
        while True:
            item = self.queue.get()
            try:
                if item is self._STOP:
                    return
                self.target.send(item)
            except Exception:
                # O worker precisa sobreviver a falhas do sink alvo,
                # senão a fila para de drenar e o close() trava.
                logger.exception("Sink alvo falhou ao receber item; item descartado.")
            finally:
                self.queue.task_done()

    def close(self) -> None:
        # Idempotente: apenas a primeira chamada executa o fechamento
        with self._close_lock:
            if self._closed:
                return
            self._closed = True

        # O sentinela entra atrás dos itens pendentes: o worker drena
        # tudo antes de encerrar
        self.queue.put(self._STOP)
        self.worker_thread.join()

        # Propaga o fechamento
        self.target.close()
