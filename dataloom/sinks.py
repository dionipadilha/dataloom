# dataloom/sinks.py

"""
Contratos de saída de dados (Sinks).
Define como e onde os resultados processados são depositados.
"""

from abc import ABC, abstractmethod
from typing import Callable, Dict, Any, Optional
from pathlib import Path
import csv
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


class CsvFileSink(Sink):
    """
    Sink que escreve resultados em um arquivo CSV local.

    O cabeçalho é definido pelas chaves do primeiro resultado recebido.
    Nos resultados seguintes, chaves extras são ignoradas e chaves
    ausentes ficam vazias. Utiliza threading.Lock para garantir
    integridade na escrita concorrente.
    """

    def __init__(self, output_dir: Path, filename: str = "results.csv"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._path = self.output_dir / filename
        self._lock = threading.Lock()
        self._fieldnames: Optional[list] = None

    def send(self, result: Dict[str, Any]) -> None:
        with self._lock:
            write_header = self._fieldnames is None
            if write_header:
                self._fieldnames = list(result.keys())
            with open(self._path, "a", newline="") as f:
                writer = csv.DictWriter(
                    f, fieldnames=self._fieldnames, extrasaction="ignore"
                )
                if write_header:
                    writer.writeheader()
                writer.writerow(result)


class CallbackSink(Sink):
    """
    Sink que delega cada resultado a um callable fornecido pelo usuário.
    Permite integrar o pipeline a qualquer destino (fila externa, banco,
    métrica) sem precisar criar uma subclasse de Sink.

    Atenção: o callable é invocado a partir das threads Weaver — deve
    ser thread-safe.
    """

    def __init__(
        self,
        callback: Callable[[Dict[str, Any]], None],
        on_close: Optional[Callable[[], None]] = None,
    ):
        self.callback = callback
        self.on_close = on_close

    def send(self, result: Dict[str, Any]) -> None:
        self.callback(result)

    def close(self) -> None:
        if self.on_close is not None:
            self.on_close()


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
