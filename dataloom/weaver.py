# dataloom/weaver.py

"""
Definição do Trabalhador (Weaver).
Thread dedicada que consome tarefas da fila, processa e envia ao Sink.
Este módulo é interno e não deve ser importado diretamente pelo usuário.
"""

import logging
import queue
import threading
from typing import Any, Callable, Optional

from dataloom.exceptions import WeaverError
from dataloom.processors import Processor
from dataloom.sinks import Sink

logger = logging.getLogger(__name__)

# Sentinela interno: instrui o Weaver a encerrar. O Loom enfileira um
# sentinela por Weaver após os dados, garantindo que a fila seja drenada
# por completo antes das threads morrerem.
STOP_SENTINEL: Any = object()


class Weaver(threading.Thread):
    """
    Agente de execução que roda em uma thread separada.
    Orquestra o fluxo: Fila -> Processor -> Sink.

    Erros de processamento não matam a thread: são convertidos em
    WeaverError, logados e reportados via callback on_error (se fornecido).
    """

    def __init__(
        self,
        task_queue: queue.Queue,
        processor: Processor,
        sink: Sink,
        on_error: Optional[Callable[[Exception], None]] = None,
    ):
        super().__init__(daemon=True)
        self.task_queue = task_queue
        self.processor = processor
        self.sink = sink
        self.on_error = on_error

    def run(self) -> None:
        while True:
            batch = self.task_queue.get()
            try:
                if batch is STOP_SENTINEL:
                    return
                self._process_batch(batch)
            finally:
                # Sinaliza que o item da fila foi tratado (sucesso ou falha)
                self.task_queue.task_done()

    def _process_batch(self, batch: Any) -> None:
        try:
            result = self.processor.process(batch)
            self.sink.send(result)
        except Exception as exc:
            logger.exception("Weaver falhou ao processar um lote; o lote foi descartado.")
            if self.on_error is not None:
                error = WeaverError(f"Falha ao processar lote: {exc}")
                error.__cause__ = exc
                try:
                    self.on_error(error)
                except Exception:
                    logger.exception("Callback on_error lançou uma exceção.")
