# dataloom/weaver.py

"""
Definição do Trabalhador (Weaver).
Thread dedicada que consome tarefas da fila, processa e envia ao Sink.
Este módulo é interno e não deve ser importado diretamente pelo usuário.
"""

import threading
import queue

from dataloom.processors import Processor
from dataloom.sinks import Sink


class Weaver(threading.Thread):
    """
    Agente de execução que roda em uma thread separada.
    Orquestra o fluxo: Fila -> Processor -> Sink.
    """

    def __init__(
        self,
        task_queue: queue.Queue,
        processor: Processor,
        sink: Sink,
        stop_event: threading.Event,
    ):
        super().__init__(daemon=True)
        self.task_queue = task_queue
        self.processor = processor
        self.sink = sink
        self.stop_event = stop_event

    def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                # Timeout permite verificar o stop_event periodicamente
                batch = self.task_queue.get(timeout=1)
            except queue.Empty:
                continue

            try:
                # Processamento e envio
                result = self.processor.process(batch)
                self.sink.send(result)
            finally:
                # Sinaliza que o item da fila foi tratado (sucesso ou falha)
                self.task_queue.task_done()
