# dataloom/loom.py

"""
O módulo Loom é o coração do motor de orquestração.
Define a classe principal responsável por gerenciar o ciclo de vida
das threads (Weavers) e a distribuição de tarefas.
"""

import threading
import queue
import time
from typing import Optional
import numpy as np

from dataloom.types import LoomState
from dataloom.config import LoomConfig
from dataloom.processors import Processor
from dataloom.sinks import Sink
from dataloom.weaver import Weaver
from dataloom.hooks import LoomHooks


class Loom:
    """
    Orquestrador principal do DataLoom.

    Exemplo de uso:
        config = LoomConfig(...)
        loom = Loom(config, processor, sink)
        loom.start()
    """

    def __init__(
        self,
        config: LoomConfig,
        processor: Processor,
        sink: Sink,
        hooks: Optional[LoomHooks] = None,
        num_weavers: int = 2,
    ):
        self.config = config
        self.processor = processor
        self.sink = sink

        # Garante nova instância de hooks se não fornecida (evita estado global)
        self.hooks = hooks or LoomHooks()
        self.num_weavers = num_weavers

        self.state = LoomState.PENDING
        self.task_queue: queue.Queue = queue.Queue()
        self.stop_event = threading.Event()
        self.weavers: list[Weaver] = []

    def start(self) -> None:
        """
        Inicializa os Weavers e começa o loop de geração de tarefas.
        Este método bloqueia a execução até que ocorra erro ou parada.
        """
        self.state = LoomState.RUNNING
        self.hooks.on_start()

        # Acorda os tecelões
        for _ in range(self.num_weavers):
            weaver = Weaver(
                self.task_queue,
                self.processor,
                self.sink,
                self.stop_event,
            )
            weaver.start()
            self.weavers.append(weaver)

        try:
            while not self.stop_event.is_set():
                # Geração de dados (Simulação)
                batch = np.random.rand(self.config.batch_size)
                self.task_queue.put(batch)
                time.sleep(self.config.interval_seconds)
        except Exception as e:
            self.state = LoomState.FAILED
            self.hooks.on_error(e)
            raise
        finally:
            self.stop()

    def stop(self) -> None:
        """
        Sinaliza a parada de todos os componentes e aguarda limpeza.
        Seguro para ser chamado múltiplas vezes ou dentro de blocos finally.
        """
        self.stop_event.set()
        # Aguarda que a fila seja totalmente consumida
        self.task_queue.join()
        self.state = LoomState.COMPLETED
        self.hooks.on_stop()
