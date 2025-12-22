# dataloom/loom.py

"""
O módulo Loom é o coração do motor de orquestração.
Define a classe principal responsável por gerenciar o ciclo de vida
das threads (Weavers) e a distribuição de tarefas.
"""

import threading
import queue
import time
from typing import Optional, TYPE_CHECKING
import numpy as np

from dataloom.types import LoomState
from dataloom.config import LoomConfig
from dataloom.processors import Processor
from dataloom.sinks import Sink
from dataloom.weaver import Weaver
from dataloom.hooks import LoomHooks

if TYPE_CHECKING:
    from dataloom.sources import Source


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
        source: Optional["Source"] = None,
        hooks: Optional[LoomHooks] = None,
        num_weavers: int = 2,
    ):
        self.config = config
        self.processor = processor
        self.sink = sink
        
        # Default dependency injection if not provided
        if source is None:
            # Avoid circular import at top-level if possible, or just import at top
            from dataloom.sources import RandomNumPySource
            self.source = RandomNumPySource(config)
        else:
            self.source = source

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
            # Consome do Source
            for batch in self.source:
                if self.stop_event.is_set():
                    break
                self.task_queue.put(batch)
            
            # O processamento normal acabou. Aguarda esvaziar a fila.
            # Isso garante que stop() não trave esperando join() enquanto weavers já pararam.
            self.task_queue.join()
            
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
        try:
            self.sink.close()
        except Exception as e:
            # Não queremos que erro no close esconda outros erros, mas logamos
            self.hooks.on_error(e)
            
        self.hooks.on_stop()
