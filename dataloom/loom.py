# dataloom/loom.py

"""
O módulo Loom é o coração do motor de orquestração.
Define a classe principal responsável por gerenciar o ciclo de vida
das threads (Weavers) e a distribuição de tarefas.
"""

import queue
import threading
from typing import TYPE_CHECKING, Optional

from dataloom.config import LoomConfig
from dataloom.hooks import LoomHooks
from dataloom.processors import Processor
from dataloom.sinks import Sink
from dataloom.types import LoomState
from dataloom.weaver import STOP_SENTINEL, Weaver

if TYPE_CHECKING:
    from dataloom.sources import Source


class Loom:
    """
    Orquestrador principal do DataLoom.

    Exemplo de uso:
        config = LoomConfig(...)
        loom = Loom(config, processor, sink)
        loom.start()

    Também pode ser usado como context manager, garantindo stop()
    mesmo se o bloco levantar exceção ou for interrompido:
        with Loom(config, processor, sink) as loom:
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
            # Import tardio para evitar dependência circular no import do módulo
            from dataloom.sources import RandomNumPySource

            source = RandomNumPySource(config)
        self.source: "Source" = source

        # Garante nova instância de hooks se não fornecida (evita estado global)
        self.hooks = hooks or LoomHooks()
        self.num_weavers = num_weavers

        self.state = LoomState.PENDING

        # Fila limitada (backpressure): se os Weavers não acompanharem o
        # ritmo do Source, o produtor aguarda em vez de acumular memória.
        # queue_maxsize=0 na config desliga o limite.
        if config.queue_maxsize is not None:
            maxsize = config.queue_maxsize
        else:
            maxsize = num_weavers * 4
        self.task_queue: queue.Queue = queue.Queue(maxsize=maxsize)

        self.stop_event = threading.Event()
        self.weavers: list[Weaver] = []
        self._stop_lock = threading.Lock()
        self._stopped = False

    def __enter__(self) -> "Loom":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        # Garante limpeza completa ao sair do bloco with, inclusive em
        # exceções (como KeyboardInterrupt) que escapem do start().
        # stop() é idempotente, então não há custo se já foi chamado.
        self.stop()

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
                on_error=self.hooks.on_error,
                on_batch_processed=self.hooks.on_batch_processed,
            )
            weaver.start()
            self.weavers.append(weaver)

        try:
            # Consome do Source
            for batch in self.source:
                if self.stop_event.is_set():
                    break
                self._enqueue(batch)
        except Exception as e:
            self.state = LoomState.FAILED
            self.hooks.on_error(e)
            raise
        finally:
            self.stop()

    def _enqueue(self, batch) -> None:
        """
        Coloca um lote na fila sem bloquear indefinidamente: o timeout
        permite reagir a um stop() disparado por outra thread.
        """
        while not self.stop_event.is_set():
            try:
                self.task_queue.put(batch, timeout=0.1)
                return
            except queue.Full:
                continue

    def stop(self) -> None:
        """
        Sinaliza a parada de todos os componentes e aguarda limpeza.
        Seguro para ser chamado múltiplas vezes ou dentro de blocos finally.

        Os itens já enfileirados são processados antes do encerramento:
        cada Weaver consome a fila até encontrar seu sentinela de parada.
        """
        with self._stop_lock:
            if self._stopped:
                return
            self._stopped = True

        self.stop_event.set()

        # Um sentinela por Weaver: cada thread drena a fila e encerra
        # ao consumir o seu. Isso substitui o join() na fila, que podia
        # travar para sempre se um Weaver morresse antes de esvaziá-la.
        for _ in self.weavers:
            self.task_queue.put(STOP_SENTINEL)
        for weaver in self.weavers:
            weaver.join()

        # Não sobrescreve FAILED definido pelo start() em caso de erro
        if self.state is LoomState.RUNNING:
            self.state = LoomState.COMPLETED

        try:
            self.sink.close()
        except Exception as e:
            # Não queremos que erro no close esconda outros erros, mas logamos
            self.hooks.on_error(e)

        self.hooks.on_stop()
