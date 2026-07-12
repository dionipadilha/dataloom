# dataloom_engine/hooks.py

"""
Pontos de extensão para observabilidade (Hooks).
Permite injetar lógica de monitoramento sem acoplar ao core do engine.
"""

from typing import Any, Dict


class LoomHooks:
    """
    Classe base para hooks do ciclo de vida.
    Todos os métodos são opcionais e no-op por padrão: sobrescreva apenas
    os pontos de interesse. (Deliberadamente não é um ABC — não há método
    obrigatório a implementar.)
    """

    def on_start(self) -> None:
        """Chamado imediatamente antes dos Weavers serem iniciados."""
        pass

    def on_stop(self) -> None:
        """Chamado após o encerramento gracioso do orquestrador."""
        pass

    def on_error(self, error: Exception) -> None:
        """
        Chamado quando ocorre uma exceção no loop principal do Loom ou
        durante o processamento de um lote em um Weaver (WeaverError).

        Atenção: pode ser invocado a partir de múltiplas threads Weaver
        simultaneamente — implementações devem ser thread-safe.
        """
        pass

    def on_batch_processed(self, result: Dict[str, Any], duration_seconds: float) -> None:
        """
        Chamado após cada lote processado e entregue ao Sink com sucesso.

        Args:
            result: O dicionário produzido pelo Processor para o lote.
            duration_seconds: Tempo gasto em process() + send(), medido
                com relógio monotônico.

        Atenção: assim como on_error, é invocado a partir das threads
        Weaver — implementações devem ser thread-safe e rápidas, pois
        rodam no caminho quente do pipeline.
        """
        pass
