# dataloom/hooks.py

"""
Pontos de extensão para observabilidade (Hooks).
Permite injetar lógica de monitoramento sem acoplar ao core do engine.
"""

from abc import ABC


class LoomHooks(ABC):
    """
    Classe base para hooks do ciclo de vida.
    Métodos são opcionais (não abstratos) para facilitar a implementação parcial.
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
