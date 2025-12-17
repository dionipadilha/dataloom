# dataloom/logs.py

"""
Utilitários de configuração de logging para o DataLoom.
Fornece um namespace estático para configuração rápida.
"""

import logging


class LoomLogs:
    """
    Namespace utilitário para gerenciamento de logs do DataLoom.
    Não deve ser instanciado, apenas usado estaticamente.
    """

    @staticmethod
    def setup(level: int = logging.INFO) -> None:
        """
        Configura o logging básico para stdout.

        Uso:
            LoomLogs.setup(level=logging.DEBUG)
        """
        logging.basicConfig(
            level=level,
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        )
