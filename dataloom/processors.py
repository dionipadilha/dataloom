# dataloom/processors.py

"""
Contratos de processamento de dados.
Define como os lotes (batches) são transformados antes de serem enviados ao Sink.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict

import numpy as np


class Processor(ABC):
    """Interface base para transformação de dados."""

    @abstractmethod
    def process(self, batch: np.ndarray) -> Dict[str, Any]:
        """
        Processa um lote de dados.

        Args:
            batch: Array contendo os dados brutos (tamanho definido em LoomConfig).

        Returns:
            Dict[str, Any]: Dicionário com os resultados processados.
        """
        pass


class StatisticsProcessor(Processor):
    """
    Implementação de referência que calcula estatísticas básicas.
    Útil para testes e validação inicial.
    """

    def process(self, batch: np.ndarray) -> Dict[str, Any]:
        return {
            "mean": float(np.mean(batch)),
            "std": float(np.std(batch)),
            "min": float(np.min(batch)),
            "max": float(np.max(batch)),
        }
