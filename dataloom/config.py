# dataloom/config.py

"""
Gerenciamento de configuração do orquestrador.
Centraliza parâmetros operacionais como diretórios e tamanhos de lote.
"""

from pathlib import Path
from dataclasses import dataclass


@dataclass
class LoomConfig:
    """
    Objeto de configuração principal do DataLoom.

    Args:
        output_dir (Path): Diretório base onde os Sinks padrão salvarão dados.
        batch_size (int): Quantidade de itens gerados por ciclo de processamento.
        interval_seconds (int): Intervalo de tempo entre gerações de tarefas.
    """

    output_dir: Path
    batch_size: int = 10
    interval_seconds: int = 1
