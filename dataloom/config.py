# dataloom/config.py

"""
Gerenciamento de configuração do orquestrador.
Centraliza parâmetros operacionais como diretórios e tamanhos de lote.
"""

from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Union

from dataloom.exceptions import ConfigurationError


@dataclass
class LoomConfig:
    """
    Objeto de configuração principal do DataLoom.

    Args:
        output_dir (Path): Diretório base onde os Sinks padrão salvarão dados.
            Strings são convertidas para Path automaticamente.
        batch_size (int): Quantidade de itens gerados por ciclo de processamento.
        interval_seconds (float): Intervalo de tempo entre gerações de tarefas.
        queue_maxsize (Optional[int]): Capacidade máxima da fila de tarefas
            (backpressure). None usa o padrão do Loom (num_weavers * 4);
            0 significa fila ilimitada.

    Raises:
        ConfigurationError: se algum parâmetro for inválido.
    """

    output_dir: Union[str, Path]
    batch_size: int = 10
    interval_seconds: float = 1.0
    queue_maxsize: Optional[int] = None

    def __post_init__(self) -> None:
        self.output_dir = Path(self.output_dir)

        if self.batch_size <= 0:
            raise ConfigurationError(
                f"batch_size deve ser maior que zero (recebido: {self.batch_size})."
            )
        if self.interval_seconds < 0:
            raise ConfigurationError(
                f"interval_seconds não pode ser negativo (recebido: {self.interval_seconds})."
            )
        if self.queue_maxsize is not None and self.queue_maxsize < 0:
            raise ConfigurationError(
                f"queue_maxsize não pode ser negativo (recebido: {self.queue_maxsize})."
            )
