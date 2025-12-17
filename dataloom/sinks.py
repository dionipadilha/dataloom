# dataloom/sinks.py

"""
Contratos de saída de dados (Sinks).
Define como e onde os resultados processados são depositados.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any
from pathlib import Path
import json
import threading


class Sink(ABC):
    """Interface base para destinos de dados."""

    @abstractmethod
    def send(self, result: Dict[str, Any]) -> None:
        """
        Envia o resultado para o destino final.
        Implementações devem garantir thread-safety se acessarem recursos compartilhados.
        """
        pass


class JsonFileSink(Sink):
    """
    Sink padrão que escreve resultados em um arquivo JSON local.
    Utiliza threading.Lock para garantir integridade na escrita concorrente.
    """

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        # Lock garante que apenas um Weaver escreva no arquivo por vez
        self._lock = threading.Lock()

    def send(self, result: Dict[str, Any]) -> None:
        filename = self.output_dir / "results.json"

        with self._lock:
            with open(filename, "a") as f:
                json.dump(result, f)
                f.write("\n")
