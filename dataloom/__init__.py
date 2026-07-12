# dataloom/__init__.py

"""
DataLoom: Um motor de orquestração multi-thread leve e eficiente.

Este módulo expõe a API pública do pacote. Observe que classes internas
como 'Weaver' não são expostas propositalmente, mantendo a superfície
de uso limpa e segura para o consumidor.
"""

from dataloom.config import LoomConfig
from dataloom.exceptions import ConfigurationError, LoomError, WeaverError
from dataloom.hooks import LoomHooks
from dataloom.logs import LoomLogs
from dataloom.loom import Loom
from dataloom.processors import Processor
from dataloom.sinks import JsonFileSink, Sink, ThreadedBufferedSink
from dataloom.types import LoomState
from dataloom.sources import Source

__all__ = [
    "Loom",
    "LoomConfig",
    "LoomState",
    "Processor",
    "Sink",
    "JsonFileSink",
    "ThreadedBufferedSink",
    "Source",
    "LoomHooks",
    "LoomLogs",
    "LoomError",
    "WeaverError",
    "ConfigurationError",
]
