# dataloom_engine/__init__.py

"""
DataLoom: Um motor de orquestração multi-thread leve e eficiente.

Este módulo expõe a API pública do pacote. Observe que classes internas
como 'Weaver' não são expostas propositalmente, mantendo a superfície
de uso limpa e segura para o consumidor.
"""

from dataloom_engine.config import LoomConfig
from dataloom_engine.exceptions import ConfigurationError, LoomError, WeaverError
from dataloom_engine.hooks import LoomHooks
from dataloom_engine.logs import LoomLogs
from dataloom_engine.loom import Loom
from dataloom_engine.processors import Processor
from dataloom_engine.sinks import (
    CallbackSink,
    CsvFileSink,
    JsonFileSink,
    Sink,
    ThreadedBufferedSink,
)
from dataloom_engine.sources import Source
from dataloom_engine.types import LoomState

__all__ = [
    "Loom",
    "LoomConfig",
    "LoomState",
    "Processor",
    "Sink",
    "JsonFileSink",
    "CsvFileSink",
    "CallbackSink",
    "ThreadedBufferedSink",
    "Source",
    "LoomHooks",
    "LoomLogs",
    "LoomError",
    "WeaverError",
    "ConfigurationError",
]
