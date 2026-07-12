# dataloom/exceptions.py

"""
Exceções personalizadas para o DataLoom.
Permite que consumidores capturem erros específicos da biblioteca
sem depender de exceções genéricas do Python.
"""


class LoomError(Exception):
    """Exceção base para todos os erros do DataLoom."""

    pass


class ConfigurationError(LoomError):
    """Lançado quando há problemas na validação do LoomConfig."""

    pass


class WeaverError(LoomError):
    """Lançado quando um Weaver falha de forma irrecuperável."""

    pass
