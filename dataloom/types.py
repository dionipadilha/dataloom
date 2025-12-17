# dataloom/types.py

"""
Define os tipos fundamentais e estados do sistema DataLoom.
Estas definições são usadas transversalmente por vários módulos para
garantir consistência de estado.
"""

from enum import Enum


class LoomState(Enum):
    """Representa o estado atual do ciclo de vida da máquina Loom."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
