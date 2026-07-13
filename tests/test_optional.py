# tests/test_optional.py

# Guards the "zero-dependency core" contract: the engine must import and
# run without numpy, and the demo implementations that do need it must
# fail fast with an actionable message.

import sys
from pathlib import Path
from typing import Any, Iterator

import pytest

import dataloom_engine
from dataloom_engine import Loom, LoomConfig, Processor, Sink
from dataloom_engine.processors import StatisticsProcessor
from dataloom_engine.sources import RandomNumPySource, Source

CORE_MODULES = [
    "__init__",
    "_optional",
    "config",
    "exceptions",
    "hooks",
    "logs",
    "loom",
    "sinks",
    "types",
    "weaver",
]


def test_package_exposes_version():
    """__version__ mirrors the installed distribution metadata."""
    assert isinstance(dataloom_engine.__version__, str)
    assert dataloom_engine.__version__


def test_core_modules_do_not_import_numpy():
    """
    No core module may import numpy at module level. Lazy imports inside
    functions (like the _optional helper) are allowed — only unindented
    import statements count as module-level.
    """
    package_dir = Path(dataloom_engine.__file__).parent
    for name in CORE_MODULES:
        for line in (package_dir / f"{name}.py").read_text().splitlines():
            is_top_level_import = line.startswith("import numpy") or line.startswith("from numpy")
            assert not is_top_level_import, f"{name}.py imports numpy at module level"


def test_pipeline_runs_without_numpy():
    """A full pipeline with pure-Python source and processor needs no numpy."""

    class ListSource(Source):
        def __iter__(self) -> Iterator[Any]:
            yield [1, 2, 3]
            yield [4, 5]

    class SumProcessor(Processor):
        def process(self, batch):
            return {"sum": sum(batch)}

    class CollectSink(Sink):
        def __init__(self):
            self.results = []

        def send(self, result):
            self.results.append(result)

    sink = CollectSink()
    # No file sink involved: output_dir can simply be omitted
    config = LoomConfig(batch_size=1, interval_seconds=0)
    with Loom(config=config, processor=SumProcessor(), sink=sink, source=ListSource()) as loom:
        loom.start()

    assert sorted(r["sum"] for r in sink.results) == [6, 9]


def test_demo_classes_fail_fast_without_numpy(monkeypatch):
    """With numpy unavailable, the demo classes raise an actionable ImportError."""
    # Setting a module to None in sys.modules makes 'import numpy' raise ImportError
    monkeypatch.setitem(sys.modules, "numpy", None)

    with pytest.raises(ImportError, match=r"dataloom-engine\[numpy\]"):
        StatisticsProcessor()

    config = LoomConfig(output_dir=".", batch_size=1, interval_seconds=0)
    with pytest.raises(ImportError, match=r"dataloom-engine\[numpy\]"):
        RandomNumPySource(config)
