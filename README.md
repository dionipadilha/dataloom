# 🧵 DataLoom

> A lightweight, efficient and safe multi-threaded orchestration engine for Python.

🇧🇷 [Versão em português](README.pt-BR.md)

[![CI](https://github.com/dionipadilha/dataloom/actions/workflows/ci.yml/badge.svg)](https://github.com/dionipadilha/dataloom/actions/workflows/ci.yml) [![PyPI](https://img.shields.io/pypi/v/dataloom-engine)](https://pypi.org/project/dataloom-engine/) [![Python](https://img.shields.io/pypi/pyversions/dataloom-engine)](https://pypi.org/project/dataloom-engine/) ![License](https://img.shields.io/badge/license-MIT-green)

<img width="2752" height="1536" alt="DataLoom: a lightweight, efficient and safe multi-threaded orchestration engine for Python." src="https://github.com/user-attachments/assets/c1f3fc98-5eaf-4add-a38c-2717ae699cd5" />

**DataLoom** is a library designed to process data streams using the **Producer-Consumer** pattern with multiple threads (Weavers). It abstracts away the complexity of queues, locks and lifecycle management, letting you focus purely on your data transformation logic.

## Why DataLoom?
⚡ Performance: real parallel processing for I/O-bound workloads.
🧩 Simplicity: an intuitive API inspired by the weaving metaphor.
🛡️ Safety: thread-safety guaranteed by design across the whole pipeline.
📦 Lightweight: zero-dependency core, ready to run in any Python environment.

### Why not just `ThreadPoolExecutor`?

Fair question — the stdlib solves the *parallelism*, but not the *pipeline*.
`concurrent.futures` gives you a thread pool; everything around it is on you:

| You need...                                       | With the stdlib             | With DataLoom                       |
| -------------------------------------------------- | --------------------------- | ----------------------------------- |
| A continuous producer-consumer flow                 | `Queue` + hand-rolled loops | `Loom` + `Source`                   |
| Backpressure (producer faster than consumers)       | Manual `Queue(maxsize=...)` | Built-in, configurable              |
| Clean shutdown (drain queue, close resources)       | Manual sentinels and joins  | `stop()` / `with Loom(...)`         |
| Workers that survive errors and report them         | try/except in every worker  | Centralized `hooks.on_error`        |
| Per-item processing metrics                         | Manual instrumentation      | `hooks.on_batch_processed`          |
| Safe concurrent file writes                         | Manual locking              | Ready-made sinks (JSON, CSV, callback) |

If your use case is "apply a function to a list and collect the results",
use `ThreadPoolExecutor.map` — it is the right tool. DataLoom is for
**continuous or long-running flows** where lifecycle, resilience and
observability matter. And, like every thread-based solution on CPython,
the parallelism gains apply to **I/O-bound** workloads; for CPU-bound
work, prefer multiprocessing.

## ✨ Features

- **Cohesive API:** aligned concepts (`Loom`, `Weaver`, `Sink`).
- **Concurrent:** automatically manages multiple _Weavers_ (threads) in parallel.
- **Safe:** built-in sinks are thread-safe and exceptions are typed (`LoomError`).
- **Resilient:** processing errors don't kill the Weavers and are reported through hooks.
- **Manageable:** `Loom` is a context manager — `with Loom(...) as loom:` guarantees automatic `stop()`.
- **Backpressure:** the task queue is bounded by default (`queue_maxsize`), preventing unbounded memory growth.
- **Observable:** lifecycle hooks and per-batch metrics (`on_batch_processed` with result and duration), plus built-in logging.

## 📦 Installation

```bash
pip install dataloom-engine
```

The engine core has **zero dependencies**. The demo `RandomNumPySource` and
`StatisticsProcessor` (used in the quick start below) need NumPy, which
ships as an optional extra:

```bash
pip install "dataloom-engine[numpy]"
```

> ⚠️ **Mind the name:** the package installs the `dataloom_engine` module
> (`import dataloom_engine`). Don't confuse it with the `dataloom` package
> on PyPI, which is an ORM by another author and unrelated to this project.

To develop or use the latest version from the repository:

```bash
git clone https://github.com/dionipadilha/dataloom.git
cd dataloom
pip install -e .
```

## 🚀 Quick Start

Here is a complete example of weaving a data pipeline:

```python
from pathlib import Path
import numpy as np
from dataloom_engine import (
    Loom,
    LoomConfig,
    LoomLogs,
    Processor,
    JsonFileSink,
    ThreadedBufferedSink
)
from dataloom_engine.sources import RandomNumPySource

# 1. Define your processing logic (stateless)
class MyFilterProcessor(Processor):
    def process(self, batch: np.ndarray) -> dict:
        # 'batch' is a numpy array sized according to the config
        avg = float(batch.mean())
        return {
            "processed_items": len(batch),
            "average_value": avg,
            "status": "high" if avg > 0.5 else "low"
        }

# 2. Initial setup
if __name__ == "__main__":
    # Configure console logging
    LoomLogs.setup()

    # Engine parameters
    config = LoomConfig(
        output_dir=Path("./data_out"),
        batch_size=100,      # Process 100 items at a time
        interval_seconds=1   # Produce a new batch every second
    )

    # 3. Data source
    # Decoupled from the engine, enabling custom sources (DB, CSV, S3)
    source = RandomNumPySource(config)

    # 4. Buffered file sink
    # ThreadedBufferedSink keeps I/O from blocking the processing
    file_sink = JsonFileSink(config.output_dir)
    sink = ThreadedBufferedSink(file_sink)

    # 5. Initialize the Loom (the orchestrator)
    loom = Loom(
        config=config,
        processor=MyFilterProcessor(),
        sink=sink,
        source=source,
        num_weavers=4  # 4 threads working in parallel
    )

    print("🧵 DataLoom started! Press Ctrl+C to stop.")
    try:
        # The context manager guarantees stop() and resource cleanup,
        # even on exceptions or Ctrl+C
        with loom:
            loom.start()
    except KeyboardInterrupt:
        print("\n🛑 Loom stopped.")
```

More runnable, real-world examples — API enrichment with parallel speedup,
a resilient file-processing pipeline, a buffered sensor stream — live in the
[`examples/`](examples/) directory.

## 🏗️ Architecture

DataLoom is built around a weaving metaphor:

- **Loom:** the main machine. Manages the task queue and the lifecycle.
- **Weaver:** the worker threads. They take the raw material (a batch), process it and deliver it.
- **Processor:** the business logic. Turns raw data into information.
- **Sink:** the final destination. Where the finished product is deposited (e.g. `JsonFileSink`, `CsvFileSink`, or any destination via `CallbackSink`).

## 🛠️ Development and Testing

To contribute to the project or run the test suite:

1.  Install the development dependencies:

    ```bash
    pip install -e ".[dev]"
    ```

2.  Run the test suite (via pytest):
    ```bash
    pytest
    ```

3.  Run the linter and type checker:
    ```bash
    ruff check .
    mypy
    ```

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

## 🗒️ Version History

Changes in each release are documented in the [CHANGELOG](CHANGELOG.md).
