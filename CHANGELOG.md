# Changelog

All notable changes to this project are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project adheres to [Semantic Versioning](https://semver.org/).

## [0.4.0] - 2026-07-12

### Changed

- **BREAKING:** numpy is no longer a required dependency. The engine core
  has zero third-party requirements; only the demo implementations
  (`RandomNumPySource`, `StatisticsProcessor`) need NumPy, which now ships
  as an optional extra — install with `pip install "dataloom-engine[numpy]"`.
  Those classes fail fast at construction with an actionable ImportError
  when numpy is missing.
- `Processor.process` is now typed as accepting `Any` batch: the engine
  imposes no data type — sources may yield lists, dicts, arrays or any
  other object.
- All code documentation (docstrings, comments) and runtime error
  messages are now in English, matching the primary README and making the
  codebase consistent for international contributors. The Portuguese
  README remains available as `README.pt-BR.md`.

## [0.3.0] - 2026-07-12

### Changed

- **BREAKING:** the package is now distributed as **`dataloom-engine`**
  and the Python module was renamed from `dataloom` to **`dataloom_engine`**
  (`pip install dataloom-engine`; `import dataloom_engine`). Reason: the
  `dataloom` name on PyPI belongs to an ORM by another author; keeping the
  `dataloom` module would collide file-for-file for anyone installing both
  packages, and would induce wrong installs (`pip install dataloom`
  installs the ORM, not this project). The public API is identical — just
  swap the import prefix.
- PyPI publishing workflow via Trusted Publishing (OIDC), triggered by
  publishing a GitHub release.

## [0.2.0] - 2026-07-12

### Fixed

- Weavers no longer die silently when the `Processor` or the `Sink`
  raises: the error is wrapped in `WeaverError`, logged and reported
  through `hooks.on_error`, and the thread keeps processing subsequent
  batches.
- Shutdown (`Loom.stop()`) no longer uses `task_queue.join()`, which
  could block forever if a Weaver exited with items still queued (e.g.
  on a `KeyboardInterrupt`). Each Weaver now drains the queue until it
  consumes its stop sentinel.
- `stop()` is idempotent and no longer overwrites the `FAILED` state
  with `COMPLETED` after a source error.
- `ThreadedBufferedSink` no longer loses items when `close()` is called
  right after `send()` (race between signaling and draining the buffer).
  A target sink failure no longer kills the writer thread.

### Added

- `Loom` works as a context manager: `with Loom(...) as loom:` guarantees
  `stop()` and resource cleanup even on exceptions or `Ctrl+C`.
- Backpressure: the task queue is bounded by default
  (`num_weavers * 4`), configurable via `LoomConfig.queue_maxsize`
  (`0` = unbounded).
- Parameter validation in `LoomConfig` (`__post_init__`), raising
  `ConfigurationError`; `output_dir` accepts `str` and is converted to
  `Path`; `interval_seconds` accepts fractional values.
- Per-batch metrics: `LoomHooks.on_batch_processed(result, duration_seconds)`,
  invoked after each batch is successfully delivered to the Sink.
- New sinks: `CsvFileSink` (thread-safe CSV writing) and `CallbackSink`
  (delegates results to a user callable, with optional `on_close`).
- `send()` after `close()` on `ThreadedBufferedSink` raises `LoomError`
  instead of silently dropping data.
- `WeaverError` and `ConfigurationError` exported in the public API.
- `py.typed` marker: consumers using mypy/pyright can see the library's
  type annotations.

### Changed

- The internal `Weaver` signature changed (takes `on_error` and
  `on_batch_processed` callbacks instead of a `stop_event`). The module
  is internal and not part of the public API.
- CI updated: `actions/checkout@v4`, `setup-python@v5`, install via the
  `[dev]` extras, coverage with `pytest --cov` and Python 3.13 in the
  matrix.

## [0.1.0] - 2026-07-05

### Added

- Initial version: producer-consumer orchestration with multiple threads
  (`Loom`, `Weaver`), `Processor`, `Sink` and `Source` contracts,
  `JsonFileSink`, `ThreadedBufferedSink`, lifecycle hooks (`LoomHooks`),
  logging (`LoomLogs`) and typed exceptions (`LoomError`).

[0.4.0]: https://github.com/dionipadilha/dataloom/releases/tag/v0.4.0
[0.3.0]: https://github.com/dionipadilha/dataloom/releases/tag/v0.3.0
[0.2.0]: https://github.com/dionipadilha/dataloom/releases/tag/v0.2.0
[0.1.0]: https://github.com/dionipadilha/dataloom/releases/tag/v0.1.0
