# DataLoom Examples

Runnable, self-contained examples — each one maps to a real-world use
case and terminates on its own (finite sources). None of them needs
network access.

| Example | Real-world case | Extra dependencies |
| --- | --- | --- |
| [`api_enrichment.py`](api_enrichment.py) | Enriching records through a rate-limited external API (scoring, geocoding, LLM classification). Shows the parallel speedup, per-batch metrics and a thread-safe `CallbackSink`. | none |
| [`file_pipeline.py`](file_pipeline.py) | Processing files that arrive in a directory (uploads, statements, exports). Shows error resilience: a malformed file is reported via `hooks.on_error` while the rest keep flowing. | none |
| [`sensor_statistics.py`](sensor_statistics.py) | Continuous sensor-style stream with rolling statistics and buffered CSV output (`ThreadedBufferedSink`). | `pip install "dataloom-engine[numpy]"` |

Run any of them from the repository root:

```bash
python examples/api_enrichment.py
python examples/file_pipeline.py
python examples/sensor_statistics.py   # needs the [numpy] extra
```

The examples write their artifacts to local folders (`file_pipeline_output/`,
`sensor_output/`) so you can inspect the results; delete the folders freely.
