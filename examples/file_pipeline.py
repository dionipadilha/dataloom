# examples/file_pipeline.py

"""
Processing files that arrive in a directory — the back-office classic.

Scenario: uploads, bank statements or partner exports land in a folder
and each file must be parsed, validated and summarized. The Source yields
file paths, the Weavers process them in parallel, and a JsonFileSink
records one summary line per file.

The example also demonstrates error resilience: one of the generated
files is deliberately malformed. The pipeline reports it through
hooks.on_error and keeps processing the others — no thread dies, nothing
hangs.

Run it (no extra dependencies needed):
    python examples/file_pipeline.py
"""

import csv
import threading
from pathlib import Path
from typing import Any, Dict, Iterator

from dataloom_engine import JsonFileSink, Loom, LoomConfig, LoomHooks, Processor
from dataloom_engine.sources import Source

BASE_DIR = Path("./file_pipeline_output")
INCOMING_DIR = BASE_DIR / "incoming"


def create_sample_files() -> None:
    """Simulates files arriving from the outside world (one of them broken)."""
    INCOMING_DIR.mkdir(parents=True, exist_ok=True)
    for i in range(4):
        with open(INCOMING_DIR / f"sales_{i}.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["product", "amount"])
            for j in range(10):
                writer.writerow([f"product-{j}", (i + 1) * (j + 1)])
    # A malformed file: non-numeric amount
    (INCOMING_DIR / "sales_broken.csv").write_text("product,amount\nwidget,not-a-number\n")


class DirectorySource(Source):
    """Yields every CSV in the incoming directory (in real use: watch the folder)."""

    def __iter__(self) -> Iterator[Path]:
        yield from sorted(INCOMING_DIR.glob("*.csv"))


class SalesFileProcessor(Processor):
    """Parses one CSV file and produces its summary. Raises on malformed data."""

    def process(self, batch: Any) -> Dict[str, Any]:
        path: Path = batch
        with open(path, newline="") as f:
            rows = list(csv.DictReader(f))
        total = sum(float(row["amount"]) for row in rows)  # raises on bad data
        return {"file": path.name, "rows": len(rows), "total_amount": total}


class ErrorReportingHooks(LoomHooks):
    """Failures land here — in real use: alert, quarantine the file, retry later."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.failures: list = []

    def on_error(self, error: Exception) -> None:
        with self._lock:
            self.failures.append(str(error))


def main() -> None:
    create_sample_files()
    hooks = ErrorReportingHooks()

    config = LoomConfig(output_dir=BASE_DIR, interval_seconds=0)
    with Loom(
        config=config,
        processor=SalesFileProcessor(),
        sink=JsonFileSink(config.output_dir),
        source=DirectorySource(),
        hooks=hooks,
        num_weavers=4,
    ) as loom:
        loom.start()

    print(f"Final state: {loom.state.name}")
    print(f"Summaries written to: {BASE_DIR / 'results.json'}")
    print((BASE_DIR / "results.json").read_text().strip())
    print(f"\nFiles that failed ({len(hooks.failures)}):")
    for failure in hooks.failures:
        print(f"  - {failure}")


if __name__ == "__main__":
    main()
