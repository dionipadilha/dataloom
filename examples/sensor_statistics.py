# examples/sensor_statistics.py

"""
Continuous sensor-style stream with buffered CSV output.

Scenario: a gateway keeps producing batches of readings (here simulated
by RandomNumPySource) and you want rolling statistics written to disk
without file I/O ever blocking the processing threads — that is what
ThreadedBufferedSink adds on top of any sink.

Requires the optional numpy extra:
    pip install "dataloom-engine[numpy]"

Run it:
    python examples/sensor_statistics.py
"""

from pathlib import Path

from dataloom_engine import (
    CsvFileSink,
    Loom,
    LoomConfig,
    LoomLogs,
    ThreadedBufferedSink,
)
from dataloom_engine.processors import StatisticsProcessor
from dataloom_engine.sources import RandomNumPySource

OUTPUT_DIR = Path("./sensor_output")


def main() -> None:
    LoomLogs.setup()

    config = LoomConfig(
        output_dir=OUTPUT_DIR,
        batch_size=50,  # 50 readings per batch
        interval_seconds=0.1,  # a new batch every 100ms
    )

    # ThreadedBufferedSink decouples disk I/O from the processing threads
    sink = ThreadedBufferedSink(CsvFileSink(config.output_dir))
    source = RandomNumPySource(config, limit=20)  # 20 batches, then stop

    with Loom(
        config=config,
        processor=StatisticsProcessor(),
        sink=sink,
        source=source,
        num_weavers=4,
    ) as loom:
        loom.start()

    csv_path = OUTPUT_DIR / "results.csv"
    lines = csv_path.read_text().strip().splitlines()
    print(f"Final state: {loom.state.name}")
    print(f"{len(lines) - 1} statistic rows written to {csv_path}")
    print("Last row:", lines[-1])


if __name__ == "__main__":
    main()
