# tests/test_config.py

from pathlib import Path

import pytest

from dataloom_engine import ConfigurationError, LoomConfig


def test_config_defaults_are_valid():
    config = LoomConfig(output_dir="./data_out")
    assert config.batch_size == 10
    assert config.interval_seconds == 1.0
    assert config.queue_maxsize is None


def test_config_coerces_output_dir_to_path():
    config = LoomConfig(output_dir="./data_out")
    assert isinstance(config.output_dir, Path)


def test_config_accepts_float_interval():
    config = LoomConfig(output_dir=".", interval_seconds=0.25)
    assert config.interval_seconds == 0.25


@pytest.mark.parametrize("batch_size", [0, -1, -100])
def test_config_rejects_non_positive_batch_size(batch_size):
    with pytest.raises(ConfigurationError):
        LoomConfig(output_dir=".", batch_size=batch_size)


def test_config_rejects_negative_interval():
    with pytest.raises(ConfigurationError):
        LoomConfig(output_dir=".", interval_seconds=-1)


def test_config_rejects_negative_queue_maxsize():
    with pytest.raises(ConfigurationError):
        LoomConfig(output_dir=".", queue_maxsize=-1)


def test_config_accepts_zero_queue_maxsize_as_unbounded():
    config = LoomConfig(output_dir=".", queue_maxsize=0)
    assert config.queue_maxsize == 0
