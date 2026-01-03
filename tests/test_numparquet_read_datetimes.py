import numpy as np
import pytest

try:
    from simple_pyarrow_writer import write_simple_pyarrow_parquet
except ImportError:
    write_simple_pyarrow_parquet = None

import numparquet


@pytest.mark.parametrize("unit", ["us", "ns", "ms"])
@pytest.mark.skipif(write_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_timespans_small_few(tmp_path, unit):
    """Test reading a small table with few unique values."""
    unit = "us"

    arr = np.zeros(10_000, dtype=np.datetime64(1, unit))

    arr[0: 1_000] = np.datetime64("2025-01-01T00:00:00.000000000", unit)
    arr[9_000: 10_000] = np.datetime64("2025-01-01T00:00:00.000000000", unit)

    data = {"a": arr}

    # fname = tmp_path / "datetime_small_few.parquet"
    fname = "blah.parquet"
    write_simple_pyarrow_parquet(data, fname)

    new_data = numparquet.read_numparquet(fname)

    assert new_data["a"].dtype == data["a"].dtype
    np.testing.assert_array_equal(new_data["a"], data["a"])
