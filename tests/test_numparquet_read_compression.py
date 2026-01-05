import numpy as np
import pytest

try:
    from simple_pyarrow_writer import write_simple_pyarrow_parquet
except ImportError:
    write_simple_pyarrow_parquet = None

import numparquet


@pytest.mark.parametrize("compression", ["none", "snappy", "gzip", "brotli", "zstd"])
@pytest.mark.skipif(write_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_basic_dtypes_compression(tmp_path, compression):
    """Test reading with different compression."""
    dtype = np.int32

    arr = np.zeros(10_000, dtype=dtype)
    if dtype == np.bool_:
        arr[0: 1_000] = True
        arr[9_000: 10_000] = True
    else:
        arr[0: 1_000] = 1
        arr[9_000: 10_000] = 1

    data = {"a": arr}

    fname = tmp_path / "basic_small_few.parquet"
    write_simple_pyarrow_parquet(data, fname, compression=compression)

    new_data = numparquet.read_numparquet(fname)

    assert new_data["a"].dtype == data["a"].dtype
    np.testing.assert_array_equal(new_data["a"], data["a"])
