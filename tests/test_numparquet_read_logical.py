import numpy as np
import pytest

try:
    from simple_pyarrow_writer import write_simple_pyarrow_parquet
except ImportError:
    write_simple_pyarrow_parquet = None

import numparquet


@pytest.mark.parametrize("dtype", [np.uint64, np.uint32, np.int16, np.uint16, np.int8, np.uint8, np.float16])
@pytest.mark.parametrize("version", ["1.0", "2.4", "2.6"])
@pytest.mark.skipif(write_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_logical_dtypes_small_few(tmp_path, dtype, version):
    """Test reading a small table with few unique values."""
    if version == "1.0" and dtype == np.uint32:
        pytest.skip("Parquet version 1.0 does not support unsigned 32-bit integers")

    arr = np.zeros(10_000, dtype=dtype)

    arr[0: 1_000] = 1
    arr[9_000: 10_000] = 1

    if np.issubdtype(dtype, np.signedinteger) or dtype == np.float16:
        arr[2_000: 3_000] = -1

    data = {"a": arr}

    fname = tmp_path / "logical_small_few.parquet"
    write_simple_pyarrow_parquet(data, fname, version)

    new_data = numparquet.read_numparquet(fname)

    assert new_data["a"].dtype == data["a"].dtype
    np.testing.assert_array_equal(new_data["a"], data["a"])


@pytest.mark.parametrize("dtype", [np.uint64, np.uint32, np.int16, np.uint16, np.int8, np.uint8, np.float16])
@pytest.mark.skipif(write_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_logical_dtypes_small_many(tmp_path, dtype):
    """Test reading a small table with many unique values."""
    arr = np.arange(10_000, dtype=dtype)

    if np.issubdtype(dtype, np.signedinteger):
        arr[2_000] = -1

    data = {"a": arr}

    fname = tmp_path / "logical_small_many.parquet"
    write_simple_pyarrow_parquet(data, fname)

    new_data = numparquet.read_numparquet(fname)

    assert new_data["a"].dtype == data["a"].dtype
    np.testing.assert_array_equal(new_data["a"], data["a"])


@pytest.mark.parametrize("dtype", [np.uint64, np.uint32, np.int16, np.uint16, np.int8, np.uint8, np.float16])
@pytest.mark.skipif(write_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_logical_dtypes_large_few(tmp_path, dtype):
    """Test reading a large table with few unique values."""
    arr = np.zeros(1_000_002, dtype=dtype)

    arr[0: 1_000] = 1
    arr[900_000: 900_100] = 1

    if np.issubdtype(dtype, np.signedinteger) or dtype == np.float16:
        arr[2_000: 3_000] = -1

    data = {"a": arr}

    fname = tmp_path / "logical_large_few.parquet"
    write_simple_pyarrow_parquet(data, fname)

    new_data = numparquet.read_numparquet(fname)

    assert new_data["a"].dtype == data["a"].dtype
    np.testing.assert_array_equal(new_data["a"], data["a"])


@pytest.mark.parametrize("dtype", [np.uint64, np.uint32, np.int16, np.uint16, np.int8, np.uint8])
@pytest.mark.skipif(write_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_logical_dtypes_large_many(tmp_path, dtype):
    """Test reading a large table with many unique values."""
    arr = np.arange(1_000_002, dtype=dtype)

    if np.issubdtype(dtype, np.signedinteger):
        arr[2_000] = -1

    data = {"a": arr}

    fname = tmp_path / "logical_large_many.parquet"
    write_simple_pyarrow_parquet(data, fname)

    new_data = numparquet.read_numparquet(fname)

    assert new_data["a"].dtype == data["a"].dtype
    np.testing.assert_array_equal(new_data["a"], data["a"])
