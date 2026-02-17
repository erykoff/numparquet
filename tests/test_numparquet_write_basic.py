import numpy as np
import pytest

try:
    from simple_pyarrow_reader import read_simple_pyarrow_parquet
except ImportError:
    write_simple_pyarrow_parquet = None

import numparquet


@pytest.mark.parametrize("dtype", [np.bool_, np.int32, np.int64, np.float32, np.float64])
@pytest.mark.parametrize("data_page_version", [1, 2])
@pytest.mark.skipif(read_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_basic_dtypes_small_few(
    tmp_path,
    dtype,
    data_page_version,
):
    """Test writing a small table with few unique values."""
    arr = np.zeros(10_000, dtype=dtype)
    if dtype == np.bool_:
        arr[1_000: 2_000] = True
        arr[8_000: 9_000] = True
    else:
        arr[1_000: 2_000] = 1
        arr[8_000: 9_000] = 1

    data = {"a": arr}

    fname = tmp_path / "basic_small_few.parquet"
    numparquet.write_numparquet(fname, data, data_page_version=data_page_version)

    new_data, _ = read_simple_pyarrow_parquet(fname)

    assert new_data["a"].dtype == data["a"].dtype
    np.testing.assert_array_equal(new_data["a"], data["a"])


@pytest.mark.parametrize("dtype", [np.int32, np.int64, np.float32, np.float64])
@pytest.mark.parametrize("data_page_version", [1, 2])
@pytest.mark.skipif(read_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_basic_dtypes_small_many(
    tmp_path,
    dtype,
    data_page_version,
):
    """Test writing a small table with many unique values."""
    arr = np.arange(10_000, dtype=dtype)

    data = {"a": arr}

    fname = tmp_path / "basic_small_few.parquet"
    numparquet.write_numparquet(fname, data, data_page_version=data_page_version)

    new_data, _ = read_simple_pyarrow_parquet(fname)

    assert new_data["a"].dtype == data["a"].dtype
    np.testing.assert_array_equal(new_data["a"], data["a"])


@pytest.mark.parametrize("dtype", [np.bool_, np.int32, np.int64, np.float32, np.float64])
@pytest.mark.parametrize("data_page_version", [1, 2])
@pytest.mark.skipif(read_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_basic_dtypes_large_few(
    tmp_path,
    dtype,
    data_page_version,
):
    """Test writing a large table with few unique values."""
    arr = np.zeros(1_000_002, dtype=dtype)
    if dtype == np.bool_:
        arr[0: 1_000] = True
        arr[900_000: 900_100] = True
    else:
        arr[0: 1_000] = 1
        arr[900_000: 900_100] = 1

    data = {"a": arr}

    fname = tmp_path / "basic_small_few.parquet"
    numparquet.write_numparquet(fname, data, data_page_version=data_page_version)

    new_data, _ = read_simple_pyarrow_parquet(fname)

    assert new_data["a"].dtype == data["a"].dtype
    np.testing.assert_array_equal(new_data["a"], data["a"])


@pytest.mark.parametrize("dtype", [np.int32, np.int64, np.float32, np.float64])
@pytest.mark.parametrize("data_page_version", [1, 2])
@pytest.mark.skipif(read_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_basic_dtypes_large_many(
    tmp_path,
    dtype,
    data_page_version,
):
    """Test writing a large table with many unique values."""
    arr = np.arange(1_000_002, dtype=dtype)

    data = {"a": arr}

    fname = tmp_path / "basic_small_few.parquet"
    numparquet.write_numparquet(fname, data, data_page_version=data_page_version)

    new_data, _ = read_simple_pyarrow_parquet(fname)

    assert new_data["a"].dtype == data["a"].dtype
    np.testing.assert_array_equal(new_data["a"], data["a"])
