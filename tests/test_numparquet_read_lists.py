import numpy as np
import pytest

try:
    from simple_pyarrow_writer import write_simple_pyarrow_parquet
except ImportError:
    write_simple_pyarrow_parquet = None

import numparquet


@pytest.mark.parametrize("dtype", [np.bool_, np.int32, np.int64, np.float32, np.float64])
@pytest.mark.parametrize("version", ["1.0", "2.4", "2.6"])
@pytest.mark.skipif(write_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_basic_list_dtypes_small_few(tmp_path, dtype, version):
    """Test reading a small table with few unique values."""
    arr = np.zeros((10_000, 4), dtype=dtype)
    if dtype == np.bool_:
        arr[0: 1_000, :] = True
        arr[9_000: 10_000, :] = True
    else:
        arr[0: 1_000, :] = 1
        arr[9_000: 10_000, :] = 1

    data = {"a": arr}

    fname = tmp_path / "basic_list_small_few.parquet"
    write_simple_pyarrow_parquet(data, fname, version=version)

    new_data = numparquet.read_numparquet(fname)

    assert new_data["a"].dtype == data["a"].dtype
    assert new_data["a"].shape == data["a"].shape
    np.testing.assert_array_equal(new_data["a"], data["a"])


@pytest.mark.parametrize("dtype", [np.int32, np.int64, np.float32, np.float64])
@pytest.mark.skipif(write_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_basic_list_dtypes_small_many(tmp_path, dtype):
    """Test reading a small table with many unique values."""
    arr = np.arange(10_000 * 4, dtype=dtype).reshape((10_000, 4))

    data = {"a": arr}

    fname = tmp_path / "basic_list_small_many.parquet"
    write_simple_pyarrow_parquet(data, fname)

    new_data = numparquet.read_numparquet(fname)

    assert new_data["a"].dtype == data["a"].dtype
    assert new_data["a"].shape == data["a"].shape
    np.testing.assert_array_equal(new_data["a"], data["a"])


@pytest.mark.parametrize("dtype", [np.bool_, np.int32, np.int64, np.float32, np.float64])
@pytest.mark.skipif(write_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_basic_list_dtypes_large_few(tmp_path, dtype):
    """Test reading a large table with few unique values."""
    arr = np.zeros((1_000_002, 6), dtype=dtype)
    if dtype == np.bool_:
        arr[0: 1_000, :] = True
        arr[900_000: 900_100, :] = True
    else:
        arr[0: 1_000, :] = 1
        arr[900_000: 900_100, :] = 1

    data = {"a": arr}

    fname = tmp_path / "basic_list_large_few.parquet"
    write_simple_pyarrow_parquet(data, fname)

    new_data = numparquet.read_numparquet(fname)

    assert new_data["a"].dtype == data["a"].dtype
    assert new_data["a"].shape == data["a"].shape
    np.testing.assert_array_equal(new_data["a"], data["a"])


@pytest.mark.parametrize("dtype", [np.int32, np.int64, np.float32, np.float64])
@pytest.mark.skipif(write_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_basic_list_dtypes_large_many(tmp_path, dtype):
    """Test reading a large table with many unique values."""
    arr = np.arange(1_000_002 * 2, dtype=dtype).reshape((1_000_002, 2))

    data = {"a": arr}

    fname = tmp_path / "basic_list_large_many.parquet"
    write_simple_pyarrow_parquet(data, fname)

    new_data = numparquet.read_numparquet(fname)

    assert new_data["a"].dtype == data["a"].dtype
    assert new_data["a"].shape == data["a"].shape
    np.testing.assert_array_equal(new_data["a"], data["a"])


@pytest.mark.skipif(write_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_basic_multiple_lists(tmp_path):
    arr1 = np.zeros((10_000, 4), dtype=np.int32)
    arr1[0: 1_000, :] = 1
    arr1[9_000: 10_000, :] = 1

    arr2 = np.zeros((10_000, 10), dtype=np.int32)
    arr1[0: 1_000, :] = -1
    arr1[9_000: 10_000, :] = -1

    data = {"a": arr1, "b": arr2}

    fname = tmp_path / "basic_list_multiple.parquet"
    write_simple_pyarrow_parquet(data, fname)

    new_data = numparquet.read_numparquet(fname)

    for column in ["a", "b"]:
        assert new_data[column].dtype == data[column].dtype
        assert new_data[column].shape == data[column].shape
        np.testing.assert_array_equal(new_data[column], data[column])
