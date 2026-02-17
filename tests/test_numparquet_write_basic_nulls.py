import numpy as np
import pytest

try:
    from simple_pyarrow_reader import read_simple_pyarrow_parquet
except ImportError:
    read_simple_pyarrow_parquet = None

import numparquet


@pytest.mark.parametrize("dtype", [np.int32, np.int64, np.float32, np.float64])
@pytest.mark.parametrize("data_page_version", [1, 2])
@pytest.mark.skipif(read_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_basic_dtypes_small_few_nulls(tmp_path, dtype, data_page_version):
    """Test reading a small table with few unique values, with nulls."""
    arr = np.ma.masked_array(data=np.zeros(10_000, dtype=dtype), mask=np.zeros(10_000, dtype=np.bool_))
    if dtype == np.bool_:
        arr[0: 1_000] = True
        arr[9_000: 10_000] = True
    else:
        arr[0: 1_000] = 1
        arr[9_000: 10_000] = 1

    arr.mask[3_000: 4_000] = True
    arr.mask[3_003: 3_008] = False

    data = {"a": arr}

    fname = tmp_path / "basic_small_few_nulls.parquet"
    numparquet.write_numparquet(fname, data, data_page_version=data_page_version)

    new_data, _ = read_simple_pyarrow_parquet(fname)

    assert new_data["a"].dtype == data["a"].dtype
    assert new_data["a"].mask.sum() == data["a"].mask.sum()

    np.testing.assert_array_equal(new_data["a"], data["a"])
    np.testing.assert_array_equal(new_data["a"].mask, data["a"].mask)


@pytest.mark.parametrize("dtype", [np.int32, np.int64, np.float32, np.float64])
@pytest.mark.parametrize("data_page_version", [1, 2])
@pytest.mark.skipif(read_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_basic_dtypes_small_many_nulls(tmp_path, dtype, data_page_version):
    """Test reading a small table with many unique values, with nulls."""
    arr = np.ma.masked_array(data=np.zeros(10_000, dtype=dtype), mask=np.zeros(10_000, dtype=np.bool_))

    arr.mask[3_000: 4_000] = True
    arr.mask[3_003: 3_008] = False

    data = {"a": arr}

    fname = tmp_path / "basic_small_many_nulls.parquet"
    numparquet.write_numparquet(fname, data, data_page_version=data_page_version)

    new_data, _ = read_simple_pyarrow_parquet(fname)

    assert new_data["a"].dtype == data["a"].dtype
    assert new_data["a"].mask.sum() == data["a"].mask.sum()

    np.testing.assert_array_equal(new_data["a"], data["a"])
    np.testing.assert_array_equal(new_data["a"].mask, data["a"].mask)


@pytest.mark.parametrize("dtype", [np.int32, np.int64, np.float32, np.float64])
@pytest.mark.parametrize("data_page_version", [1, 2])
@pytest.mark.skipif(read_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_basic_dtypes_large_few_nulls(tmp_path, dtype, data_page_version):
    """Test reading a large table with few unique values, with nulls."""
    arr = np.ma.masked_array(data=np.zeros(1_000_002, dtype=dtype), mask=np.zeros(1_000_002, dtype=np.bool_))

    if dtype == np.bool_:
        arr[0: 1_000] = True
        arr[900_000: 900_100] = True
    else:
        arr[0: 1_000] = 1
        arr[900_000: 900_100] = 1

    arr.mask[200_000: 210_000] = True
    arr.mask[200_002: 200_005] = False

    data = {"a": arr}

    fname = tmp_path / "basic_large_few_nulls.parquet"
    numparquet.write_numparquet(fname, data, data_page_version=data_page_version)

    new_data, _ = read_simple_pyarrow_parquet(fname)

    assert new_data["a"].dtype == data["a"].dtype
    assert new_data["a"].mask.sum() == data["a"].mask.sum()

    np.testing.assert_array_equal(new_data["a"], data["a"])
    np.testing.assert_array_equal(new_data["a"].mask, data["a"].mask)


@pytest.mark.parametrize("dtype", [np.int32, np.int64, np.float32, np.float64])
@pytest.mark.parametrize("data_page_version", [1, 2])
@pytest.mark.skipif(read_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_basic_dtypes_large_many_nulls(tmp_path, dtype, data_page_version):
    """Test reading a large table with many unique values, with nulls."""
    arr = np.ma.masked_array(data=np.arange(1_000_002, dtype=dtype), mask=np.zeros(1_000_002, dtype=np.bool_))

    arr.mask[200_000: 210_000] = True
    arr.mask[200_002: 200_005] = False

    data = {"a": arr}

    fname = tmp_path / "basic_large_many_nulls.parquet"
    numparquet.write_numparquet(fname, data, data_page_version=data_page_version)

    new_data, _ = read_simple_pyarrow_parquet(fname)

    assert new_data["a"].dtype == data["a"].dtype
    assert new_data["a"].mask.sum() == data["a"].mask.sum()

    np.testing.assert_array_equal(new_data["a"], data["a"])
    np.testing.assert_array_equal(new_data["a"].mask, data["a"].mask)
