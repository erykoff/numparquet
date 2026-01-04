import numpy as np
import pytest

try:
    from simple_pyarrow_writer import write_simple_pyarrow_parquet
except ImportError:
    write_simple_pyarrow_parquet = None

import numparquet


@pytest.mark.parametrize("dtype", [np.bool_, np.int32, np.int64, np.float32, np.float64])
@pytest.mark.parametrize("version", ["1.0", "2.4", "2.6"])
@pytest.mark.parametrize("use_dictionary", [True, False])
@pytest.mark.parametrize("use_byte_stream_split", [False, True])
@pytest.mark.skipif(write_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_basic_dtypes_small_few(tmp_path, dtype, version, use_dictionary, use_byte_stream_split):
    """Test reading a small table with few unique values."""
    if dtype == np.bool_ and use_byte_stream_split:
        pytest.skip("Byte stream split not used with booleans.")

    arr = np.zeros(10_000, dtype=dtype)
    if dtype == np.bool_:
        arr[0: 1_000] = True
        arr[9_000: 10_000] = True
    else:
        arr[0: 1_000] = 1
        arr[9_000: 10_000] = 1

    data = {"a": arr}

    fname = tmp_path / "basic_small_few.parquet"
    write_simple_pyarrow_parquet(
        data,
        fname,
        version=version,
        use_dictionary=use_dictionary,
        use_byte_stream_split=use_byte_stream_split,
    )

    new_data = numparquet.read_numparquet(fname)

    assert new_data["a"].dtype == data["a"].dtype
    np.testing.assert_array_equal(new_data["a"], data["a"])


@pytest.mark.parametrize("dtype", [np.int32, np.int64, np.float32, np.float64])
@pytest.mark.parametrize("version", ["1.0", "2.4", "2.6"])
@pytest.mark.parametrize("use_dictionary", [True, False])
@pytest.mark.parametrize("use_byte_stream_split", [False, True])
@pytest.mark.skipif(write_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_basic_dtypes_small_many(tmp_path, dtype, version, use_dictionary, use_byte_stream_split):
    """Test reading a small table with many unique values."""
    arr = np.arange(10_000, dtype=dtype)

    data = {"a": arr}

    fname = tmp_path / "basic_small_many.parquet"
    write_simple_pyarrow_parquet(
        data,
        fname,
        version=version,
        use_dictionary=use_dictionary,
        use_byte_stream_split=use_byte_stream_split,
    )

    new_data = numparquet.read_numparquet(fname)

    assert new_data["a"].dtype == data["a"].dtype
    np.testing.assert_array_equal(new_data["a"], data["a"])


@pytest.mark.parametrize("dtype", [np.bool_, np.int32, np.int64, np.float32, np.float64])
@pytest.mark.parametrize("version", ["1.0", "2.4", "2.6"])
@pytest.mark.parametrize("use_dictionary", [True, False])
@pytest.mark.parametrize("use_byte_stream_split", [False, True])
@pytest.mark.skipif(write_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_basic_dtypes_large_few(tmp_path, dtype, version, use_dictionary, use_byte_stream_split):
    """Test reading a large table with few unique values."""
    if dtype == np.bool_ and use_byte_stream_split:
        pytest.skip("Byte stream split not used with booleans.")

    arr = np.zeros(1_000_002, dtype=dtype)
    if dtype == np.bool_:
        arr[0: 1_000] = True
        arr[900_000: 900_100] = True
    else:
        arr[0: 1_000] = 1
        arr[900_000: 900_100] = 1

    data = {"a": arr}

    fname = tmp_path / "basic_large_few.parquet"
    write_simple_pyarrow_parquet(
        data,
        fname,
        version=version,
        use_dictionary=use_dictionary,
        use_byte_stream_split=use_byte_stream_split,
    )

    new_data = numparquet.read_numparquet(fname)

    assert new_data["a"].dtype == data["a"].dtype
    np.testing.assert_array_equal(new_data["a"], data["a"])


@pytest.mark.parametrize("dtype", [np.int32, np.int64, np.float32, np.float64])
@pytest.mark.parametrize("version", ["1.0", "2.4", "2.6"])
@pytest.mark.parametrize("use_dictionary", [True, False])
@pytest.mark.parametrize("use_byte_stream_split", [False, True])
@pytest.mark.skipif(write_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_basic_dtypes_large_many(tmp_path, dtype, version, use_dictionary, use_byte_stream_split):
    """Test reading a large table with many unique values."""
    arr = np.arange(1_000_002, dtype=dtype)

    data = {"a": arr}

    fname = tmp_path / "basic_large_many.parquet"
    write_simple_pyarrow_parquet(
        data,
        fname,
        version=version,
        use_dictionary=use_dictionary,
        use_byte_stream_split=use_byte_stream_split,
    )

    new_data = numparquet.read_numparquet(fname)

    assert new_data["a"].dtype == data["a"].dtype
    np.testing.assert_array_equal(new_data["a"], data["a"])
