import numpy as np
import pytest
import string

try:
    from simple_pyarrow_writer import write_simple_pyarrow_parquet
except ImportError:
    write_simple_pyarrow_parquet = None

import numparquet


@pytest.mark.parametrize("dtype", ["S5", "U5"])
@pytest.mark.skipif(write_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_string_dtypes_small_few(tmp_path, dtype):
    """Test reading a small table with few unique values."""
    arr = np.zeros(10_000, dtype=dtype)
    arr[0: 1_000] = "ABCD"
    arr[9_000: 10_000] = "ABCDE"

    data = {"a": arr}

    fname = tmp_path / "string_small_few.parquet"
    write_simple_pyarrow_parquet(data, fname)

    new_data = numparquet.read_numparquet(fname)

    assert new_data["a"].dtype == data["a"].dtype
    np.testing.assert_array_equal(new_data["a"], data["a"])


@pytest.mark.parametrize("dtype", ["S5", "U5"])
@pytest.mark.skipif(write_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_string_dtypes_small_many(tmp_path, dtype):
    """Test reading a small table with many unique values."""
    arr = np.zeros(10_000, dtype=dtype)

    alphabet = string.ascii_letters + string.digits

    np_alphabet = np.array(list(alphabet), dtype="U1")

    np.random.seed(12345)
    random_chars = np.random.choice(np_alphabet, size=(len(arr), 4), replace=True)
    random_strings = ["".join(chars.astype(str)) for chars in random_chars]
    arr[:] = random_strings
    arr[9_000] = "12345"

    data = {"a": arr}

    fname = tmp_path / "string_small_many.parquet"
    write_simple_pyarrow_parquet(data, fname)

    new_data = numparquet.read_numparquet(fname)

    assert new_data["a"].dtype == data["a"].dtype
    np.testing.assert_array_equal(new_data["a"], data["a"])


@pytest.mark.parametrize("dtype", ["S5", "U5"])
@pytest.mark.skipif(write_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_string_dtypes_large_few(tmp_path, dtype):
    """Test reading a large table with few unique values."""
    arr = np.zeros(1_000_002, dtype=dtype)

    arr[0: 1_000] = "ABCD"
    arr[900_000: 900_100] = "ABCDE"

    data = {"a": arr}

    fname = tmp_path / "string_large_few.parquet"
    write_simple_pyarrow_parquet(data, fname)

    new_data = numparquet.read_numparquet(fname)

    assert new_data["a"].dtype == data["a"].dtype
    np.testing.assert_array_equal(new_data["a"], data["a"])


@pytest.mark.parametrize("dtype", ["S5", "U5"])
@pytest.mark.skipif(write_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_string_dtypes_large_many(tmp_path, dtype):
    """Test reading a large table with many unique values."""
    arr = np.zeros(1_000_002, dtype=dtype)

    alphabet = string.ascii_letters + string.digits

    np_alphabet = np.array(list(alphabet), dtype="U1")

    np.random.seed(12345)
    random_chars = np.random.choice(np_alphabet, size=(len(arr), 4), replace=True)
    random_strings = ["".join(chars.astype(str)) for chars in random_chars]
    arr[:] = random_strings
    arr[900_000] = "12345"

    data = {"a": arr}

    fname = tmp_path / "string_large_many.parquet"
    write_simple_pyarrow_parquet(data, fname)

    new_data = numparquet.read_numparquet(fname)

    assert new_data["a"].dtype == data["a"].dtype
    np.testing.assert_array_equal(new_data["a"], data["a"])
