import numpy as np
import pytest

try:
    from simple_pyarrow_writer import write_simple_pyarrow_parquet
except ImportError:
    write_simple_pyarrow_parquet = None

try:
    from fsspec.implementations.local import LocalFileSystem
except ImportError:
    LocalFileSystem = None

import numparquet


@pytest.mark.skipif(write_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_read_filename_string(tmp_path):
    """Test reading with a string filename."""
    arr = np.zeros(10_000, dtype=np.int32)

    arr[0: 1_000] = 1
    arr[9_000: 10_000] = 1

    data = {"a": arr}

    fname = tmp_path / "basic_small_few.parquet"
    write_simple_pyarrow_parquet(data, fname)

    # Convert the pathlib.Path fname to a string.
    new_data = numparquet.read_numparquet(str(fname))

    assert new_data["a"].dtype == data["a"].dtype
    np.testing.assert_array_equal(new_data["a"], data["a"])


@pytest.mark.skipif(write_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_read_filename_path(tmp_path):
    """Test reading with a pathlib.Path name."""
    arr = np.zeros(10_000, dtype=np.int32)

    arr[0: 1_000] = 1
    arr[9_000: 10_000] = 1

    data = {"a": arr}

    fname = tmp_path / "basic_small_few.parquet"
    write_simple_pyarrow_parquet(data, fname)

    # Note that tmp_path is a pathlib.Path object.
    new_data = numparquet.read_numparquet(fname)

    assert new_data["a"].dtype == data["a"].dtype
    np.testing.assert_array_equal(new_data["a"], data["a"])


@pytest.mark.skipif(write_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_read_filename_open_handle(tmp_path):
    """Test reading with an open file handle."""
    arr = np.zeros(10_000, dtype=np.int32)

    arr[0: 1_000] = 1
    arr[9_000: 10_000] = 1

    data = {"a": arr}

    fname = tmp_path / "basic_small_few.parquet"
    write_simple_pyarrow_parquet(data, fname)

    # Note that tmp_path is a pathlib.Path object.
    with open(fname, "rb") as fh:
        new_data = numparquet.read_numparquet(fh)

    assert new_data["a"].dtype == data["a"].dtype
    np.testing.assert_array_equal(new_data["a"], data["a"])


@pytest.mark.skipif(write_simple_pyarrow_parquet is None, reason="pyarrow not installed")
@pytest.mark.skipif(LocalFileSystem is None, reason="fsspec not installed")
def test_read_filename_fsspec(tmp_path):
    """Test reading with fsspec."""
    arr = np.zeros(10_000, dtype=np.int32)

    arr[0: 1_000] = 1
    arr[9_000: 10_000] = 1

    data = {"a": arr}

    fname = tmp_path / "basic_small_few.parquet"
    write_simple_pyarrow_parquet(data, fname)

    # Convert the pathlib.Path fname to a string.
    new_data = numparquet.read_numparquet(fname, fs=LocalFileSystem(fname))

    assert new_data["a"].dtype == data["a"].dtype
    np.testing.assert_array_equal(new_data["a"], data["a"])
