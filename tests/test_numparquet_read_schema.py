import numpy as np
import pytest

try:
    from simple_pyarrow_writer import write_simple_pyarrow_parquet
except ImportError:
    write_simple_pyarrow_parquet = None

import numparquet


@pytest.mark.skipif(write_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_read_table_and_schema(tmp_path):
    """Test reading with a string filename."""
    arr = np.zeros(10_000, dtype=np.int32)

    arr[0: 1_000] = 1
    arr[9_000: 10_000] = 1

    data = {"a": arr, "b": arr}

    metadata = {"md1": "a first string", "md2": "a slightly longer string is here."}

    fname = tmp_path / "schema_testing.parquet"
    write_simple_pyarrow_parquet(data, fname, metadata=metadata)

    new_data, new_schema = numparquet.read_numparquet(fname, return_schema=True)

    assert new_data["a"].dtype == data["a"].dtype
    np.testing.assert_array_equal(new_data["a"], data["a"])
    assert new_data["b"].dtype == data["b"].dtype
    np.testing.assert_array_equal(new_data["b"], data["b"])

    assert len(metadata) == len(new_schema.metadata)
    for key, item in metadata.items():
        assert key in new_schema.metadata
        assert new_schema.metadata[key] == item

    assert new_schema.arrow_schema_encoded is not None

    assert len(new_schema.columns) == 2
    assert new_schema.columns == ["a", "b"]


@pytest.mark.skipif(write_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_read_schema(tmp_path):
    """Test reading with a string filename."""
    arr = np.zeros(10_000, dtype=np.int32)

    arr[0: 1_000] = 1
    arr[9_000: 10_000] = 1

    data = {"a": arr, "b": arr}

    metadata = {"md1": "a first string", "md2": "a slightly longer string is here."}

    fname = tmp_path / "schema_testing.parquet"
    write_simple_pyarrow_parquet(data, fname, metadata=metadata)

    new_schema = numparquet.read_schema(fname)

    assert len(metadata) == len(new_schema.metadata)
    for key, item in metadata.items():
        assert key in new_schema.metadata
        assert new_schema.metadata[key] == item

    assert new_schema.arrow_schema_encoded is not None

    assert len(new_schema.columns) == 2
    assert new_schema.columns == ["a", "b"]


@pytest.mark.parametrize("dtype", [np.bool_, np.int32, np.int64, np.float32, np.float64,
                                   np.uint64, np.uint32, np.int16, np.uint16, np.int8, np.uint8, np.float16,
                                   "S5", "U5"])
@pytest.mark.skipif(write_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_schema_repr(tmp_path, dtype):
    """Test that the schema repr works."""
    data = {"a": np.zeros(100, dtype=dtype)}

    metadata = {"md1": "a first string", "md2": "a slightly longer string is here."}

    fname = tmp_path / "schema_testing.parquet"
    write_simple_pyarrow_parquet(data, fname, metadata=metadata)

    new_schema = numparquet.read_schema(fname)

    _ = repr(new_schema)


@pytest.mark.skipif(write_simple_pyarrow_parquet is None, reason="pyarrow not installed")
def test_list_schema_repr(tmp_path):
    """Test that list schema repr works."""
    data = {"a": np.zeros((100, 10), dtype=np.int32)}

    metadata = {"md1": "a first string", "md2": "a slightly longer string is here."}

    fname = tmp_path / "schema_testing.parquet"
    write_simple_pyarrow_parquet(data, fname, metadata=metadata)

    new_schema = numparquet.read_schema(fname)

    repr1 = repr(new_schema)

    assert "list (unknown length)" in repr1

    _, new_schema2 = numparquet.read_numparquet(fname, return_schema=True)

    repr2 = repr(new_schema2)

    assert "list (10 elements)" in repr2
