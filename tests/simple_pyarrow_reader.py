import numpy as np

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    raise ImportError("pyarrow not found for tests")


def arrow_table_to_numpy_dict(arrow_table):
    """Convert an arrow table to a dictionary of numpy arrays.

    Parameters
    ----------
    arrow_table : `pyarrow.Table`

    Returns
    -------
    numpy_dict : `dict` [`str`, `np.ndarray` or `np.ma.MaskedArray`]
    metadata : `dict` [`str`, `str`]
    """
    schema = arrow_table.schema
    metadata = schema.metadata if schema.metadata is not None else {}

    numpy_dict = {}

    for name in schema.names:
        t = schema.field(name).type

        if arrow_table[name].null_count == 0:
            col = arrow_table[name].to_numpy()
        else:
            if t in (pa.float64(), pa.float32(), pa.float16()):
                null_value = np.nan
            elif t in (p.int64(), pa.int32(), pa.int16(), pa.int8()):
                null_value = -1
            elif t in (pa.bool_(),):
                null_value = True
            elif t in (pa.string(), pa.binary()):
                null_value = ""
            else:
                null_value = 0

            col = np.ma.masked_array(
                data=arrow_table[name].fill_null(null_value).to_numpy(),
                mask=arrow_table[name].is_null().to_numpy(),
                fill_value=null_value,
            )

        if t in (pa.string(), pa.binary()):
            lengths = [len(row) for row in col if row]
            strlen = max(lengths) if lengths else 0
            dtype = f"U{strlen}" if schema.field(name).type == pa.string() else f"|S{strlen}"
            col = col.astype(dtype)
        elif isinstance(t, pa.FixedSizeListType):
            if len(col) > 0:
                col = np.stack(col)
            else:
                col = col.astype(t.value_type.to_pandas_dtype())

        numpy_dict[name] = col

    return numpy_dict, metadata


def read_simple_pyarrow_parquet(fname):
    """
    """
    arrow_table = pq.read_table(fname)

    numpy_dict, metadata = arrow_table_to_numpy_dict(arrow_table)

    return numpy_dict, metadata
