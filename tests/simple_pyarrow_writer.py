import numpy as np

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    raise ImportError("pyarrow not found for tests")


def numpy_dict_to_arrow_table(numpy_dict, metadata={}):
    """Convert a dictionary of numpy arrays to an arrow table.

    Parameters
    ----------
    numpy_dict : `dict` [`str`, `np.ndarray` or `np.ma.MaskedArray`]
    metadata : `dict` [`str`, `str`], optional

    Returns
    -------
    arrow_table : `pyarrow.Table`
    """
    type_list = []
    arrays = []

    for name, column in numpy_dict.items():
        dt = column.dtype
        if len(column.shape) > 1:
            arrow_type = pa.list_(
                pa.from_numpy_dtype(dt.type),
                column.shape[1],
            )
        elif dt.type == np.datetime64:
            if "ns" in dt.str:
                time_unit = "ns"
            elif "us" in dt.str:
                time_unit = "us"
            elif "ms" in dt.str:
                time_unit = "ms"
            else:
                raise ValueError("Unsupported date type")
            # The pa.timestamp() is the correct datatype to round-trip
            # a numpy datetime64[ns] or datetime[us] array.
            arrow_type = pa.timestamp(time_unit)
        else:
            arrow_type = pa.from_numpy_dtype(dt.type)

        type_list.append((name, arrow_type))

        mask = None
        if len(column.shape) > 1:
            val = np.split(np.asarray(column).ravel(), len(column))
            if isinstance(column, np.ma.MaskedArray):
                raise NotImplementedError("Pyarrow does not support fixed-size lists with nulls")
        else:
            val = np.asarray(column)
            if isinstance(column, np.ma.MaskedArray):
                mask = column.mask

        arrays.append(pa.array(val, type=arrow_type, mask=mask))

    schema = pa.schema(type_list, metadata=metadata)

    arrow_table = pa.Table.from_arrays(arrays, schema=schema)

    return arrow_table


def write_simple_pyarrow_parquet(
    numpy_dict,
    fname,
    version="2.6",
    row_group_size=None,
    metadata={},
    compression="snappy",
):
    arrow_table = numpy_dict_to_arrow_table(numpy_dict, metadata=metadata)

    pq.write_table(
        arrow_table,
        fname,
        row_group_size=row_group_size,
        version=version,
        compression=compression,
    )
