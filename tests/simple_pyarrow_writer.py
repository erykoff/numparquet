import numpy as np
from math import prod

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    # This is necessary for all pyarrow numpy functionality, unfortunately.
    # import pandas as pd
except ImportError:
    raise ImportError("pyarrow not found for tests")


def numpy_dict_to_arrow_table(numpy_dict):
    """Convert a dictionary of numpy arrays to an arrow table.

    Parameters
    ----------

    Returns
    -------
    """
    type_list = []
    arrays = []

    for name, column in numpy_dict.items():
        dt = column.dtype
        if len(dt.shape) > 0:
            arrow_type = pa.list_(
                pa.from_numpy_dtype(dt.type),
                prod(dt.shape),
            )
        elif dt.type == np.datetime64:
            time_unit = "ns" if "ns" in dt.str else "us"
            # The pa.timestamp() is the correct datatype to round-trip
            # a numpy datetime64[ns] or datetime[us] array.
            arrow_type = pa.timestamp(time_unit)
        else:
            arrow_type = pa.from_numpy_dtype(dt.type)

        type_list.append((name, arrow_type))

        mask = None
        if len(dt.shape) > 0:
            val = np.split(np.asarray(column).ravel(), len(column))
            if isinstance(column, np.ma.MaskedArray):
                mask = np.split(column.mask.ravel(), len(column))
        else:
            val = np.asarray(column)
            if isinstance(column, np.ma.MaskedArray):
                mask = column.mask

        arrays.append(pa.array(val, type=arrow_type, mask=mask))

    schema = pa.schema(type_list)

    arrow_table = pa.Table.from_arrays(arrays, schema=schema)

    return arrow_table


def write_simple_pyarrow_parquet(numpy_dict, fname, row_group_size=None):
    arrow_table = numpy_dict_to_arrow_table(numpy_dict)

    pq.write_table(arrow_table, fname, row_group_size=row_group_size)
