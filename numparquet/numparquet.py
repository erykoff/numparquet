import os
import numpy as np

import pyarrow as pa
from pyarrow import parquet, dataset


def write_numparquet(filename, recarray, clobber=False):
    """
    Write a numpy recarray in parquet format.

    Parameters
    ----------
    filename : `str`
        Output filename.
    recarray : `np.ndarray`
        Numpy recarray to output
    """
    if os.path.exists(filename):
        raise NotImplementedError("No clobbering yet.")

    if not isinstance(recarray, np.ndarray):
        raise ValueError("Input recarray is not a numpy recarray.")

    if recarray.dtype.names is None:
        raise ValueError("Input recarray is not a numpy recarray.")

    columns = recarray.dtype.names

    metadata = {}
    for col in columns:
        # Special-case string types to record the length
        if recarray[col].dtype.type is np.str_:
            metadata[f'recarray::strlen::{col}'] = str(recarray[col].dtype.itemsize//4)

    type_list = [(name, pa.from_numpy_dtype(recarray[name].dtype.type))
                 for name in recarray.dtype.names]
    schema = pa.schema(type_list, metadata=metadata)

    with parquet.ParquetWriter(filename, schema) as writer:
        arrays = [pa.array(recarray[name])
                  for name in recarray.dtype.names]
        pa_table = pa.Table.from_arrays(arrays, schema=schema)

        writer.write_table(pa_table)


def read_numparquet(filename, columns=None, filter=None):
    """
    Read a numpy recarray in parquet format.

    Parameters
    ----------
    filename : `str`
        Input filename
    columns : `list` [`str`], optional
        Name of columns to read.
    filter : `expression thing`, optional
        Pyarrow filter expression to filter rows.

    Returns
    -------
    recarray : `np.ndarray`
    """
    ds = dataset.dataset(filename, format='parquet', partitioning='hive')

    schema = ds.schema

    # Convert from bytes to strings
    md = {key.decode(): schema.metadata[key].decode()
          for key in schema.metadata}

    names = schema.names

    if columns is not None:
        names = [name for name in schema.names
                 if name in columns]

        if names == []:
            # Should this raise instead?
            return np.zeros(0)
    else:
        names = schema.names

    dtype = []
    for name in names:
        if schema.field(name).type == pa.string():
            dtype.append('U%d' % (int(md[f'recarray::strlen::{name}'])))
        else:
            dtype.append(schema.field(name).type.to_pandas_dtype())

    pa_table = ds.to_table(columns=names, filter=None)
    data = np.zeros(pa_table.num_rows, dtype=list(zip(names, dtype)))

    for name in names:
        data[name][:] = pa_table[name].to_numpy()

    return data
