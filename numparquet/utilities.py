import contextlib
import os
import numpy as np


@contextlib.contextmanager
def generic_open(path_or_handle, fs=None):
    if isinstance(path_or_handle, (str, os.PathLike)):
        if fs is None:
            with open(path_or_handle, "rb") as fh:
                yield fh
        else:
            with fs.open(path_or_handle) as fh:
                yield fh
    elif hasattr(path_or_handle, "read"):
        # This is an open handle.
        yield path_or_handle
    else:
        raise ValueError("Illegal path or handle provided.")


def make_empty_column(schema, name, repetition_length=None, byte_array_dtype=None):
    """Make an empty column.

    Parameters
    ----------
    schema : `numparquet.NumparquetSchema`
        Schema information.
    name : `str`
        Name of column.
    repetition_length : `int`, optional
        Repetition length for list columns.
    byte_array_dtype : `np.dtype`, optional
        Datatype to use for byte arrays.

    Returns
    -------
    empty_array : `np.ndarray` or `np.ma.MaskedArray`
        Empty array.  Regular if no nulls; masked array if null.
    """
    dtype = schema[name].dtype
    num_rows = schema.num_rows

    if schema[name].is_byte_array:
        dtype = byte_array_dtype

    if schema[name].is_list:
        # We make the initial array 1D and reshape at the end.
        num_rows *= repetition_length

    if schema.get_null_count(name) > 0:
        return np.ma.masked_array(
            data=np.empty(num_rows, dtype=dtype),
            mask=np.zeros(num_rows, dtype=np.bool_),
            fill_value=schema[name].null_value,
        )
    else:
        return np.empty(num_rows, dtype=dtype)


def update_byte_array_column(input_array, read_dictionary_data, dict_values, data_values, n_copy):
    """Update a byte array or string column.

    Parameters
    ----------
    input_array : `np.ndarray` or `np.ma.MaskedArray`
        Input array; may be returned unchanged.
    read_dictionary_data : `bool`, optional
        Did we read new dictionary data?
    dict_values : `np.ndarray`
        Dictionary value array; may be None.
    data_values : `np.ndarray`
        Data value array; may be None.
    n_copy : `int`
        Number of rows to copy (if necessary).

    Returns
    -------
    new_array : `np.ndarray` or `np.ma.MaskedArray`
        If no changes, will be ``input_array``. Otherwise will
        be resized array with ``input_array`` data copied.
    """
    if read_dictionary_data:
        new_dtype = dict_values.dtype
    else:
        new_dtype = data_values.dtype

    if new_dtype.itemsize > input_array.dtype.itemsize:
        # The strings got longer, so we need to reallocate and copy.
        if isinstance(input_array, np.ma.MaskedArray):
            temp = np.ma.masked_array(
                data=np.empty(len(input_array), dtype=new_dtype),
                mask=np.empty(len(input_array.mask), dtype=np.bool_),
                fill_value=input_array.fill_value,
            )
            temp[0: n_copy] = input_array[0: n_copy]
            temp.mask[0: n_copy] = input_array.mask[0: n_copy]
        else:
            temp = np.empty(len(input_array), dtype=new_dtype)
            temp[0: n_copy] = input_array[0: n_copy]
        return temp
    else:
        return input_array


def compute_repetition_length(repetition_values):
    """Compute repetition size.

    Parameters
    ----------
    repetition_values : `np.ndarray`
        Array of initial indices.

    Returns
    -------
    repetition_length : `int`
        Repetition length.
    """
    zeros, = np.where(repetition_values == 0)
    rep_length = int(zeros[1] - zeros[0])

    # Check that these are aligned.
    if not np.all(zeros % rep_length == 0):
        raise NotImplementedError("Only fixed width lists are supported.")

    return rep_length
