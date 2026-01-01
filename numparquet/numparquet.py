import numpy as np

from .thrift import check_valid_parquet, read_md_length, read_file_metadata, read_page_header
from .schema import NumparquetSchema
from .compression import decompress_into
from .encoding import NumpyBuffer, decode_data
from .utilities import make_empty_column, update_byte_array_column, compute_repetition_length


# TODO:
#  * test memory usage CHECK
#  * test speed CHECK
#  * test all the logical types
#  * tests (!)
#  * fsspec/open file handles.
#  * Was there anything special about S3 in pyarrow?
#  * metadata keys!
#  * rename decode functions.
#  * What do pandas strings look like inside?
#  * Do we need to optimize string decoding?  Probably.
#  * Do nullable list tests ... both single values and whole rows.

def read_numparquet(filename, columns=None):
    """
    Read a numpy dict array thing.

    Parameters
    ----------
    filename : `str`
        Input filename
    columns : `list` [`str`], optional
        Name of columns to read.

    Returns
    -------
    dict_of_arrays : `dict` [`np.ndarray` or `np.ma.maskedarray`]
    """
    # TODO: allow fsspec or input open handle.
    with open(filename, "rb") as file_buffer:
        if not check_valid_parquet(file_buffer):
            raise IOError("Not a valid parquet file.")

        md_length = read_md_length(file_buffer)
        file_metadata = read_file_metadata(file_buffer, md_length)

        schema = NumparquetSchema(file_metadata)

        if columns is None:
            read_columns = schema.columns
        else:
            read_columns = columns

        # Make an empty output data dictionary.
        data_dict = {}

        # Loop over the row groups.
        row_group_index = 0
        for row_group in file_metadata.row_groups:
            row_group_rows = row_group.num_rows

            for col_group in row_group.columns:
                col_metadata = col_group.meta_data
                codec = col_metadata.codec
                schema_element = schema.get_element_from_path(col_metadata.path_in_schema)
                name = schema_element.name

                # Skip if not reading.
                if name not in read_columns:
                    continue

                is_byte_array = schema[name].is_byte_array

                # Seek to the start of the data in this column group.
                dict_offset = col_metadata.dictionary_page_offset
                data_offset = col_metadata.data_page_offset
                read_dictionary_data = False
                if dict_offset is None:
                    file_buffer.seek(data_offset)
                else:
                    read_dictionary_data = True
                    file_buffer.seek(dict_offset)

                dict_values = None

                # Loop until we have read all the data.
                col_group_index = 0
                while col_group_index < row_group_rows:
                    if read_dictionary_data:
                        # Note that only the first page can be a dictionary
                        # page; we will have to reset this at the end of the
                        # loop.

                        page_header = read_page_header(file_buffer)
                        read_buffer = np.empty(page_header.compressed_page_size, dtype="S1")
                        file_buffer.readinto(read_buffer)

                        dict_value_buffer = np.empty(page_header.uncompressed_page_size, dtype="S1")
                        decompress_into(codec, read_buffer, dict_value_buffer)

                        dict_npbuffer = NumpyBuffer(dict_value_buffer)

                        dict_values, _ = decode_data(
                            dict_npbuffer,
                            page_header.dictionary_page_header.encoding,
                            page_header.dictionary_page_header.num_values,
                            schema_element=schema[name],
                        )

                    # Read the data page and uncompress it.
                    page_header = read_page_header(file_buffer)

                    num_values_in_page = page_header.data_page_header.num_values

                    read_buffer = np.empty(page_header.compressed_page_size, dtype="S1")
                    file_buffer.readinto(read_buffer)

                    data_page_buffer = np.empty(page_header.uncompressed_page_size, dtype="S1")
                    decompress_into(codec, read_buffer, data_page_buffer)

                    # This is a useful container for operating on the decompressed
                    # data.
                    npbuffer = NumpyBuffer(data_page_buffer)

                    # 1. Repetition levels data.
                    if schema[name].max_repetition_level > 0:
                        repetition_values, _ = decode_data(
                            npbuffer,
                            page_header.data_page_header.repetition_level_encoding,
                            num_values_in_page,
                            bit_width=schema[name].repetition_level_bit_width,
                            read_length=True,
                        )
                        repetition_length = compute_repetition_length(repetition_values)
                    else:
                        repetition_length = None

                    # 2. Definition levels data.  Only for optional columns.
                    #    This tells which are NULL.
                    if schema[name].nullable:
                        definition_values, _ = decode_data(
                            npbuffer,
                            page_header.data_page_header.definition_level_encoding,
                            num_values_in_page,
                            bit_width=schema[name].definition_level_bit_width,
                            read_length=True,
                        )
                    else:
                        definition_values = None

                    # 3. Encoded values.
                    if schema[name].nullable:
                        # Only non-null entries are stored.
                        data_value_count = np.sum(definition_values > 0)
                    else:
                        # All entries are stored.
                        data_value_count = num_values_in_page

                    null_count = num_values_in_page - data_value_count

                    data_values, use_dictionary_data = decode_data(
                        npbuffer,
                        page_header.data_page_header.encoding,
                        data_value_count,
                        read_length=False,
                        schema_element=schema[name],
                    )

                    # Make empty column if necessary
                    if name not in data_dict:
                        if is_byte_array:
                            if read_dictionary_data:
                                byte_array_dtype = dict_values.dtype
                            else:
                                byte_array_dtype = data_values.dtype
                        else:
                            byte_array_dtype = None

                        data_dict[name] = make_empty_column(
                            schema,
                            name,
                            repetition_length=repetition_length,
                            byte_array_dtype=byte_array_dtype,
                        )
                    elif is_byte_array:
                        # Special handling for possible resizing of byte arrays.
                        data_dict[name] = update_byte_array_column(
                            data_dict[name],
                            read_dictionary_data,
                            dict_values,
                            data_values,
                            row_group_index + col_group_index,
                        )

                    # Make this a utility?
                    if repetition_length is not None:
                        rgslice = slice(
                            row_group_index * repetition_length,
                            (row_group_index + row_group_rows) * repetition_length,
                        )
                    else:
                        rgslice = slice(row_group_index, row_group_index + row_group_rows)
                    cgslice = slice(col_group_index, col_group_index + num_values_in_page)
                    if use_dictionary_data:
                        if schema[name].nullable and (null_count > 0):
                            non_null = (definition_values > 0)
                            data_dict[name][rgslice][cgslice][non_null] = dict_values[data_values]
                            data_dict[name][rgslice][cgslice][~non_null] = schema[name].null_value
                            data_dict[name].mask[rgslice][cgslice][~non_null] = True
                        else:
                            data_dict[name][rgslice][cgslice] = dict_values[data_values]
                    else:
                        if schema[name].nullable and (null_count > 0):
                            non_null = (definition_values > 0)
                            data_dict[name][rgslice][cgslice][non_null] = data_values
                            data_dict[name][rgslice][cgslice][~non_null] = schema[name].null_value
                            data_dict[name].mask[rgslice][cgslice][~non_null] = True
                        else:
                            data_dict[name][rgslice][cgslice] = data_values

                    # Increment the counter of number of rows read.
                    col_group_index += num_values_in_page
                    # Subsequent pages will not have dictionary data.
                    read_dictionary_data = False

            row_group_index += row_group_rows

    # Reshape any list (2D) arrays.
    for name in data_dict:
        if schema[name].is_list:
            arr = data_dict[name]
            data_dict[name] = arr.reshape((schema.num_rows, arr.size // schema.num_rows))

    return data_dict
