import numpy as np

from .thrift import check_valid_parquet, read_md_length, read_file_metadata, read_page_header
from .schema import NumparquetSchema
from .compression import decompress_into
from .encoding import NumpyBuffer, decode_data


# TODO:
#  * test memory usage
#  * test speed
#  * add fixed length byte arrays
#  * derived types (what do they look like)
#  * tests (!)
#  * fsspec/open file handles.


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

        # Make the output data dictionary.
        # Skip if not in read list!
        data_dict = {}
        for column in schema.columns:
            if column not in read_columns:
                continue

            if schema[column].is_byte_array:
                # String/byte columns need special handling.
                data_dict[column] = None
            elif schema.get_null_count(column) > 0:
                data_dict[column] = np.ma.masked_array(
                    data=np.empty(schema.num_rows, dtype=schema[column].dtype),
                    mask=np.zeros(schema.num_rows, dtype=np.bool_),
                    fill_value=schema[column].null_value,
                )
            else:
                data_dict[column] = np.empty(schema.num_rows, dtype=schema[column].dtype)

        # Loop over the row groups.
        row_group_index = 0
        for row_group in file_metadata.row_groups:
            row_group_rows = row_group.num_rows
            rgslice = slice(row_group_index, row_group_index + row_group_rows)

            for col_group in row_group.columns:
                col_metadata = col_group.meta_data
                codec = col_metadata.codec
                name = col_metadata.path_in_schema[-1]

                # Skip if not reading.
                if name not in read_columns:
                    continue

                is_byte_array = schema[name].is_byte_array

                has_definition_data = False
                if not schema[name].required:
                    has_definition_data = True

                    # Compute the maximum definition level.
                    # This is here for use in the future.
                    max_definition_level = 0
                    for part in col_metadata.path_in_schema:
                        if not schema[part].required:
                            max_definition_level += 1

                    definition_level_bit_width = int(np.ceil(np.log2(max_definition_level + 1)))

                # Seek to the start of the data in this column group.
                dict_offset = col_metadata.dictionary_page_offset
                data_offset = col_metadata.data_page_offset
                has_dictionary_data = False
                if dict_offset is None:
                    file_buffer.seek(data_offset)
                else:
                    has_dictionary_data = True
                    file_buffer.seek(dict_offset)

                # Loop until we have read all the data.
                col_group_index = 0
                while col_group_index < row_group_rows:

                    if has_dictionary_data:
                        # Note that only the first page can be a dictionary
                        # page; we will have to reset this at the end of the
                        # loop.

                        page_header = read_page_header(file_buffer)
                        read_buffer = np.empty(page_header.compressed_page_size, dtype="S1")
                        file_buffer.readinto(read_buffer)

                        dict_value_buffer = np.empty(page_header.uncompressed_page_size, dtype="S1")
                        decompress_into(codec, read_buffer, dict_value_buffer)

                        dict_npbuffer = NumpyBuffer(dict_value_buffer)

                        dict_values = decode_data(
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

                    # 1. Repetition levels data.  Currently unsupported.
                    if len(col_metadata.path_in_schema) > 1:
                        raise NotImplementedError("Repetition level not currently supported.")

                    # 2. Definition levels data.  Only for optional columns.
                    #    This tells which are NULL.

                    if has_definition_data:
                        definition_values = decode_data(
                            npbuffer,
                            page_header.data_page_header.definition_level_encoding,
                            num_values_in_page,
                            bit_width=definition_level_bit_width,
                            read_length=True,
                        )

                    # 3. Encoded values.

                    if has_definition_data:
                        # Only non-null entries are stored.
                        data_value_count = np.sum(definition_values > 0)
                    else:
                        # All entries are stored.
                        data_value_count = num_values_in_page

                    null_count = num_values_in_page - data_value_count

                    if has_dictionary_data:
                        bit_width = int(npbuffer.read(1, dtype=np.uint8)[0])
                    else:
                        bit_width = None

                    data_values = decode_data(
                        npbuffer,
                        page_header.data_page_header.encoding,
                        data_value_count,
                        bit_width=bit_width,
                        read_length=False,
                        schema_element=schema[name],
                    )

                    if is_byte_array:
                        if data_dict[name] is None:
                            # First batch of rows with this column.
                            data_dict[name] = np.empty(schema.num_rows, dtype=dict_values.dtype)
                        else:
                            # Subsequent batches of rows with this column.
                            if has_dictionary_data:
                                new_dtype = dict_values.dtype
                            else:
                                new_dtype = data_values.dtype
                            if new_dtype.itemsize > data_dict[name].dtype.itemsize:
                                # The strings got longer; we need to reallocate and
                                # copy over the other data.
                                temp = np.empty(schema.num_rows, dtype=new_dtype)
                                n_copy = row_group_index + col_group_index
                                temp[0: n_copy] = data_dict[name][0: n_copy]
                                data_dict[name] = temp

                    # Fill the output data.
                    cgslice = slice(col_group_index, col_group_index + num_values_in_page)
                    if has_dictionary_data:
                        if has_definition_data and (null_count > 0):
                            non_null = (definition_values > 0)
                            data_dict[name][rgslice][cgslice][non_null] = dict_values[data_values]
                            data_dict[name][rgslice][cgslice][~non_null] = schema[column].null_value
                            data_dict[name].mask[rgslice][cgslice][~non_null] = True
                        else:
                            data_dict[name][rgslice][cgslice] = dict_values[data_values]
                    else:
                        if has_definition_data and (null_count > 0):
                            non_null = (definition_values > 0)
                            data_dict[name][rgslice][cgslice][non_null] = data_values
                            data_dict[name][rgslice][cgslice][~non_null] = schema[column].null_value
                            data_dict[name].mask[rgslice][cgslice][~non_null] = True
                        else:
                            data_dict[name][rgslice][cgslice] = data_values

                    # Increment the counter of number of rows read.
                    col_group_index += num_values_in_page
                    # Subsequent pages will not have dictionary data.
                    has_dictionary_data = False

            row_group_index += row_group_rows

    return data_dict
