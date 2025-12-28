import numpy as np

from .thrift import check_valid_parquet, read_md_length, read_file_metadata, read_page_header
from .schema import NumparquetSchema
from .compression import decompress_into
from .encoding import NumpyBuffer, decode_data


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

        # print(file_metadata)
        schema = NumparquetSchema(file_metadata)

        # Make the output data dictionary.
        # Skip if not in read list!
        data_dict = {}
        for column in schema.columns:
            if schema.get_null_count(column) > 0:
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
                # Skip if not in read list!
                col_metadata = col_group.meta_data
                codec = col_metadata.codec
                name = col_metadata.path_in_schema[-1]

                native_dtype = schema[name].native_dtype

                dict_offset = col_metadata.dictionary_page_offset
                data_offset = col_metadata.data_page_offset

                has_dictionary_data = False
                if dict_offset is not None:
                    has_dictionary_data = True

                    # We read in the dictionary page.
                    file_buffer.seek(dict_offset)
                    page_header = read_page_header(file_buffer)

                    read_buffer = np.empty(page_header.compressed_page_size, dtype="S1")
                    file_buffer.readinto(read_buffer)

                    # I'm unsure if this should be native_dtype or dtype
                    # This works because the dictionary has PLAIN
                    # encoding.
                    # This needs to be UPDATED because buffer name is BAD.
                    dict_value_buffer = np.empty(
                        page_header.dictionary_page_header.num_values,
                        dtype=native_dtype,
                    )
                    decompress_into(codec, read_buffer, dict_value_buffer)

                # Read the data page and uncompress it.
                file_buffer.seek(data_offset)
                page_header = read_page_header(file_buffer)

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
                has_definition_data = False
                if not schema[name].required:
                    has_definition_data = True

                    # Compute the maximum definition level.
                    # This is here for use in the future.
                    max_definition_level = 0
                    for part in col_metadata.path_in_schema:
                        if not schema[part].required:
                            max_definition_level += 1

                    bit_width = int(np.ceil(np.log2(max_definition_level + 1)))

                    definition_values = decode_data(
                        npbuffer,
                        page_header.data_page_header.definition_level_encoding,
                        page_header.data_page_header.num_values,
                        bit_width=bit_width,
                        read_length=True,
                    )

                # 3. Encoded values.

                if has_definition_data:
                    # Only non-null entries are stored.
                    data_value_count = np.sum(definition_values > 0)
                else:
                    # All entries are stored.
                    data_value_count = row_group_rows

                null_count = row_group_rows - data_value_count

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
                )

                # Fill the output data.
                if has_dictionary_data:
                    if has_definition_data and (null_count > 0):
                        non_null = (definition_values == 1)
                        data_dict[name][rgslice][non_null] = dict_value_buffer[data_values]
                        data_dict[name][rgslice][~non_null] = schema[column].null_value
                        data_dict[name].mask[rgslice][~non_null] = True
                    else:
                        data_dict[name][rgslice] = dict_value_buffer[data_values]

    return data_dict
