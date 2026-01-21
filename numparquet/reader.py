import numpy as np

from .thrift import check_valid_parquet, read_md_length, read_file_metadata, read_page_header, parquet_thrift
from .schema import NumparquetSchema
from .compression import decompress_into
from .decoding import NumpyBuffer, decode_data
from .utilities import (
    make_empty_column,
    update_byte_array_column,
    compute_repetition_length,
    generic_open,
    translate_encoding,
)


# TODO:
#  * Investigate optimizations of string decoding.
#  * variable length lists?

def read_numparquet(filename_or_handle, columns=None, fs=None, return_schema=False):
    """
    Read a numpy dict array thing.

    Parameters
    ----------
    filename_or_handle : `str` or `os.PathLike` or open file handle
        Input filename, path, or open file handle.
    columns : `list` [`str`], optional
        Name of columns to read.
    fs : `fsspec.AbstractFileSystem`, optional
        FSSpec filesystem to use to open the file.
    return_schema : `bool`, optional
        Additionally return the schema with metadata?

    Returns
    -------
    dict_of_arrays : `dict` [`np.ndarray` or `np.ma.maskedarray`]
        Dictionary of numpy arrays, keyed by column name.
    schema : `numparquet.NumparquetSchema`, optional
        Additionally returned if ``return_schema`` is True.
    """
    with generic_open(filename_or_handle, fs=fs) as file_buffer:
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

            for column_chunk in row_group.columns:
                col_metadata = column_chunk.meta_data
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
                column_chunk_index = 0
                while column_chunk_index < col_metadata.num_values:
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
                            translate_encoding(schema, True, page_header.dictionary_page_header.encoding),
                            page_header.dictionary_page_header.num_values,
                            schema_element=schema[name],
                        )

                    # Read the data page and uncompress it.
                    page_header = read_page_header(file_buffer)

                    read_buffer = np.empty(page_header.compressed_page_size, dtype="S1")
                    file_buffer.readinto(read_buffer)
                    data_page_buffer = np.empty(page_header.uncompressed_page_size, dtype="S1")

                    has_v1_header = page_header.data_page_header is not None
                    if has_v1_header:
                        header_v1 = page_header.data_page_header
                        num_values_in_page = header_v1.num_values
                        encoding = header_v1.encoding
                        # For v1 header we read the length from the data.
                        repetition_level_encoding = header_v1.repetition_level_encoding
                        repetition_level_length = None
                        definition_level_encoding = header_v1.definition_level_encoding
                        definition_level_length = None

                        # For v1 header, the full page is compressed together.
                        decompress_into(codec, read_buffer, data_page_buffer)
                    else:
                        header_v2 = page_header.data_page_header_v2
                        num_values_in_page = header_v2.num_values
                        encoding = header_v2.encoding
                        # For v2 header the encoding is always RLE and the length
                        # is in the header.
                        repetition_level_encoding = parquet_thrift.Encoding.RLE
                        repetition_level_length = header_v2.repetition_levels_byte_length
                        definition_level_encoding = parquet_thrift.Encoding.RLE
                        definition_level_length = header_v2.definition_levels_byte_length

                        # For v2 header, compression is optional and partial.
                        if header_v2.is_compressed:
                            # The repetition and definition level data are not
                            # compressed.
                            uncompressed_length = repetition_level_length + definition_level_length
                            np.copyto(
                                data_page_buffer[0: uncompressed_length],
                                read_buffer[0: uncompressed_length],
                                casting="no",
                            )
                            decompress_into(
                                codec,
                                read_buffer[uncompressed_length:],
                                data_page_buffer[uncompressed_length:],
                            )
                        else:
                            # Compression was off for this section; do a straight copy.
                            np.copyto(data_page_buffer, read_buffer, casting="no")

                    # This is a useful container for operating on the decompressed
                    # data.
                    npbuffer = NumpyBuffer(data_page_buffer)

                    # 1. Repetition levels data.
                    if schema[name].max_repetition_level > 0:
                        repetition_values, _ = decode_data(
                            npbuffer,
                            repetition_level_encoding,
                            num_values_in_page,
                            bit_width=schema[name].repetition_level_bit_width,
                            length=repetition_level_length,
                        )
                        repetition_length = compute_repetition_length(repetition_values)
                        if schema[name].list_length < 0:
                            schema[name].list_length = repetition_length
                        elif repetition_length != schema[name].list_length:
                            raise NotImplementedError("Variable length lists not supported.")
                    else:
                        repetition_length = None

                    # 2. Definition levels data.  Only for optional columns.
                    #    This tells which are NULL.
                    if schema[name].nullable:
                        definition_values, _ = decode_data(
                            npbuffer,
                            definition_level_encoding,
                            num_values_in_page,
                            bit_width=schema[name].definition_level_bit_width,
                            length=definition_level_length,
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
                        translate_encoding(schema, False, encoding),
                        data_value_count,
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
                            row_group_index + column_chunk_index,
                        )

                    # Make this a utility?
                    if repetition_length is not None:
                        rgslice = slice(
                            row_group_index * repetition_length,
                            (row_group_index + row_group_rows) * repetition_length,
                        )
                    else:
                        rgslice = slice(row_group_index, row_group_index + row_group_rows)
                    ccslice = slice(column_chunk_index, column_chunk_index + num_values_in_page)
                    if use_dictionary_data:
                        if schema[name].nullable and (null_count > 0):
                            non_null = (definition_values > 0)
                            data_dict[name][rgslice][ccslice][non_null] = dict_values[data_values]
                            data_dict[name][rgslice][ccslice][~non_null] = schema[name].null_value
                            data_dict[name].mask[rgslice][ccslice][~non_null] = True
                        else:
                            data_dict[name][rgslice][ccslice] = dict_values[data_values]
                    else:
                        if schema[name].nullable and (null_count > 0):
                            non_null = (definition_values > 0)
                            data_dict[name][rgslice][ccslice][non_null] = data_values
                            data_dict[name][rgslice][ccslice][~non_null] = schema[name].null_value
                            data_dict[name].mask[rgslice][ccslice][~non_null] = True
                        else:
                            data_dict[name][rgslice][ccslice] = data_values

                    # Increment the counter of number of rows read.
                    column_chunk_index += num_values_in_page
                    # Subsequent pages will not have dictionary data.
                    read_dictionary_data = False

            row_group_index += row_group_rows

    # Reshape any list (2D) arrays.
    for name in data_dict:
        if schema[name].is_list:
            arr = data_dict[name]
            data_dict[name] = arr.reshape((schema.num_rows, arr.size // schema.num_rows))

    if return_schema:
        return data_dict, schema
    else:
        return data_dict


def read_schema(filename_or_handle, fs=None):
    """
    Read a numparquet schema.

    Parameters
    ----------
    filename_or_handle : `str` or `os.PathLike` or open file handle
        Input filename, path, or open file handle.
    fs : `fsspec.AbstractFileSystem`, optional
        FSSpec filesystem to use to open the file.

    Returns
    -------
    schema : `numparquet.NumparquetSchema`
        Schema for the file.
    """
    with generic_open(filename_or_handle, fs=fs) as file_buffer:
        if not check_valid_parquet(file_buffer):
            raise IOError("Not a valid parquet file.")

        md_length = read_md_length(file_buffer)
        file_metadata = read_file_metadata(file_buffer, md_length)

        schema = NumparquetSchema(file_metadata)

    return schema
