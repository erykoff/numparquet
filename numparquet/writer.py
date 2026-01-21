import os
import numpy as np

from .thrift import parquet_thrift, write_marker, write_file_metadata
from .encoding import encode_uleb128, encode_plain
from .compression import compress, compression_string_map
from .schema import NumparquetSchema, parquet_schema_from_numpy_dict
from thriftpy2.utils import serialize
from thriftpy2.protocol.compact import TCompactProtocolFactory


PARQUET_MARKER = b"PAR1"


class NumparquetWriter:
    """docstring."""
    def __init__(
        self,
        filename,
        compression="snappy",
        data_page_size=1024,
        column_encoding="auto",
        data_page_version=1,
        overwrite=False,
    ):
        self._filename = filename

        # Make this more flexible.
        if os.path.isfile(filename) and not overwrite:
            raise RuntimeError(f"File {filename} already exists and overwrite=False")

        self._file_buffer = open(filename, "wb")
        write_marker(self._file_buffer)

        self._initialized = False
        self._file_metadata = parquet_thrift.FileMetaData()
        self._file_metadata.version = 2
        self._file_metadata.num_rows = 0
        self._file_metadata.row_groups = []
        self._schema = None

        self._compression = compression
        self._data_page_size = data_page_size
        self._column_encoding = column_encoding
        self._data_page_version = data_page_version

        self._check_row_group_data = True

        self._codec = compression_string_map.get(compression.lower())
        if self._codec is None:
            raise ValueError(f"Unknown compression name: {compression}")

        self._row_groups = []

    def write_data(self, data, row_group_size=1024*1024):
        """
        """

        self._check_data(data)

        if not self._initialized:
            self._file_metadata.schema = parquet_schema_from_numpy_dict(data)
            self._schema = NumparquetSchema(self._file_metadata)

            self._initialized = True
        else:
            raise NotImplementedError("Append!")

        # Split into row groups!
        self._check_row_group_data = False

        # TODO: split
        self.write_row_group(data)

        self._check_row_group_data = True

    def write_row_group(self, row_group_data):
        """
        """
        if self._check_row_group_data:
            self._check_data(row_group_data)

        row_group = parquet_thrift.RowGroup()
        column_chunks = []

        row_group.total_byte_size = 0
        row_group.total_compressed_size = 0

        for name, arr in row_group_data.items():
            row_group.num_rows = len(arr)

            column_chunk = self._write_column_chunk(name, arr)
            row_group.total_byte_size += column_chunk.meta_data.total_uncompressed_size
            row_group.total_compressed_size += column_chunk.meta_data.total_compressed_size

            column_chunks.append(column_chunk)

        row_group.columns = column_chunks

        self._row_groups.append(row_group)

    # Something about set metadata key/values

    @property
    def schema(self):
        return NumparquetSchema(self._file_metadata.schema)

    def close(self):
        """
        """
        self._write_footer()
        self._file_buffer.close()

        self._row_groups = []
        self._file_metadata = None

    def _check_data(self, data):
        """
        """
        # Check that this has all the same length.
        n_rows = None
        for name, arr in data.items():
            if not isinstance(arr, (np.ndarray, np.ma.MaskedArray)):
                raise ValueError(f"Data column {name} is not a numpyarray/masked array")
            if n_rows is None:
                n_rows = len(arr)
            elif len(arr) != n_rows:
                raise ValueError(f"Data column {name} does not have the same length as others!")

        # And check the same types if we are appending!
        if self._initialized:
            raise NotImplementedError("Need to check types!")

    def _write_footer(self):
        """
        """

        # Set final values
        self._file_metadata.row_groups = self._row_groups
        self._file_metadata.num_rows = 0

        for row_group in self._row_groups:
            self._file_metadata.num_rows += row_group.num_rows

        write_file_metadata(self._file_buffer, self._file_metadata)

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    def _write_column_chunk(self, name, array):
        """
        """
        column_chunk = parquet_thrift.ColumnChunk()
        column_metadata = parquet_thrift.ColumnMetaData()

        # TODO: Determine encoding here
        encoding = parquet_thrift.Encoding.PLAIN

        # These include byte size of everything including headers.
        column_metadata.total_uncompressed_size = 0
        column_metadata.total_compressed_size = 0
        # column_metadata.type = schema_element.type
        column_metadata.type = self._schema[name].parquet_type
        # TODO: Add support for dictionary and other encodings and lists.
        column_metadata.encodings = [
            parquet_thrift.Encoding.RLE,
            parquet_thrift.Encoding.PLAIN,
        ]
        # TODO: figure out for lists
        column_metadata.path_in_schema = [name]
        column_metadata.codec = self._codec
        column_metadata.num_values = len(array)
        column_metadata.data_page_offset = self._file_buffer.tell()
        # TODO: Add support for dictionary.
        column_metadata.dictionary_page_offset = None

        data_size_bytes = array.itemsize * len(array)
        n_pages = 1 + data_size_bytes // (self._data_page_size * 1024)
        n_page_rows = len(array) // n_pages

        index = 0
        for page_index in range(n_pages):
            start = index
            stop = len(array) if page_index == (n_pages - 1) else index + n_page_rows

            uncompressed_size, compressed_size = self._write_data_page(array[start: stop], encoding)

            column_metadata.total_uncompressed_size += uncompressed_size
            column_metadata.total_compressed_size += compressed_size

        # Column chunk statistics.
        column_metadata.statistics = self._compute_statistics(array)
        column_chunk.meta_data = column_metadata

        return column_chunk

    def _write_data_page(self, sub_array, encoding):
        """
        """
        page_buffer = bytearray()

        page_header = parquet_thrift.PageHeader()
        page_header.type = parquet_thrift.PageType.DATA_PAGE

        # TODO: support v2
        data_page_header = parquet_thrift.DataPageHeader()
        data_page_header.num_values = len(sub_array)
        data_page_header.encoding = encoding
        data_page_header.definition_level_encoding = parquet_thrift.Encoding.RLE
        data_page_header.repetition_level_encoding = parquet_thrift.Encoding.RLE

        # 1. Store repeatability in page_buffer.
        #      There is no repeatability.

        # 2. Store definitions in page_buffer.

        # TODO: support nulls etc.
        count = len(sub_array)
        header = count << 1
        # width = 1
        header_encoded = encode_uleb128(header)
        # This is the compressing thing ... need to work on it.
        temp = np.zeros(1, dtype=np.int32)
        temp[0] = 1
        rle = np.frombuffer(temp, dtype="S1")[0]
        rle_length = np.array([len(header_encoded) + len(rle)], dtype=np.int32)

        page_buffer.extend(rle_length)
        page_buffer.extend(header_encoded)
        page_buffer.extend(rle)

        # 3. Store values in page_buffer.
        #    TODO: make an encode thing here.
        page_buffer.extend(encode_plain(sub_array))

        # 4. Record uncompressed size.
        page_header.uncompressed_page_size = len(page_buffer)

        # 5. Compress!
        page_buffer_compressed = compress(self._codec, page_buffer)

        # 6. Record compressed size.
        page_header.compressed_page_size = len(page_buffer_compressed)

        # 7. Add page stats.
        data_page_header.statistics = self._compute_statistics(sub_array)

        # 8. Write the header to the file.
        page_header.data_page_header = data_page_header
        page_header_ser = serialize(page_header, proto_factory=TCompactProtocolFactory())
        self._file_buffer.write(page_header_ser)
        self._file_buffer.write(page_buffer_compressed)

        page_uncompressed_size = len(page_header_ser) + len(page_buffer)
        page_compressed_size = len(page_header_ser) + len(page_buffer_compressed)

        return page_uncompressed_size, page_compressed_size

    def _compute_statistics(self, array):
        """
        """
        stats = parquet_thrift.Statistics()

        stats.null_count = 0
        stats.distict_count = len(array)
        stats.min_value = encode_plain(np.asarray([np.nanmin(array)]))
        stats.max_value = encode_plain(np.asarray([np.nanmax(array)]))
        stats.is_min_value_exact = True
        stats.is_max_value_exact = True

        return stats


def write_numparquet(
    filename,
    data,
    compression="snappy",
    data_page_size=1024,
    column_encoding="auto",
    data_page_version=1,
    overwrite=False,
    row_group_size=1024*1024,
):
    """
    """
    with NumparquetWriter(
        filename,
        compression=compression,
        data_page_size=data_page_size,
        column_encoding=column_encoding,
        data_page_version=data_page_version,
        overwrite=overwrite,
    ) as npw:
        npw.write_data(data, row_group_size=row_group_size)
