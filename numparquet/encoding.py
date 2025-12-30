import numpy as np

from .thrift import parquet_thrift
from ._numparquet import _read_bitpacked


class NumpyBuffer:
    def __init__(self, buffer):
        self._buffer = buffer
        self._index = 0
        self._size = len(buffer)

        # Note: check for overruns

    def read(self, count=-1, dtype=np.dtype("S1")):
        dt = np.dtype(dtype)
        itemsize = dt.itemsize

        if count == -1:
            count = (self._size - self._index) // itemsize

        if dt == np.dtype("S1"):
            nbytes = count
            res = self._buffer[self._index: self._index + nbytes]
        else:
            nbytes = int(count * itemsize)
            res = np.frombuffer(self._buffer[self._index: self._index + nbytes], dtype=dtype)

        self._index += int(nbytes)
        return res

    def readinto(self, out):
        out_bytes = np.frombuffer(out.data, dtype="S1")
        out_bytes[:] = self._buffer[self._index: self._index + out.nbytes]
        self._index += out.nbytes

    @property
    def index(self):
        return self._index

    @property
    def size(self):
        return self._size

    @property
    def remaining(self):
        return self._size - self._index


def read_uleb128(npbuffer):
    """Read an unsigned int from LEB128 encoding.

    Parameters
    ----------
    npbuffer : `NumpyBuffer`

    Returns
    -------
    value : `np.uint16`
    """
    # https://en.wikipedia.org/wiki/LEB128
    result = np.uint64(0)
    shift = 0
    while True:
        byte = npbuffer.read(1, dtype=np.uint8).astype(np.uint64)[0]
        result |= ((byte & 0x7F) << shift)
        shift += 7
        if ((byte & 0x80) == 0):
            break

    return result


def read_rle(npbuffer, header, bit_width):
    """Read from a buffer a run-length-encoding.

    Parameters
    ----------
    npbuffer : `NumpyBuffer`
    header : `np.uint64`
    bit_width : `int`

    Returns
    -------
    count : `int`
        Repeat count.
    value : `np.int32`
        Value to be repeated.
    """
    count = header >> 1
    width = (bit_width + 7) // 8

    data = np.zeros(4, dtype=np.uint8)
    npbuffer.readinto(data[0: width])
    value = data.astype(np.int32)[0]

    return count, value


def read_bitpacked(npbuffer, header, width):
    """Read from a buffer a set of bit-packed values.

    Parameters
    ----------
    npbuffer : `NumpyBuffer`
    header : `np.uint64`
    width : `int`

    Returns
    -------
    values : `np.ndarray`
        Array of values (np.int32)
    """
    num_groups = header >> 1
    count = num_groups * 8
    byte_count = (width * count) // 8

    if width == 0:
        return np.zeros(count, dtype=np.int32)

    raw_bytes = npbuffer.read(byte_count, dtype=np.uint8)

    return _read_bitpacked(raw_bytes, width, count)


def read_rle_bit_packed_hybrid(npbuffer, width, values, index, length=None):
    """Read RLE/Bit-Packed Hybrid.

    Parameters
    ----------
    npbuffer : `NumpyBuffer`
    width : `int`
    values : `np.ndarray`
    index : `int`
    length : `int`, optional

    Returns
    -------
    n_read : `int`
    """
    if length is None:
        length = npbuffer.read(1, np.int32)[0]
        if length == 0:
            return np.zeros(0, dtype=np.int32)
        npbuffer = NumpyBuffer(np.frombuffer(npbuffer.read(length).data, dtype="S1"))

    n_read = 0
    start_index = npbuffer.index
    while npbuffer.index < (start_index + length):
        header = read_uleb128(npbuffer)
        if header & 1 == 0:
            count, value = read_rle(npbuffer, header, width)
            values[index + n_read: index + n_read + count] = value
            n_read += count
        else:
            bitpacked_values = read_bitpacked(npbuffer, header, width)
            stop_index = len(bitpacked_values)
            if (index + n_read + stop_index) > len(values):
                stop_index = len(values) - (index + int(n_read))

            values[index + n_read: index + n_read + stop_index] = bitpacked_values[0: stop_index]

            n_read += len(bitpacked_values)

    return n_read


def decode_data(npbuffer, encoding, num_values, bit_width=None, read_length=False, schema_element=None):
    """Decode data and return an array.

    Parameters
    ----------
    npbuffer : `NumpyBuffer`
    encoding : `int`
    num_values : `int`
    bit_width : `int`, optional
    read_length : `bool`, optional
    schema_element : `NumparquetSchemaElement`, optional

    Returns
    -------
    values : `np.ndarray`
    use_dictionary_data : `bool`
        These data should be used with dictionary data.
    """
    use_dictionary_data = False
    if encoding == parquet_thrift.Encoding.PLAIN:
        if schema_element.is_byte_array:
            # There has to be a better way.
            convert_unicode = (np.dtype(schema_element.dtype).kind == "U")

            values = []
            for index in range(num_values):
                length = npbuffer.read(count=1, dtype=np.int32)[0]
                val = npbuffer.read(count=length, dtype="S1").tobytes()
                if convert_unicode:
                    values.append(val.decode("UTF-8"))
                else:
                    values.append(val)

            values = np.asarray(values)
        else:
            # This is a direct translation, and reuses the buffer.
            values = npbuffer.read(count=num_values, dtype=schema_element.native_dtype)
    elif encoding in (
        parquet_thrift.Encoding.RLE,
        parquet_thrift.Encoding.RLE_DICTIONARY,
        parquet_thrift.Encoding.PLAIN_DICTIONARY,  # For backwards-compatibility
    ):
        if encoding in (parquet_thrift.Encoding.RLE_DICTIONARY, parquet_thrift.Encoding.PLAIN_DICTIONARY):
            # The data page leads with the bit width.
            bit_width = int(npbuffer.read(1, dtype=np.uint8)[0])
            use_dictionary_data = True

        index = 0
        values = np.zeros(num_values, dtype=np.int32)

        if not read_length:
            length = npbuffer.remaining
        else:
            length = None

        while index < num_values:
            n_read = read_rle_bit_packed_hybrid(npbuffer, bit_width, values, index, length=length)
            index += n_read
    else:
        raise NotImplementedError("Only RLE and PLAIN so far.")

    return values, use_dictionary_data
