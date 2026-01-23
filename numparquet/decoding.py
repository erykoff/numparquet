import numpy as np

from .thrift import parquet_thrift
from ._numparquet import _decode_bitpacked


class NumpyBuffer:
    """Container class for numpy buffer utilities.

    Parameters
    ----------
    buffer : `np.ndarray`
        Numpy byte array with dtype np.uint8.
    """
    def __init__(self, buffer):
        self._buffer = buffer
        self._index = 0
        self._size = len(buffer)

        # Note: check for overruns

    def read(self, count=-1, dtype=np.uint8):
        """Read bytes from the buffer and increment the index.

        This returns a no-copy view to the underlying buffer.

        Parameters
        ----------
        count : `int`, optional
            Number of elements to read.  -1 means read all.
            Number of bytes read will be width of ``dtype`` times
            the count.
        dtype : `np.dtype`, optional
            Datatype for output data.

        Returns
        -------
        result : `np.ndarray`
            Result array with requested dtype.
        """
        dt = np.dtype(dtype)
        itemsize = dt.itemsize

        if count == -1:
            count = (self._size - self._index) // itemsize

        if dt == np.uint8:
            nbytes = count
            res = self._buffer[self._index: self._index + nbytes]
        else:
            nbytes = int(count * itemsize)
            res = np.frombuffer(self._buffer[self._index: self._index + nbytes], dtype=dtype)

        self._index += int(nbytes)
        return res

    def readinto(self, out):
        """Read bytes from the buffer into a second buffer.

        This returns a copy of the underlying data.

        Parameters
        ----------
        out : `np.ndarray`
            Output buffer, byte type np.uint8.  Number of bytes read will equal
            length of the buffer.
        """
        out_bytes = np.frombuffer(out.data, dtype=np.uint8)
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


def decode_uleb128(npbuffer):
    """Decode an unsigned int from LEB128 encoding.

    See https://en.wikipedia.org/wiki/LEB128

    Parameters
    ----------
    npbuffer : `NumpyBuffer`
        Buffer to read data.

    Returns
    -------
    value : `np.uint64`
        Decoded value.
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


def decode_rle(npbuffer, header, bit_width):
    """Decode from a buffer a run-length-encoding.

    Parameters
    ----------
    npbuffer : `NumpyBuffer`
        Numpy buffer object to read from.
    header : `np.uint64`
        Header value for this chunk.  Count will be header >> 1.
    bit_width : `int`
        Bit width of the packed array.

    Returns
    -------
    count : `int`
        Repeat count.
    value : `np.uint8` or `np.uint16` or `np.uint32` or `np.uint64`
        Value to be repeated. Data type depends on width.
    """
    count = header >> 1
    byte_width = (bit_width + 7) // 8

    data = np.zeros(8, dtype=np.uint8)
    npbuffer.readinto(data[0: byte_width])

    if bit_width == 1:
        # Special case 1-bit because of possible
        # undefined behavior according to arrow.
        value = np.uint8(data[0] & 1)
    else:
        if byte_width == 1:
            dtype = ">u1"
        elif byte_width == 2:
            dtype = ">u2"
        elif byte_width <= 4:
            dtype = ">u4"
        else:
            dtype = ">u8"

        value_buffer = np.frombuffer(data.data, dtype=dtype)
        value = value_buffer[0]

    return count, value


def decode_bitpacked(npbuffer, header, width, boolean=False):
    """Decode from a buffer a set of bit-packed values.

    Parameters
    ----------
    npbuffer : `NumpyBuffer`
        Numpy buffer object to read from.
    header : `np.uint64`
        Header value for this chunk.  Count will be header >> 1.
    width : `int`
        Bit width of packed array.
    boolean : `bool`, optional
        Is this a boolean array?

    Returns
    -------
    values : `np.ndarray`
        Array of values (np.int32 unless boolean=True)
    """
    # Note that zero-padding may produce extra zero values.
    # These should be handled by the calling function.
    num_groups = header >> 1
    count = num_groups * 8
    byte_count = (width * count) // 8

    if width == 0:
        return np.zeros(count, dtype=np.int32)

    raw_bytes = npbuffer.read(byte_count, dtype=np.uint8)

    return _decode_bitpacked(raw_bytes, width, count, boolean=boolean)


def decode_rle_bit_packed_hybrid(npbuffer, width, values, length):
    """Decode RLE/Bit-Packed Hybrid.

    Parameters
    ----------
    npbuffer : `NumpyBuffer`
        Numpy buffer object.
    width : `int`
        Bit width of the packed data.
    values : `np.ndarray`
        Array of output values.
    length : `int`
        Length of the data in the buffer.

    Returns
    -------
    n_read : `int`
        Number of values read.
    """
    if length == 0:
        # There is nothing to read.
        return 0

    n_read = 0
    start_index = npbuffer.index
    while npbuffer.index < (start_index + length):
        header = decode_uleb128(npbuffer)
        if header & 1 == 0:
            count, value = decode_rle(npbuffer, header, width)
            values[n_read: n_read + count] = value
            n_read += count
        else:
            bitpacked_values = decode_bitpacked(npbuffer, header, width)
            stop_index = len(bitpacked_values)
            if (n_read + stop_index) > len(values):
                stop_index = len(values) - int(n_read)

            values[n_read: n_read + stop_index] = bitpacked_values[0: stop_index]

            n_read += len(bitpacked_values)

    return n_read


def decode_byte_stream_split(npbuffer, count, dtype):
    """Decode byte stream split data.

    This encoding creates K byte-streams of length N where K is the size in
    bytes of the data type and N is the number of elements in the data
    sequence.

    The bytes of each value are scattered to the corresponding streams.
    The 0-th byte goes to the 0-th stream, the 1st byte goes to the 1st
    stream and so on. The streams are concatenated in the following order:
    0th stream, 1st stream, etc.

    Parameters
    ----------
    npbuffer : `NumpyBuffer`
        Numpy buffer object.
    count : `int`
        Number of values to read.
    dtype : `np.dtype`
        Datatype of the split values.

    Returns
    -------
    values : `np.ndarray`
        Array of decoded values.
    """
    values = np.empty(count, dtype=dtype)
    n_stream = values.dtype.itemsize

    # value_buffer is a byte-wise view of the values array.
    value_buffer = np.frombuffer(values, dtype=np.uint8)
    # split_values is the byte-wise view of the encoded array.
    split_values = npbuffer.read(count=count * n_stream, dtype=np.uint8)

    # Reconstruct the values from the concatenated streams.
    for i in range(n_stream):
        value_buffer[i::n_stream] = split_values[i * count: (i + 1) * count]

    return values


def decode_data(
    npbuffer,
    encoding,
    num_values,
    bit_width=1,
    length=None,
    schema_element=None,
):
    """Decode data and return an array.

    Parameters
    ----------
    npbuffer : `NumpyBuffer`
        Numpy buffer object.
    encoding : `int`
        Parquet thrift encoding key.
    num_values : `int`
        Number of values expected.
    bit_width : `int`, optional
        Width in bits of any bit packed or RLE/bit-packed values.
    length : `int`, optional
        Length of the data in the buffer. Specified for data page v2
        RLE encoding of definition, repetition, and boolean values.
    schema_element : `NumparquetSchemaElement`, optional
        Schema element.  Used for data decoding but not definition/
        repetition encoding.

    Returns
    -------
    values : `np.ndarray`
        Array of decoded values.
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
                val = bytearray(npbuffer.read(count=length, dtype=np.uint8).data)
                if convert_unicode:
                    values.append(val.decode("UTF-8"))
                else:
                    values.append(bytes(val))

            values = np.asarray(values)
        elif schema_element.dtype == np.bool_:
            # PLAIN encoding for Boolean uses bit-packed.
            count = npbuffer.remaining
            values = decode_bitpacked(npbuffer, count << 1, 1, boolean=True)[0: num_values]
        else:
            # This is a direct translation, and reuses the buffer.
            values = npbuffer.read(count=num_values, dtype=schema_element.native_dtype)
    elif encoding in (
        parquet_thrift.Encoding.RLE,
        parquet_thrift.Encoding.RLE_DICTIONARY,
    ):
        if encoding == parquet_thrift.Encoding.RLE_DICTIONARY:
            # The data page leads with the bit width.
            bit_width = int(npbuffer.read(1, dtype=np.uint8)[0])
            use_dictionary_data = True
            length = npbuffer.remaining
        elif encoding == parquet_thrift.Encoding.RLE:
            if length is None:
                length = npbuffer.read(1, np.int32)[0]

        values = np.zeros(num_values, dtype=np.int32)
        decode_rle_bit_packed_hybrid(npbuffer, bit_width, values, length)

    elif encoding == parquet_thrift.Encoding.BYTE_STREAM_SPLIT:
        values = decode_byte_stream_split(npbuffer, num_values, schema_element.native_dtype)
    else:
        raise NotImplementedError(f"Encoding {encoding} not supported.")

    return values, use_dictionary_data
