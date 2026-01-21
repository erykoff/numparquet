import numpy as np


def encode_uleb128(value):
    """Encode an unsigned int using LEB128 encoding.

    See https://en.wikipedia.org/wiki/LEB128

    Parameters
    ----------
    value : `np.uint64`
        Raw value.

    Returns
    -------
    byte_array : `np.ndarray`
        ULEB128 encoded value.
    """
    byte_arr = np.zeros(100, dtype=np.uint8)
    index = 0
    value = np.uint64(value)
    while True:
        byte = value & 0x7F
        value >>= 7
        if value != 0:
            byte |= 0x80
        byte_arr[index] = np.uint8(byte)
        index += 1
        if value == 0:
            break

    return np.frombuffer(byte_arr[0: index], dtype="S1")


def encode_plain(array):
    """Encode an array using PLAIN encoding.

    Parameters
    ----------
    array : `np.ndarray`

    Returns
    -------
    encoded : `np.ndarray`  # NO
    """
    # Need to check about native vs converted types!  Maybe.
    # And boolean special.
    # return np.frombuffer(array, dtype=np.uint8)
    return bytearray(array.data)
