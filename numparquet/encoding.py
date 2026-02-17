import numpy as np
from ._numparquet import encode_bitpacked_array, encode_rle_array, encode_rle_bitpacked_array  # noqa: F401


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
    if array.dtype == np.bool_:
        packed = encode_bitpacked_array(array, 1)
        return bytearray(packed.data)
    else:
        return bytearray(array.data)
