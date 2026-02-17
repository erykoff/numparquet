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
    return bytearray(array.data)
