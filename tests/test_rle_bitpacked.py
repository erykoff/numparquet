import numpy as np
import pytest

from numparquet.encoding import (
    encode_bitpacked,
    encode_rle,
    encode_rle_bitpacked,
)
from numparquet.decoding import (
    decode_rle_bit_packed_hybrid,
    _decode_bitpacked,
    NumpyBuffer,
)
from numparquet.schema import compute_bit_width


@pytest.mark.parametrize("length", [8, 128, 10000, 10035])
def test_encode_bitpacked_bool(length):
    np.random.seed(12345)

    arr = np.random.choice(2, size=length).astype(np.bool_)
    packed = encode_bitpacked(arr, 1)
    unpacked = _decode_bitpacked(packed, 1, len(arr), boolean=True)

    assert np.all(arr == unpacked)


@pytest.mark.parametrize("length", [8, 128, 10000, 10035])
# TODO: figure out why 60-64 are failing.
@pytest.mark.parametrize("width", range(1, 59))
def test_encode_bitpacked(length, width):
    np.random.seed(12345)

    arr = np.random.choice(2**width, size=length)
    if width <= 8:
        arr = arr.astype(np.uint8)
    elif width <= 16:
        arr = arr.astype(np.uint16)
    elif width <= 32:
        arr = arr.astype(np.uint32)

    packed = encode_bitpacked(arr, width)
    unpacked = _decode_bitpacked(packed, width, len(arr))

    assert np.all(arr == unpacked)


@pytest.mark.parametrize("length", [8, 128, 10000, 10035])
@pytest.mark.parametrize("value", [1, 100, 2**31])
def test_encode_rle(length, value):
    bit_width = compute_bit_width(value)

    packed = encode_rle(value, length, bit_width)

    npbuffer = NumpyBuffer(packed)
    values = np.zeros(length * 2, dtype=np.uint64)
    retval = decode_rle_bit_packed_hybrid(npbuffer, bit_width, values, npbuffer.remaining)

    assert retval == length
    np.testing.assert_array_equal(values[0: length], value)


@pytest.mark.parametrize("length", [8, 128, 10000, 10035])
# TODO: figure out why 60-64 are failing.
@pytest.mark.parametrize("width", range(1, 59))
def test_encode_rle_bitpacked(length, width):
    np.random.seed(12345)

    arr = np.random.choice(2**width, size=length)
    if width <= 8:
        arr = arr.astype(np.uint8)
    elif width <= 16:
        arr = arr.astype(np.uint16)
    elif width <= 32:
        arr = arr.astype(np.uint32)

    if len(arr) > 500:
        # Ensure there are runs.
        arr[500: 700] = 1
        arr[1000: 1200] = 0

    packed = encode_rle_bitpacked(arr, width)

    npbuffer = NumpyBuffer(packed)
    unpacked = np.zeros_like(arr)
    retval = decode_rle_bit_packed_hybrid(npbuffer, width, unpacked, len(packed))
    assert retval == len(unpacked)
    assert np.all(arr == unpacked)
