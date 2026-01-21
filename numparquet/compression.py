import numpy as np

from .thrift import parquet_thrift


def decompress_into(codec, in_buffer, out_buffer):
    """
    Decompress from an input buffer to an output buffer, using codec.

    Parameters
    ----------
    codec : `int`
        Parquet codec code.
    in_buffer : `np.ndarray`
    out_buffer : `np.ndarray`
    """
    if codec == parquet_thrift.CompressionCodec.SNAPPY:
        from cramjam import snappy

        snappy.decompress_raw_into(in_buffer, out_buffer)
    elif codec == parquet_thrift.CompressionCodec.UNCOMPRESSED:
        np.copyto(out_buffer, in_buffer, casting="no")
    elif codec == parquet_thrift.CompressionCodec.GZIP:
        from cramjam import gzip

        gzip.decompress_into(in_buffer, out_buffer)
    elif codec == parquet_thrift.CompressionCodec.BROTLI:
        from cramjam import brotli

        brotli.decompress_into(in_buffer, out_buffer)
    elif codec == parquet_thrift.CompressionCodec.ZSTD:
        from cramjam import zstd

        zstd.decompress_into(in_buffer, out_buffer)
    else:
        raise NotImplementedError("Unsupported compression")


def compress(codec, in_buffer):
    """
    Compress from an input butter, using codec.

    Parameters
    ----------
    codec : `int`
        Parquet codec code.
    in_buffer : `np.ndarray` # NO
    """
    if codec == parquet_thrift.CompressionCodec.SNAPPY:
        from cramjam import snappy

        return snappy.compress_raw(in_buffer)
    elif codec == parquet_thrift.CompressionCodec.UNCOMPRESSED:
        # np.copyto(out_buffer, in_buffer, casting="no")
        # out_buffer[:] = in_buffer
        return in_buffer
    elif codec == parquet_thrift.CompressionCodec.GZIP:
        from cramjam import gzip

        return gzip.compress(in_buffer)
    elif codec == parquet_thrift.CompressionCodec.BROTLI:
        from cramjam import brotli

        return brotli.compress(in_buffer)
    elif codec == parquet_thrift.CompressionCodec.ZSTD:
        from cramjam import zstd

        return zstd.compress(in_buffer)
    else:
        raise NotImplementedError("Unsupported compression")


compression_string_map = {
    "none": parquet_thrift.CompressionCodec.UNCOMPRESSED,
    "snappy": parquet_thrift.CompressionCodec.SNAPPY,
    "gzip": parquet_thrift.CompressionCodec.GZIP,
    "brotli": parquet_thrift.CompressionCodec.BROTLI,
    "zstd": parquet_thrift.CompressionCodec.ZSTD,
}
