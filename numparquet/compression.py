import numpy as np

from .thrift import parquet_thrift


def decompress_into(codec, in_buffer, out_buffer):
    """
    Decompress from an input buffer to an output buffer, using codecs.

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
        # This may work.
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
