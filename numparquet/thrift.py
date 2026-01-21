import numpy as np
import os
import thriftpy2 as thriftpy
from thriftpy2.utils import serialize
from thriftpy2.protocol.compact import TCompactProtocolFactory
from thriftpy2.http import TFileObjectTransport


thriftfile = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "parquet.thrift")
parquet_thrift = thriftpy.load(thriftfile, module_name="parquet_thrift")


PARQUET_MARKER = b"PAR1"


def check_valid_parquet(file_buffer):
    """Check that the file is valid parquet.

    Returns
    -------
    is_valid : `bool`
    """
    file_buffer.seek(0)

    first = file_buffer.read(4)
    if first != PARQUET_MARKER:
        return False

    file_buffer.seek(-4, 2)
    last = file_buffer.read(4)
    if last != PARQUET_MARKER:
        return False

    return True


def read_md_length(file_buffer):
    """Read the metadata length.

    Returns
    -------
    md_length : `int`
    """
    file_buffer.seek(-8, 2)
    md_length_bytes = file_buffer.read(4)

    return int.from_bytes(md_length_bytes, byteorder="little")


def read_file_metadata(file_buffer, md_length):
    """
    Read the file metadata from an open file handle.

    Parameters
    ----------
    file_buffer : `BufferedReader`
    md_length : `int`

    Returns
    -------
    file_metadata : `parquet_thrift.FileMetaData`
    """
    file_buffer.seek(-(8 + md_length), 2)

    tin = TFileObjectTransport(file_buffer)
    pin = TCompactProtocolFactory().get_protocol(tin)
    file_metadata = parquet_thrift.FileMetaData()
    file_metadata.read(pin)

    return file_metadata


def read_page_header(file_buffer):
    """
    Read a page header from an open file handle.
    The file handle offset is expected to be at the correct place.

    Parameters
    ----------
    file_buffer : `BufferedReader`

    Returns
    -------
    page_header : `parquet_thrift.PageHeader`
    """
    tin = TFileObjectTransport(file_buffer)
    pin = TCompactProtocolFactory().get_protocol(tin)
    page_header = parquet_thrift.PageHeader()
    page_header.read(pin)

    return page_header


def write_marker(file_buffer):
    """
    """
    file_buffer.write(PARQUET_MARKER)


def write_file_metadata(file_buffer, file_metadata):
    """docstring"""
    file_metadata_ser = serialize(file_metadata, proto_factory=TCompactProtocolFactory())
    file_buffer.write(file_metadata_ser)

    # We need to serialize the size of the file metadata as
    # a 4 byte int, little endian.
    sz = np.asarray([len(file_metadata_ser)], dtype=np.int32)
    file_buffer.write(sz)
    write_marker(file_buffer)
