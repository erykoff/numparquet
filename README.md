# numparquet

A memory-efficient and fast parquet reader, directly into numpy arrays.

`numparquet` is a reader of [Apache Parquet](https://parquet.apache.org) files that is connected directly to [numpy](https://numpy.org) with no [arrow](https://arrow.apache.org) or [pandas](https://pandas.pydata.org) intermediaries.
It is designed to be extremely memory efficient (up to 4x less overhead compared to `pyarrow`), and is also faster to read Parquet files into numpy arrays (single-threaded) due to less memory overhead.

Note that `numparquet` is a single-threaded Parquet file reader, with no implicit multithreading.
This makes it slower for a single user reading a single file on a standalone machine than pyarrow or polars (although it is much more memory efficient).
However, when reading multiple files on a shared or compute cluster environment, implicit multithreading is a terrible feature.

## Features Supported

Currently, `numparquet` supports the following Parquet features:

* Basic datatypes (bool, int32, int64, float32, float64).
* Logical datatypes (int8, uint8, int16, uint16, uint32, uint64, unicode strings, byte strings, datetime64, and float16).
* Fixed length list/array columns (but not yet variable length list/array columns).
* Nulls via numpy masked arrays.
* Column compression with snappy, gzip, brotli, and zstd (via [cramjam](https://docs.rs/cramjam/latest/cramjam/)).
* Data page v1 and v2.
* Plain encoding, dictionary encoding, bit-packed encoding, and byte stream split encoding.

## Requirements

At runtime, `numparquet` requires [numpy](https://numpy.org), [thriftpy2](https://thriftpy2.readthedocs.io), and [cramjam](https://docs.rs/cramjam/latest/cramjam/).

At build time, a working C compiler is also required.

The full suite of tests require [pyarrow](https://arrow.apache.org/docs/python/) as this is used as the reference parquet implementation.
