# numparquet

A memory-efficient and fast parquet reader, directly into numpy arrays.

`numparquet` is a reader of [Apache Parquet](https://parquet.apache.org) files that is connected directly to [numpy](https://numpy.org) with no [arrow](https://arrow.apache.org) or [pandas](https://pandas.pydata.org) intermediaries.
It is designed to be extremely memory efficient (up to 4x less overhead compared to `pyarrow`), and is also faster to read parquet files into numpy arrays due to less memory overhead.

## Features Supported

Currently, `numparquet` supports the following Parquet features:

* Basic datatypes (boolean, int32, int64, float32, float64).
* Logical datatypes (int8, uint8, int16, uint16, uint32, uint64, unicode strings, byte strings, datetime64, and float16).
* Fixed length list/array columns (but not yet variable length list/array columns).
* Nulls via numpy masked arrays.
* Column compression with snappy, gzip, brotli, and zstd (via [cramjam](https://docs.rs/cramjam/latest/cramjam/)).

## Requirements

At runtime, `numparquet` requires [numpy](https://numpy.org), [thriftpy2](https://thriftpy2.readthedocs.io), and [cramjam](https://docs.rs/cramjam/latest/cramjam/).

At build time, a working C compiler is also required.

The full suite of tests require [pyarrow](https://arrow.apache.org/docs/python/) as this is used as the reference parquet implementation.
