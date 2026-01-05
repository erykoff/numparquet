from setuptools import setup, Extension
import numpy


ext = Extension(
    "numparquet._numparquet",
    [
        "numparquet/numparquet.c",
    ],
)

setup(
    ext_modules=[ext],
    include_dirs=numpy.get_include(),
)
