try:
    from importlib.metadata import version, PackageNotFoundError
except ImportError:
    from importlib_metadata import version, PackageNotFoundError

try:
    __version__ = version("numparquet")
except PackageNotFoundError:
    # package is not installed
    pass

from .numparquet import read_numparquet, write_numparquet
