from .api import open_rcm  # noqa: F401

try:
    from importlib import metadata
except ImportError: # for Python<3.8
    import importlib_metadata as metadata
__version__ = metadata.version('xarray-safe-rcm')
