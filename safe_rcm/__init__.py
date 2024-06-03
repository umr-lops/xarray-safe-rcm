from importlib.metadata import version

from .api import open_rcm  # noqa: F401

try:
    __version__ = version("xarray-safe-rcm")
except Exception:
    __version__ = "9999"
