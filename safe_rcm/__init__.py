from importlib.metadata import version

from safe_rcm.api import open_rcm  # noqa: F401

try:
    __version__ = version("xarray-safe-rcm")
except Exception:
    __version__ = "9999"
