from importlib.metadata import version

from .api import open_rcm  # noqa: F401

try:
    __version__ = version("safe_rcm")
except Exception:
    __version__ = "999"
