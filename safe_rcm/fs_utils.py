import posixpath
from urllib.parse import urlsplit, urlunsplit


def absolute_url_path(url):
    """convert the url's path component to absolute"""
    if url.count("::") > 1:
        # TODO: unlike urllib.parse, `fsspec` allows nested urls
        #       so we need to find a way to support that, as well
        raise ValueError("don't know how to deal with nested urls")

    split = urlsplit(url)
    absolute_path = posixpath.abspath(split.path)

    return urlunsplit(split._replace(path=absolute_path))
