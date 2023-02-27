import posixpath
from urllib.parse import urlsplit, urlunsplit


def normalize_url_path(url):
    """convert the url's path component to absolute"""
    if url.count("::") > 1:
        # TODO: unlike urllib.parse, `fsspec` allows nested urls
        #       so we need to find a way to support that, as well
        raise ValueError("don't know how to deal with nested urls")

    split = urlsplit(url)
    normalized = posixpath.normpath(split.path)

    return urlunsplit(split._replace(path=normalized))


def dirname(url):
    split = urlsplit(url)
    new_path = posixpath.dirname(split.path)
    return urlunsplit(split._replace(path=new_path))


def join_path(url, path):
    split = urlsplit(url)
    if posixpath.isabs(path):
        joined_path = path
    else:
        joined_path = posixpath.normpath(posixpath.join(split.path, path))

    joined = split._replace(path=joined_path)

    return urlunsplit(joined)
