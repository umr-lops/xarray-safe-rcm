import posixpath
from urllib.parse import urljoin, urlsplit, urlunsplit


def split_url(url):
    if url.count("::") != 0:
        # TODO: unlike urllib.parse, `fsspec` allows chaining urls
        #       so we need to find a way to support that, as well
        raise ValueError("don't know how to deal with url chains")

    return urlsplit(url)


def normalize_url_path(url):
    """convert the url's path component to absolute"""
    split = split_url(url)
    normalized = posixpath.normpath(split.path)

    return urlunsplit(split._replace(path=normalized))


def dirname(url):
    split = split_url(url)
    new_path = posixpath.dirname(split.path)
    return urlunsplit(split._replace(path=new_path))


def join_path(url, path):
    return urljoin(url, path)


def split(url):
    split = split_url(url)

    dirname, fname = posixpath.split(split.path)

    return urlunsplit(split._replace(path=dirname)), fname
