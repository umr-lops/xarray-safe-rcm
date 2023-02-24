import posixpath
from urllib.parse import urlsplit, urlunsplit

import toolz


def is_scalar(x):
    return not toolz.itertoolz.isiterable(x) or isinstance(x, (str, bytes))


def absolute_url_path(url):
    """convert the url's path component to absolute"""
    if url.count("::") > 1:
        # TODO: unlike urllib.parse, `fsspec` allows nested urls
        #       so we need to find a way to support that, as well
        raise ValueError("don't know how to deal with nested urls")

    split = urlsplit(url)
    absolute_path = posixpath.abspath(split.path)

    return urlunsplit(split._replace(path=absolute_path))


def query(mapping, path):
    if path == "/":
        return mapping

    keys = path.lstrip("/").split("/")
    return toolz.dicttoolz.get_in(keys, mapping, no_default=True)


def valsplit(predicate, d):
    wrapper = lambda item: predicate(item[1])
    groups = toolz.itertoolz.groupby(wrapper, d.items())
    first = dict(groups.get(True, ()))
    second = dict(groups.get(False, ()))

    return first, second


def split_marked(mapping, marker="@"):
    groups = toolz.itertoolz.groupby(
        lambda item: item[0].startswith(marker), mapping.items()
    )

    attrs = {key.lstrip(marker): value for key, value in groups.get(True, {})}
    data = {key: value for key, value in groups.get(False, {})}

    return attrs, data


def strip_namespaces(name, namespaces):
    """remove the given namespaces from a name

    Parameters
    ----------
    name : str
        The string to trim
    namespaces : sequence of str
        The list of namespaces.

    Returns
    -------
    trimmed : str
        The string without prefix and without leading colon.
    """
    funcs = [toolz.functoolz.flip(str.removeprefix, ns) for ns in namespaces]
    return toolz.functoolz.pipe(name, *funcs).lstrip(":")
