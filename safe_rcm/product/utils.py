from tlz.functoolz import flip, pipe
from tlz.itertoolz import first, groupby


def split_marked(mapping, marker="@"):
    groups = groupby(lambda item: item[0].startswith(marker), mapping.items())

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
    funcs = [
        flip(str.removeprefix, ns) for ns in sorted(namespaces, key=len, reverse=True)
    ]
    return pipe(name, *funcs).lstrip(":")


def starcall(func, args, **kwargs):
    return func(*args, **kwargs)


def dictfirst(mapping):
    return first(mapping.values())
