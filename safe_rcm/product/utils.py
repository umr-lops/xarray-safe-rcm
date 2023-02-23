import toolz


def split_marked(mapping, marker="@"):
    groups = toolz.itertoolz.groupby(
        lambda item: item[0].startswith(marker), mapping.items()
    )
    attrs = {key.lstrip(marker): value for key, value in groups[True]}
    data = {key: value for key, value in groups[False]}

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
    funcs = [lambda n: n.removeprefix(ns) for ns in namespaces]

    return toolz.functoolz.pipe(name, *funcs).lstrip(":")
