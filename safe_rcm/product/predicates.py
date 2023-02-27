import toolz


def is_scalar(x):
    return not toolz.itertoolz.isiterable(x) or isinstance(x, (str, bytes))


def is_complex(obj):
    if not isinstance(obj, list) or len(obj) != 2:
        return False

    if not all("@dataStream" in el for el in obj):
        return False

    return [el["@dataStream"].lower() for el in obj] == ["real", "imaginary"]
