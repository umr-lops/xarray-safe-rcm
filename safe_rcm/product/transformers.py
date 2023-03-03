import datatree
import toolz
import xarray as xr

from .dicttoolz import keysplit, valsplit
from .predicates import is_array, is_composite_value, is_scalar


def convert_composite(value):
    if not is_composite_value(value):
        raise ValueError(f"not a composite: {value}")

    converted = {part["@dataStream"].lower(): part["$"] for part in value}

    if list(converted) == ["magnitude"]:
        return "magnitude", converted["magnitude"]
    else:
        return "complex", converted["real"] + 1j * converted["imaginary"]


def extract_array(obj, dims):
    # special case for pulses:
    if dims == "pulses" and len(obj) == 1 and isinstance(obj[0], str):
        obj = obj[0].split()
    return xr.Variable(dims, obj)


def extract_composite(obj, dims=()):
    type_, value = convert_composite(obj)

    if is_scalar(value):
        dims = ()
    return xr.Variable(dims, value, {"type": type_})


def extract_variable(obj, dims=()):
    attributes, data = keysplit(lambda k: k.startswith("@"), obj)
    if list(data) != ["$"]:
        raise ValueError("not a variable")

    values = data["$"]
    if is_scalar(values):
        dims = ()

    attrs = toolz.dicttoolz.keymap(lambda k: k.lstrip("@"), attributes)

    return xr.Variable(dims, values, attrs)


def extract_entry(name, obj, dims=None):
    if dims is None:
        dims = [name]

    if is_array(obj):
        # dimension coordinate
        return extract_array(obj, dims=dims)
    elif is_composite_value(obj):
        return extract_composite(obj, dims=dims)
    elif isinstance(obj, dict):
        return extract_variable(obj, dims=dims)
    else:
        raise ValueError(f"unknown datastructure:\n{obj}")


def extract_dataset(obj, dims=()):
    attrs, variables = valsplit(is_scalar, obj)

    vars_ = toolz.dicttoolz.itemmap(
        lambda item: (item[0], extract_entry(*item, dims=dims)),
        variables,
    )
    return xr.Dataset(data_vars=vars_, attrs=attrs)


def extract_nested_variable(obj, dims):
    if is_array(obj):
        return xr.Variable(dims, obj)

    columns = toolz.dicttoolz.merge_with(list, *obj)
    attributes, data = keysplit(lambda k: k.startswith("@"), columns)
    renamed = toolz.dicttoolz.keymap(lambda k: k.lstrip("@"), attributes)
    attrs = toolz.dicttoolz.valmap(toolz.itertoolz.first, renamed)

    return xr.Variable(dims, data["$"], attrs)


def extract_nested_dataset(obj, dims=None):
    if not isinstance(obj, list):
        raise ValueError(f"unknown type: {type(obj)}")

    columns = toolz.dicttoolz.merge_with(list, *obj)
    processed = toolz.dicttoolz.valmap(
        toolz.functoolz.curry(extract_nested_variable)(dims=dims), columns
    )

    return xr.Dataset(processed)


def extract_nested_datatree(obj, dims=None):
    if not isinstance(obj, list):
        raise ValueError(f"unknown type: {type(obj)}")

    datasets = toolz.dicttoolz.merge_with(list, *obj)

    tree = toolz.dicttoolz.valmap(
        toolz.functoolz.curry(extract_nested_dataset)(dims=dims), datasets
    )

    return datatree.DataTree.from_dict(tree)
