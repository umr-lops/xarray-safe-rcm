import numpy as np
import xarray as xr
from tlz.dicttoolz import (
    itemfilter,
    itemmap,
    keyfilter,
    keymap,
    merge_with,
    valfilter,
    valmap,
)
from tlz.functoolz import compose_left, curry, flip
from tlz.itertoolz import concat, first, second

from safe_rcm.product.dicttoolz import first_values, keysplit, valsplit
from safe_rcm.product.predicates import (
    is_array,
    is_attr,
    is_composite_value,
    is_nested_array,
    is_nested_dataset,
    is_scalar,
)

ignore = ("@xmlns", "@xmlns:xsi", "@xsi:schemaLocation")


def convert_composite(value):
    if not is_composite_value(value):
        raise ValueError(f"not a composite: {value}")

    converted = {part["@dataStream"].lower(): np.array(part["$"]) for part in value}

    if list(converted) == ["magnitude"]:
        return "magnitude", converted["magnitude"]
    else:
        return "complex", converted["real"] + 1j * converted["imaginary"]


def extract_metadata(
    mapping,
    collapse=(),
    ignore=ignore,
):
    without_ignores = keyfilter(lambda k: k not in ignore, mapping)
    # extract the metadata
    metadata_ = itemfilter(
        lambda it: it[0].startswith("@") or is_scalar(it[1]),
        without_ignores,
    )
    metadata = keymap(flip(str.lstrip, "@"), metadata_)

    # collapse the selected items
    to_collapse = keyfilter(lambda x: x in collapse, mapping)
    collapsed = dict(concat(v.items() for v in to_collapse.values()))

    attrs = metadata | collapsed
    return xr.Dataset(attrs=attrs)  # return dataset to avoid bug in datatree


def extract_array(obj, dims):
    if isinstance(dims, str):
        dims = [dims]

    # special case for pulses:
    if "pulses" in dims and len(obj) == 1 and isinstance(obj[0], str):
        obj = obj[0].split()
    elif len(obj) >= 1 and is_composite_value(obj[0]):
        obj = list(map(compose_left(convert_composite, second), obj))
    data = np.array(obj)
    if data.size > 1:
        data = np.squeeze(data)
    return xr.Variable(dims, data)


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

    attrs = keymap(lambda k: k.lstrip("@"), attributes)

    return xr.Variable(dims, values, attrs)


def extract_entry(name, obj, dims=None, default_dims=None):
    if default_dims is None:
        default_dims = [name]

    if isinstance(dims, dict):
        dims = dims.get(name, default_dims)
    elif dims is None:
        dims = default_dims

    if is_array(obj):
        # dimension coordinate
        return extract_array(obj, dims=dims)
    elif is_composite_value(obj):
        return extract_composite(obj, dims=dims)
    elif isinstance(obj, dict):
        return extract_variable(obj, dims=dims)
    elif is_nested_array(obj):
        return extract_nested_array(obj, dims=dims).pipe(rename, name)
    else:
        raise ValueError(f"unknown datastructure:\n{obj}")


def extract_dataset(obj, dims=None, default_dims=None):
    filtered = keyfilter(lambda x: x not in ignore, obj)
    attrs, variables = valsplit(is_scalar, filtered)
    if len(variables) == 1 and is_nested_dataset(first_values(variables)):
        return extract_nested_dataset(first_values(variables), dims=dims).assign_attrs(
            attrs
        )

    variables_ = keymap(lambda k: k.lstrip("@"), variables)

    filtered_variables = valfilter(lambda x: not is_nested_dataset(x), variables_)

    data_vars = itemmap(
        lambda item: (
            item[0],
            extract_entry(*item, dims=dims, default_dims=default_dims),
        ),
        filtered_variables,
    )
    return xr.Dataset(data_vars=data_vars, attrs=attrs)


def extract_nested_variable(obj, dims=None):
    if is_array(obj):
        return xr.Variable(dims, obj)

    columns = merge_with(list, *obj)
    attributes, data = keysplit(lambda k: k.startswith("@"), columns)
    renamed = keymap(lambda k: k.lstrip("@"), attributes)
    attrs = valmap(first, renamed)

    return xr.Variable(dims, data["$"], attrs)


def unstack(obj, dim="stacked"):
    if dim not in obj.dims:
        return obj

    stacked_coords = [name for name, arr in obj.coords.items() if dim in arr.dims]

    return obj.set_index({dim: stacked_coords}).unstack(dim)


def rename(obj, name):
    renamed = obj.rename(name)
    if "$" not in obj.dims:
        return renamed

    if len(obj.dims) != 1:
        raise ValueError(f"unexpected number of dimensions: {list(obj.dims)}")

    return renamed.swap_dims({"$": name})


def to_variable_tuple(name, value, dims):
    if name in dims:
        dims_ = [name]
    else:
        dims_ = dims

    return (dims_, value)


def extract_nested_array(obj, dims=None):
    columns = merge_with(list, *obj)

    attributes, data = keysplit(flip(str.startswith, "@"), columns)
    renamed = keymap(flip(str.lstrip, "@"), attributes)
    preprocessed_attrs = valmap(np.squeeze, renamed)
    attrs_, indexes = valsplit(is_attr, preprocessed_attrs)
    preprocessed_data = valmap(np.squeeze, data)

    originally_stacked = isinstance(dims, (tuple, list)) and "stacked" in dims

    if len(indexes) == 1:
        dims = list(indexes)
    elif len(indexes) >= 2:
        dims = ["stacked"]
    elif dims is None:
        dims = ["$"]

    coords = itemmap(
        lambda it: (it[0], to_variable_tuple(*it, dims=dims)),
        indexes,
    )

    arr = xr.DataArray(
        data=preprocessed_data["$"],
        attrs=valmap(first, attrs_),
        dims=dims,
        coords=coords,
    )
    if originally_stacked:
        return arr
    return arr.pipe(unstack, dim="stacked")


def extract_nested_dataset(obj, dims=None):
    if not isinstance(obj, list):
        raise ValueError(f"unknown type: {type(obj)}")

    columns = merge_with(list, *obj)

    attributes, data = keysplit(flip(str.startswith, "@"), columns)
    renamed = keymap(flip(str.lstrip, "@"), attributes)
    preprocessed = valmap(np.squeeze, renamed)
    attrs_, indexes = valsplit(is_attr, preprocessed)

    attrs = valmap(first, attrs_)

    if dims is None:
        if len(indexes) <= 1:
            dims = list(indexes)
        else:
            dims = ["stacked"]

    data_vars = valmap(curry(extract_nested_variable)(dims=dims), data)
    coords = itemmap(
        lambda it: (it[0], to_variable_tuple(*it, dims=dims)),
        indexes,
    )

    return xr.Dataset(data_vars=data_vars, coords=coords, attrs=attrs).pipe(
        unstack, dim="stacked"
    )


def extract_nested_datatree(obj, dims=None):
    if not isinstance(obj, list):
        raise ValueError(f"unknown type: {type(obj)}")

    datasets = merge_with(list, *obj)
    tree = valmap(curry(extract_nested_dataset)(dims=dims), datasets)

    return xr.DataTree.from_dict(tree)
