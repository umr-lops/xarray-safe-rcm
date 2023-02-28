import datatree
import toolz
import xarray as xr

from .dicttoolz import keysplit, valsplit
from .predicates import is_scalar


def extract_variable(obj, dims=()):
    attributes, data = keysplit(lambda k: k.startswith("@"), obj)
    if list(data) != ["$"]:
        raise ValueError("not a variable")

    values = data["$"]
    if is_scalar(values):
        dims = ()

    attrs = toolz.dicttoolz.keymap(lambda k: k.lstrip("@"), attributes)

    return xr.Variable(dims, values, attrs)


def extract_dataset(obj, dims=()):
    attrs, variables = valsplit(is_scalar, obj)

    vars_ = toolz.dicttoolz.valmap(
        toolz.functoolz.curry(extract_variable)(dims=dims), variables
    )
    return xr.Dataset(data_vars=vars_, attrs=attrs)


def extract_nested_datatree(obj, dims=None):
    # print(obj)
    return datatree.DataTree()
