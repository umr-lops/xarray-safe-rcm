import toolz
import xarray as xr

from .utils import split_marked, strip_namespaces


def determine_indexes(columns, hint):
    if hint == "first" or isinstance(hint, list):
        if hint == "first":
            hint = [0]
        return [col for index, col in enumerate(columns) if index in hint]
    elif hint == "attribute":
        return [col.lstrip("@") for col in columns]
    else:
        raise ValueError(f"unknown index hint: {hint}")


def preprocess_names(mapping, namespaces):
    def preprocess(name):
        return strip_namespaces(name, namespaces)

    return toolz.dicttoolz.keymap(preprocess, mapping)


def preprocess_variables(mapping, index_columns):
    def preprocess(col, dims):
        if not isinstance(col[0], dict):
            return (dims, col)

        merged = toolz.dicttoolz.merge_with(list, *col)
        attrs_, data = split_marked(merged, marker="@")
        attrs = toolz.dicttoolz.valmap(toolz.first, attrs_)

        return (index_columns, data["$"], attrs)

    return {key: preprocess(col, index_columns) for key, col in mapping.items()}


def convert_table(table, *, namespaces={}, index_hint="first", dtypes={}):
    columns = toolz.dicttoolz.merge_with(list, *table)
    renamed = preprocess_names(columns, namespaces)
    indexes = determine_indexes(renamed, hint=index_hint)
    transformed = preprocess_variables(renamed, indexes)

    return (
        xr.Dataset(transformed)
        .assign(
            {name: lambda ds: ds[name].astype(dtype) for name, dtype in dtypes.items()}
        )
        .pipe(lambda obj: obj if list(obj) != ["$"] else obj["$"].rename(None))
    )
