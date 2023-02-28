import datatree
import toolz
import xarray as xr

from ..xml import read_xml
from . import converters, transformers
from .dicttoolz import query


@toolz.functoolz.curry
def execute(mapping, f, path):
    subset = query(path, mapping)

    return f(subset)


@toolz.functoolz.curry
def convert(converters, item):
    key, value = item
    converter = converters.get(key, lambda x: x)
    return key, converter(value)


def read_product(fs, product_url):
    decoded = read_xml(fs, product_url)

    layout = {
        "/": {
            "path": "/",
            "f": toolz.functoolz.curry(converters.extract_metadata)(
                collapse=["securityAttributes"]
            ),
        },
        "/imageReferenceAttributes": {
            "path": "/imageReferenceAttributes",
            "f": converters.extract_metadata,
        },
        "/geographicInformation/ellipsoidParameters": {
            "path": "/imageReferenceAttributes/geographicInformation/ellipsoidParameters",
            "f": toolz.functoolz.curry(transformers.extract_dataset)(dims="params"),
        },
        "/geographicInformation/geolocationGrid": {
            "path": "/imageReferenceAttributes/geographicInformation/geolocationGrid/imageTiePoint",
            "f": toolz.functoolz.compose_left(
                toolz.functoolz.curry(transformers.extract_nested_datatree)(
                    dims="tie_points"
                ),
                lambda tree: xr.merge([node.ds for node in tree.subtree]),
                lambda ds: ds.set_index(tie_points=["line", "pixel"]),
                lambda ds: ds.unstack("tie_points"),
            ),
        },
    }

    converted = toolz.dicttoolz.valmap(
        lambda x: execute(**x)(decoded),
        layout,
    )
    return datatree.DataTree.from_dict(converted)
