import toolz
import xarray as xr

from ..xml import read_xml
from . import converters, transformers
from .dicttoolz import query


def execute(f, path, kwargs={}):
    def inner(mapping):
        subset = query(path, mapping)

        return f(subset, **kwargs)

    return inner


@toolz.functoolz.curry
def convert(converters, item):
    key, value = item
    converter = converters.get(key, lambda x: x)
    return key, converter(value)


def extract_geographic_information(mapping):
    # two keys: ellipsoidParameters and geolocationGrid
    # processing:
    # - extract ellipsoidParameters as dataset with dims "params"
    # - extract geolocationGrid as datatree with dims "tie_points"
    # - merge geolocationGrid to a dataset
    # - set_index + unstack on tiepoint → [line, pixel]
    # - merge both groups into a single dataset

    converter_funcs = {
        "ellipsoidParameters": toolz.functoolz.curry(transformers.extract_dataset)(
            dims="params"
        ),
        "geolocationGrid": toolz.functoolz.compose_left(
            toolz.functoolz.curry(transformers.extract_nested_datatree)(
                dims="tie_points"
            ),
            lambda tree: xr.merge([node.ds for node in tree.subtree]),
            lambda ds: ds.set_index(tie_points=["line", "pixel"]),
            lambda ds: ds.unstack("tie_points"),
        ),
    }
    converted = toolz.dicttoolz.itemmap(convert(converter_funcs), mapping)
    print(converted)
    return xr.merge(list(converted.values()))


def read_product(fs, product_url):
    decoded = read_xml(fs, product_url)

    layout = {
        "/": {
            "path": "/",
            "f": converters.extract_metadata,
            "kwargs": {"collapse": ["securityAttributes"]},
        },
        "/imageReferenceAttributes": {
            "path": "/imageReferenceAttributes",
            "f": converters.extract_metadata,
            "kwargs": {},
        },
        "/geolocationGrid": {
            "path": "/imageReferenceAttributes/geographicInformation",
            "f": extract_geographic_information,
            "kwargs": {},
        },
    }

    converted = toolz.dicttoolz.valmap(
        lambda x: execute(**x)(decoded),
        layout,
    )
    return converted
