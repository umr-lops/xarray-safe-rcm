import pandas as pd
import xarray as xr
from tlz.dicttoolz import keyfilter, merge, merge_with, valfilter, valmap
from tlz.functoolz import compose_left, curry, juxt
from tlz.itertoolz import first, second

from safe_rcm.product import transformers
from safe_rcm.product.dicttoolz import keysplit, query
from safe_rcm.product.predicates import disjunction, is_nested_array, is_scalar_valued
from safe_rcm.product.utils import dictfirst, starcall
from safe_rcm.xml import read_xml


@curry
def attach_path(obj, path):
    if not hasattr(obj, "encoding"):
        raise ValueError(
            "cannot attach source path: `obj` does not have a `encoding` attribute."
        )

    new = obj.copy()
    new.encoding["xpath"] = path

    return new


@curry
def execute(mapping, f, path):
    subset = query(path, mapping)

    return compose_left(f, attach_path(path=path))(subset)


def read_product(mapper, product_path):
    decoded = read_xml(mapper, product_path)

    layout = {
        "/": {
            "path": "/",
            "f": curry(transformers.extract_metadata)(collapse=["securityAttributes"]),
        },
        "/sourceAttributes": {
            "path": "/sourceAttributes",
            "f": transformers.extract_metadata,
        },
        "/sourceAttributes/radarParameters": {
            "path": "/sourceAttributes/radarParameters",
            "f": transformers.extract_dataset,
        },
        "/sourceAttributes/radarParameters/prfInformation": {
            "path": "/sourceAttributes/radarParameters/prfInformation",
            "f": transformers.extract_nested_dataset,
        },
        "/sourceAttributes/orbitAndAttitude/orbitInformation": {
            "path": "/sourceAttributes/orbitAndAttitude/orbitInformation",
            "f": compose_left(
                curry(transformers.extract_dataset)(dims="timeStamp"),
                lambda ds: ds.assign_coords(
                    {"timeStamp": pd.to_datetime(ds["timeStamp"].values).as_unit("ns")}
                ),
            ),
        },
        "/sourceAttributes/orbitAndAttitude/attitudeInformation": {
            "path": "/sourceAttributes/orbitAndAttitude/attitudeInformation",
            "f": compose_left(
                curry(transformers.extract_dataset)(dims="timeStamp"),
                lambda ds: ds.assign_coords(
                    {"timeStamp": pd.to_datetime(ds["timeStamp"].values).as_unit("ns")}
                ),
            ),
        },
        "/sourceAttributes/rawDataAttributes": {
            "path": "/sourceAttributes/rawDataAttributes",
            "f": compose_left(
                curry(keysplit, lambda k: k != "rawDataAnalysis"),
                juxt(
                    compose_left(first, transformers.extract_dataset),
                    compose_left(
                        second,
                        dictfirst,
                        curry(starcall, curry(merge_with, list)),
                        curry(
                            transformers.extract_dataset,
                            dims={"rawDataHistogram": ["stacked", "histogram"]},
                            default_dims=["stacked"],
                        ),
                        lambda obj: obj.set_index({"stacked": ["pole", "beam"]}),
                        lambda obj: obj.unstack("stacked"),
                    ),
                ),
                curry(xr.merge),
            ),
        },
        "/imageGenerationParameters/generalProcessingInformation": {
            "path": "/imageGenerationParameters/generalProcessingInformation",
            "f": transformers.extract_metadata,
        },
        "/imageGenerationParameters/sarProcessingInformation": {
            "path": "/imageGenerationParameters/sarProcessingInformation",
            "f": compose_left(
                curry(keyfilter, lambda k: k not in {"azimuthWindow", "rangeWindow"}),
                transformers.extract_dataset,
            ),
        },
        "/imageGenerationParameters/chirps": {
            "path": "/imageGenerationParameters/chirp",
            "f": compose_left(
                lambda el: merge_with(list, *el),
                curry(keysplit, lambda k: k != "chirpQuality"),
                juxt(
                    first,
                    compose_left(
                        second,
                        dictfirst,
                        lambda el: merge_with(list, *el),
                    ),
                ),
                lambda x: merge(*x),
                curry(
                    transformers.extract_dataset,
                    dims={
                        "amplitudeCoefficients": ["stacked", "coefficients"],
                        "phaseCoefficients": ["stacked", "coefficients"],
                    },
                    default_dims=["stacked"],
                ),
                lambda obj: obj.set_index({"stacked": ["pole", "pulse"]}),
                lambda obj: obj.drop_duplicates("stacked", keep="last"),
                lambda obj: obj.unstack("stacked"),
            ),
        },
        "/imageGenerationParameters/slantRangeToGroundRange": {
            "path": "/imageGenerationParameters/slantRangeToGroundRange",
            "f": compose_left(
                lambda el: merge_with(list, *el),
                curry(
                    transformers.extract_dataset,
                    dims={
                        "groundToSlantRangeCoefficients": [
                            "zeroDopplerAzimuthTime",
                            "coefficients",
                        ],
                    },
                    default_dims=["zeroDopplerAzimuthTime"],
                ),
            ),
        },
        "/imageReferenceAttributes": {
            "path": "/imageReferenceAttributes",
            "f": compose_left(
                curry(valfilter)(disjunction(is_scalar_valued, is_nested_array)),
                transformers.extract_dataset,
            ),
        },
        "/imageReferenceAttributes/rasterAttributes": {
            "path": "/imageReferenceAttributes/rasterAttributes",
            "f": transformers.extract_dataset,
        },
        "/imageReferenceAttributes/geographicInformation/ellipsoidParameters": {
            "path": "/imageReferenceAttributes/geographicInformation/ellipsoidParameters",
            "f": curry(transformers.extract_dataset)(dims="params"),
        },
        "/imageReferenceAttributes/geographicInformation/geolocationGrid": {
            "path": "/imageReferenceAttributes/geographicInformation/geolocationGrid/imageTiePoint",
            "f": compose_left(
                curry(transformers.extract_nested_datatree)(dims="tie_points"),
                lambda tree: xr.merge([node.ds for node in tree.subtree]),
                lambda ds: ds.set_index(tie_points=["line", "pixel"]),
                lambda ds: ds.unstack("tie_points"),
            ),
        },
        "/imageReferenceAttributes/geographicInformation/rationalFunctions": {
            "path": "/imageReferenceAttributes/geographicInformation/rationalFunctions",
            "f": curry(transformers.extract_dataset)(dims="coefficients"),
        },
        "/sceneAttributes": {
            "path": "/sceneAttributes/imageAttributes",
            "f": compose_left(
                first,  # GRD datasets only have 1
                curry(keyfilter)(lambda x: not x.startswith("@")),
                transformers.extract_dataset,
            ),
        },
        "/grdBurstMap": {
            "path": "/grdBurstMap",
            "f": compose_left(
                curry(
                    map,
                    compose_left(
                        curry(keysplit, lambda k: k != "burstAttributes"),
                        juxt(
                            first,
                            compose_left(
                                second,
                                dictfirst,
                                curry(starcall, curry(merge_with, list)),
                            ),
                        ),
                        curry(starcall, merge),
                        curry(
                            transformers.extract_dataset,
                            dims=["stacked"],
                        ),
                        lambda obj: obj.set_index({"stacked": ["burst", "beam"]}),
                        lambda obj: obj.unstack("stacked"),
                    ),
                ),
                list,
                curry(xr.concat, dim="burst_maps"),
            ),
        },
        "/dopplerCentroid": {
            "path": "/dopplerCentroid",
            "f": compose_left(
                curry(
                    map,
                    compose_left(
                        curry(keysplit, lambda k: k != "dopplerCentroidEstimate"),
                        juxt(
                            first,
                            compose_left(
                                second,
                                dictfirst,
                                curry(starcall, curry(merge_with, list)),
                            ),
                        ),
                        curry(starcall, merge),
                        curry(
                            transformers.extract_dataset,
                            dims={
                                "dopplerCentroidCoefficients": [
                                    "burst",
                                    "coefficients",
                                ],
                            },
                            default_dims=["burst"],
                        ),
                    ),
                ),
                list,
                curry(xr.concat, dim="burst_maps"),
            ),
        },
        "/dopplerRate": {
            "path": "/dopplerRate",
            "f": compose_left(
                curry(
                    map,
                    compose_left(
                        curry(keysplit, lambda k: k != "dopplerRateEstimate"),
                        juxt(
                            first,
                            compose_left(
                                second,
                                dictfirst,
                                curry(starcall, curry(merge_with, list)),
                            ),
                        ),
                        curry(starcall, merge),
                        curry(
                            transformers.extract_dataset,
                            dims={
                                "dopplerRateCoefficients": ["burst", "coefficients"],
                            },
                            default_dims=["burst"],
                        ),
                    ),
                ),
                list,
                curry(xr.concat, dim="burst_maps"),
            ),
        },
    }

    converted = valmap(
        lambda x: execute(**x)(decoded),
        layout,
    )
    return xr.DataTree.from_dict(converted)
