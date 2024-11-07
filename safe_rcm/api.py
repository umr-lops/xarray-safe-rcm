import os
import posixpath
from fnmatch import fnmatchcase

import fsspec
import xarray as xr
from fsspec.implementations.dirfs import DirFileSystem
from tlz.dicttoolz import valmap
from tlz.functoolz import compose_left, curry, juxt

from safe_rcm.calibrations import read_noise_levels
from safe_rcm.manifest import read_manifest
from safe_rcm.product.reader import read_product
from safe_rcm.product.transformers import extract_dataset
from safe_rcm.product.utils import starcall
from safe_rcm.xml import read_xml

try:
    ExceptionGroup
except NameError:
    from exceptiongroup import ExceptionGroup


@curry
def execute(tree, f, path):
    node = tree[path]

    return f(node)


def ignored_file(path, ignores):
    ignored = [
        fnmatchcase(path, ignore) or fnmatchcase(posixpath.basename(path), ignore)
        for ignore in ignores
    ]
    return any(ignored)


def open_rcm(
    url,
    *,
    backend_kwargs=None,
    manifest_ignores=[
        "*.pdf",
        "*.html",
        "*.xslt",
        "*.png",
        "*.kml",
        "*.txt",
        "preview/*",
    ],
    **dataset_kwargs,
):
    """read SAFE files of the radarsat constellation mission (RCM)

    Parameters
    ----------
    url : str
    backend_kwargs : mapping
    manifest_ignores : list of str, default: ["*.pdf", "*.html", "*.xslt", "*.png", \
                                              "*.kml", "*.txt", "preview/*"]
        Globs that match files from the manifest that are allowed to be missing.
    **dataset_kwargs
        Keyword arguments forwarded to `xr.open_dataset`, used to open
        the contained data files.
    """
    if not isinstance(url, (str, os.PathLike)):
        raise ValueError(f"cannot deal with object of type {type(url)}: {url}")

    if backend_kwargs is None:
        backend_kwargs = {}

    url = os.fspath(url)

    storage_options = backend_kwargs.get("storage_options", {})
    mapper = fsspec.get_mapper(url, **storage_options)
    relative_fs = DirFileSystem(path=url, fs=mapper.fs)

    try:
        declared_files = read_manifest(mapper, "manifest.safe")
    except (FileNotFoundError, KeyError):
        raise ValueError(
            "cannot find the `manifest.safe` file. Are you sure this is a SAFE dataset?"
        )

    missing_files = [
        path
        for path in declared_files
        if not ignored_file(path, manifest_ignores) and not relative_fs.exists(path)
    ]
    if missing_files:
        raise ExceptionGroup(
            "not all files declared in the manifest are available",
            [ValueError(f"{p} does not exist") for p in missing_files],
        )

    tree = read_product(mapper, "metadata/product.xml")

    calibration_root = "metadata/calibration"
    lookup_table_structure = {
        "/incidenceAngles": {
            "path": "/imageReferenceAttributes",
            "f": compose_left(
                lambda obj: obj.attrs["incidenceAngleFileName"],
                curry(posixpath.join, calibration_root),
                curry(read_xml, mapper),
                curry(extract_dataset, dims="coefficients"),
            ),
        },
        "/lookupTables": {
            "path": "/imageReferenceAttributes/lookupTableFileName",
            "f": compose_left(
                lambda obj: obj.stack(stacked=["sarCalibrationType", "pole"]),
                lambda obj: obj.reset_index("stacked"),
                juxt(
                    compose_left(
                        lambda obj: obj.to_series().to_dict(),
                        curry(valmap, curry(posixpath.join, calibration_root)),
                        curry(valmap, curry(read_xml)(mapper)),
                        curry(valmap, curry(extract_dataset, dims="coefficients")),
                        curry(valmap, lambda ds: ds["gains"].assign_attrs(ds.attrs)),
                        lambda d: xr.concat(list(d.values()), dim="stacked"),
                    ),
                    lambda obj: obj.coords,
                ),
                curry(starcall, lambda arr, coords: arr.assign_coords(coords)),
                lambda arr: arr.set_index({"stacked": ["sarCalibrationType", "pole"]}),
                lambda arr: arr.unstack("stacked"),
                lambda arr: arr.rename("lookup_tables"),
                lambda arr: arr.to_dataset(),
            ),
        },
        "/noiseLevels": {
            "path": "/imageReferenceAttributes/noiseLevelFileName",
            "f": curry(read_noise_levels, mapper, calibration_root),
        },
    }
    calibration = valmap(
        lambda x: execute(**x)(tree),
        lookup_table_structure,
    )

    imagery_paths = tree["/sceneAttributes/ipdf"].to_series().to_dict()
    resolved = valmap(
        compose_left(
            curry(posixpath.join, "metadata"),
            posixpath.normpath,
        ),
        imagery_paths,
    )
    imagery_dss = valmap(
        compose_left(
            curry(relative_fs.open),
            curry(xr.open_dataset, engine="rasterio", **dataset_kwargs),
        ),
        resolved,
    )
    dss = [ds.assign_coords(pole=coord) for coord, ds in imagery_dss.items()]
    imagery = xr.concat(dss, dim="pole")

    return tree.assign(
        {
            "lookupTables": xr.DataTree.from_dict(calibration),
            "imagery": xr.DataTree(imagery),
        }
    )
