from tlz import filter
from tlz.functoolz import compose_left, curry
from tlz.itertoolz import concat, get

from safe_rcm.product.dicttoolz import query
from safe_rcm.xml import read_xml


def merge_location(loc):
    locator = loc["@locator"]
    href = loc["@href"]

    return f"{locator}/{href}".lstrip("/")


def read_manifest(mapper, path):
    structure = {
        "/dataObjectSection/dataObject": compose_left(
            curry(
                map,
                compose_left(
                    curry(get, "byteStream"),
                    curry(
                        map,
                        compose_left(
                            curry(get, "fileLocation"), curry(map, merge_location)
                        ),
                    ),
                    concat,
                ),
            ),
            concat,
        ),
        "/metadataSection/metadataObject": compose_left(
            curry(
                filter,
                compose_left(curry(get, "@classification"), lambda x: x == "SYNTAX"),
            ),
            curry(map, compose_left(curry(get, "metadataReference"), merge_location)),
        ),
    }

    manifest = read_xml(mapper, path)

    return list(concat(func(query(path, manifest)) for path, func in structure.items()))
