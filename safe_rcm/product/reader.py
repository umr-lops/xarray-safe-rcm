import toolz
from lxml import etree

from ..schema import open_schema
from . import utils


def extract_metadata(
    mapping,
    namespaces,
    collapse=(),
    ignore=("@xmlns", "@xmlns:rcm", "@xmlns:xsi", "@xsi:schemaLocation"),
):
    def metadata_filter(item):
        """filter metadata items

        Metadata items are either attributes (the name starts with an '@'), or they are scalars.
        """
        k, v = item

        return (k.startswith("@") or utils.is_scalar(v)) and k not in ignore

    # extract the metadata
    metadata = toolz.dicttoolz.itemfilter(metadata_filter, mapping)

    # collapse the selected items
    to_collapse = toolz.dicttoolz.keyfilter(lambda x: x in collapse, mapping)
    collapsed = dict(toolz.itertoolz.concat(v.items() for v in to_collapse.values()))

    result = metadata | collapsed
    return toolz.dicttoolz.keymap(
        lambda k: utils.strip_namespaces(k, namespaces).lstrip("@"), result
    )


def execute(f, path, kwargs={}):
    def inner(mapping, namespaces):
        subset = utils.query(mapping, path)

        return f(subset, namespaces, **kwargs)

    return inner


def read_product(fs, product_url):
    tree = etree.fromstring(fs.cat(product_url))

    namespaces = toolz.dicttoolz.keymap(
        lambda x: x if x is not None else "rcm", tree.nsmap
    )
    schema_location = tree.xpath("./@xsi:schemaLocation", namespaces=namespaces)[0]
    _, schema_path = schema_location.split(" ")

    if not schema_path.startswith(".."):
        raise ValueError("schema path is absolute, the code can't handle that")

    root, _ = product_url.rsplit("/", maxsplit=1)
    schema_url = utils.absolute_url_path(f"{root}/{schema_path}")
    schema_root, schema_name = schema_url.rsplit("/", maxsplit=1)

    schema = open_schema(fs, schema_root, schema_name)

    decoded = schema.decode(tree)

    layout = {
        "/": {
            "path": "/",
            "f": extract_metadata,
            "kwargs": {"collapse": ["securityAttributes"]},
        },
    }

    converted = toolz.dicttoolz.valmap(
        lambda x: execute(**x)(decoded, namespaces),
        layout,
    )
    return converted
