import toolz
import xmlschema
from lxml import etree

from .fs_utils import normalize_url_path


def open_schema(fs, root, name, *, glob="*.xsd"):
    """fsspec-compatible way to open remote schema files

    Parameters
    ----------
    fs : fsspec.filesystem
        pre-instantiated fsspec filesystem instance
    root : str
        URL of the root directory of the schema files
    name : str
        File name of the schema to open.
    glob : str, default: "*.xsd"
        The glob used to find other schema files

    Returns
    -------
    xmlschema.XMLSchema
        The opened schema object
    """
    urls = sorted(
        fs.glob(f"{root}/{glob}"), key=lambda u: u.endswith(name), reverse=True
    )
    sources = [fs.open(u) for u in urls]

    return xmlschema.XMLSchema(sources)


def read_xml(fs, url):
    tree = etree.fromstring(fs.cat(url))

    namespaces = toolz.dicttoolz.keymap(
        lambda x: x if x is not None else "rcm", tree.nsmap
    )
    schema_location = tree.xpath("./@xsi:schemaLocation", namespaces=namespaces)[0]
    _, schema_path = schema_location.split(" ")

    if not schema_path.startswith(".."):
        raise ValueError("schema path is absolute, the code can't handle that yet")

    root, _ = url.rsplit("/", maxsplit=1)
    schema_url = normalize_url_path(f"{root}/{schema_path}")
    schema_root, schema_name = schema_url.rsplit("/", maxsplit=1)

    schema = open_schema(fs, schema_root, schema_name)

    decoded = schema.decode(tree)

    return decoded
