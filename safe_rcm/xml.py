import io
import posixpath
import re

import xmlschema
from lxml import etree
from tlz.dicttoolz import keymap

include_re = re.compile(r'\s*<xsd:include schemaLocation="(?P<location>[^"/]+)"\s?/>')


def remove_includes(f):
    text = f.read().decode()
    return io.StringIO(include_re.sub("", text))


def open_schema(mapper, root, name, *, glob="*.xsd"):
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
    schema_root = mapper._key_to_str(root)
    fs = mapper.fs

    urls = sorted(
        fs.glob(f"{schema_root}/{glob}"), key=lambda u: u.endswith(name), reverse=True
    )
    preprocessed = [remove_includes(fs.open(u)) for u in urls]
    return xmlschema.XMLSchema(preprocessed)


def read_xml(mapper, path):
    raw_data = mapper[path]
    tree = etree.fromstring(raw_data)

    namespaces = keymap(lambda x: x if x is not None else "rcm", tree.nsmap)
    schema_location = tree.xpath("./@xsi:schemaLocation", namespaces=namespaces)[0]
    _, schema_path_ = schema_location.split(" ")

    schema_path = posixpath.normpath(
        posixpath.join(posixpath.dirname(path), schema_path_)
    )
    schema_root, schema_name = posixpath.split(schema_path)

    schema = open_schema(mapper, schema_root, schema_name)

    decoded = schema.decode(tree)

    return decoded
