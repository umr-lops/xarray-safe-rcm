import io
import posixpath
import re
from collections import deque

import xmlschema
from lxml import etree
from tlz.dicttoolz import keymap

include_re = re.compile(r'\s*<xsd:include schemaLocation="(?P<location>[^"/]+)"\s?/>')


def remove_includes(text):
    return include_re.sub("", text)


def extract_includes(text):
    return include_re.findall(text)


def normalize(root, path):
    if posixpath.isabs(path) or posixpath.dirname(path):
        return path

    return posixpath.join(root, path)


def schema_paths(mapper, root_schema):
    unvisited = deque([root_schema])
    visited = []
    while unvisited:
        path = unvisited.popleft()
        if path not in visited:
            visited.append(path)

        text = mapper[path].decode()
        includes = extract_includes(text)

        current_root = posixpath.dirname(path)
        normalized = [normalize(current_root, p) for p in includes]

        unvisited.extend([p for p in normalized if p not in visited])

    return visited


def open_schema(mapper, schema):
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
    paths = schema_paths(mapper, schema)
    preprocessed = [io.StringIO(remove_includes(mapper[p].decode())) for p in paths]

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
    schema = open_schema(mapper, schema_path)

    decoded = schema.decode(tree)

    return decoded
