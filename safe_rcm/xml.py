import pathlib
import posixpath
import re
import tempfile
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


def open_schema(mapper, schema_path):
    """
    method to open xml schema from a mapper (ffspec obj like) and a path
    modified to support xmlschema==4.x

    Args:
        mapper (fsspec.mapping.FSMap object):
        schema_path (str): example 'support/schemas/rcm_prod_manifest.xsd' or 'support/schemas/rcm_prod_product.xsd'

    Returns:
        schema (xmlschema.validators.schemas.XMLSchema11):
    """

    # 1. Create a temporary directory that exists as long as we need the schema
    # Note: In a production app, you might want to cache this
    # so you don't rewrite files every time.
    temp_dir = tempfile.TemporaryDirectory()
    temp_path = pathlib.Path(temp_dir.name)

    # 2. Write all XSDs from the mapper to the temp directory
    # preserving the folder structure
    for k, v in mapper.items():
        if k.endswith(".xsd"):
            file_path = temp_path / k
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_bytes(v)

    # 3. Point xmlschema at the REAL file on the REAL disk
    # The library will now handle all relative includes/imports
    # perfectly using standard OS paths.
    absolute_schema_path = temp_path / schema_path

    try:
        schema = xmlschema.XMLSchema11(str(absolute_schema_path))
        # We attach the temp_dir object to the schema so it isn't
        # deleted until the schema object itself is garbage collected
        schema._temp_dir_handle = temp_dir
        return schema
    except Exception as e:
        temp_dir.cleanup()
        raise e


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
