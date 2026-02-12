import os
import os.path
import posixpath
import re
import urllib.request
from collections import deque

import xmlschema
from fsspec.implementations.dirfs import DirFileSystem
from lxml import etree
from tlz.dicttoolz import keymap


class CustomOpener(urllib.request.OpenerDirector):
    def __init__(self, fs):
        self.fs = fs

    def open(self, url, data: None = None, timeout: None = None):
        # undo normalization
        # FIXME: figure out how to make xmlschema skip this step
        path = os.path.relpath(url.removeprefix("file://"))

        return self.fs.open(path)


def open_schema(mapper, schema_path):
    dirfs = DirFileSystem(fs=mapper.fs, path=mapper.root)
    opener = CustomOpener(dirfs)

    settings = xmlschema.settings.SchemaSettings(opener=opener)
    return xmlschema.XMLSchema.from_settings(settings=settings, source=schema_path)


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
