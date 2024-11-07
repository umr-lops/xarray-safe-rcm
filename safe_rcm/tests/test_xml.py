import collections
import textwrap

import fsspec
import pytest

from safe_rcm import xml


def dedent(text):
    return textwrap.dedent(text.removeprefix("\n").rstrip())


schemas = [
    dedent(
        """
        <?xml version="1.0" encoding="UTF-8"?>
        <xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">
        </xsd:schema>
        """
    ),
    dedent(
        """
        <?xml version="1.0" encoding="UTF-8"?>
        <xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">
            <xsd:include schemaLocation="schema2.xsd"/>
        </xsd:schema>
        """
    ),
    dedent(
        """
        <?xml version="1.0" encoding="UTF-8"?>
        <xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">
            <xsd:include schemaLocation="schema1.xsd"/>
            <xsd:include schemaLocation="schema2.xsd"/>
        </xsd:schema>
        """
    ),
]


@pytest.fixture(params=enumerate(schemas))
def schema_setup(request):
    schema_index, schema = request.param

    mapper = fsspec.get_mapper("memory")
    mapper["schemas/root.xsd"] = schema.encode()
    mapper["schemas/schema1.xsd"] = dedent(
        """
        <?xml version="1.0" encoding="UTF-8"?>
        <xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">
            <xsd:include schemaLocation="schema3.xsd"/>
        </xsd:schema>
        """
    ).encode()
    mapper["schemas/schema2.xsd"] = dedent(
        """
        <?xml version="1.0" encoding="UTF-8"?>
        <xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">
            <xsd:include schemaLocation="schema3.xsd"/>
        </xsd:schema>
        """
    ).encode()
    mapper["schemas/schema3.xsd"] = dedent(
        """
        <?xml version="1.0" encoding="UTF-8"?>
        <xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">
        </xsd:schema>
        """
    ).encode()

    expected = [
        ["schemas/root.xsd"],
        ["schemas/root.xsd", "schemas/schema2.xsd", "schemas/schema3.xsd"],
        [
            "schemas/root.xsd",
            "schemas/schema1.xsd",
            "schemas/schema2.xsd",
            "schemas/schema3.xsd",
        ],
    ]

    schema_setup = collections.namedtuple(
        "SchemaSetup", ["mapper", "root_schema", "expected"]
    )
    return schema_setup(mapper, "schemas/root.xsd", expected[schema_index])


def test_remove_includes():
    expected = schemas[0]
    actual = xml.remove_includes(schemas[1])

    assert actual == expected


@pytest.mark.parametrize(
    ["schema", "expected"],
    (
        (schemas[0], []),
        (schemas[1], ["schema2.xsd"]),
        (schemas[2], ["schema1.xsd", "schema2.xsd"]),
    ),
)
def test_extract_includes(schema, expected):
    actual = xml.extract_includes(schema)

    assert actual == expected


@pytest.mark.parametrize(
    ["root", "path", "expected"],
    (
        ("", "file.xml", "file.xml"),
        ("/root", "file.xml", "/root/file.xml"),
        ("/root", "/other_root/file.xml", "/other_root/file.xml"),
    ),
)
def test_normalize(root, path, expected):
    actual = xml.normalize(root, path)

    assert actual == expected


def test_schema_paths(schema_setup):
    actual = xml.schema_paths(schema_setup.mapper, schema_setup.root_schema)

    expected = schema_setup.expected

    assert actual == expected
