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


Container = collections.namedtuple("SchemaSetup", ["mapper", "path", "expected"])
SchemaProperties = collections.namedtuple(
    "SchemaProperties", ["root_elements", "simple_types", "complex_types"]
)


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
          <xsd:element name="manifest" type="manifest"/>
        </xsd:schema>
        """
    ).encode()
    mapper["schemas/schema2.xsd"] = dedent(
        """
        <?xml version="1.0" encoding="UTF-8"?>
        <xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">
          <xsd:include schemaLocation="schema4.xsd"/>
          <xsd:element name="count" type="count"/>
        </xsd:schema>
        """
    ).encode()
    mapper["schemas/schema3.xsd"] = dedent(
        """
        <?xml version="1.0" encoding="UTF-8"?>
        <xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">
          <xsd:include schemaLocation="schema3.xsd"/>
          <xsd:complexType name="manifest">
            <xsd:sequence>
              <xsd:element name="quantity_a" type="count"/>
              <xsd:element name="quantity_b" type="count"/>
            </xsd:sequence>
          </xsd:complexType>
        </xsd:schema>
        """
    ).encode()
    mapper["schemas/schema4.xsd"] = dedent(
        """
        <?xml version="1.0" encoding="UTF-8"?>
        <xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">
          <xsd:simpleType name="count">
            <xsd:restriction base="xsd:integer">
              <xsd:minInclusive value="0"/>
              <xsd:maxInclusive value="10"/>
            </xsd:restriction>
          </xsd:simpleType>
        </xsd:schema>
        """
    ).encode()

    return schema_index, mapper


@pytest.fixture
def schema_paths_setup(schema_setup):
    schema_index, mapper = schema_setup

    expected = [
        ["schemas/root.xsd"],
        ["schemas/root.xsd", "schemas/schema2.xsd", "schemas/schema4.xsd"],
        [
            "schemas/root.xsd",
            "schemas/schema1.xsd",
            "schemas/schema2.xsd",
            "schemas/schema3.xsd",
            "schemas/schema4.xsd",
        ],
    ]

    return Container(mapper, "schemas/root.xsd", expected[schema_index])


@pytest.fixture
def schema_content_setup(schema_setup):
    schema_index, mapper = schema_setup

    count_type = {"name": "count", "type": "simple", "base_type": "integer"}
    manifest_type = {"name": "manifest", "type": "complex"}

    manifest_element = {"name": "manifest", "type": manifest_type}
    count_element = {"name": "count", "type": count_type}
    expected = [
        SchemaProperties([], [], []),
        SchemaProperties([count_element], [count_type], []),
        SchemaProperties(
            [manifest_element, count_element], [count_type], [manifest_type]
        ),
    ]

    return Container(mapper, "schemas/root.xsd", expected[schema_index])


@pytest.fixture(params=["data.xml", "data/file.xml"])
def data_file_setup(request):
    path = request.param
    mapper = fsspec.get_mapper("memory")

    mapper["schemas/root.xsd"] = dedent(
        """
        <?xml version="1.0" encoding="UTF-8"?>
        <xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">
          <xsd:include schemaLocation="schema1.xsd"/>
          <xsd:include schemaLocation="schema2.xsd"/>
          <xsd:complexType name="elements">
            <xsd:sequence>
              <xsd:element name="summary" type="manifest"/>
              <xsd:element name="count" type="count"/>
            </xsd:sequence>
          </xsd:complexType>
          <xsd:element name="elements" type="elements"/>
        </xsd:schema>
        """
    ).encode()
    mapper["schemas/schema1.xsd"] = dedent(
        """
        <?xml version="1.0" encoding="UTF-8"?>
        <xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">
          <xsd:include schemaLocation="schema2.xsd"/>
          <xsd:complexType name="manifest">
            <xsd:sequence>
              <xsd:element name="quantity_a" type="count"/>
              <xsd:element name="quantity_b" type="count"/>
            </xsd:sequence>
          </xsd:complexType>
        </xsd:schema>
        """
    ).encode()
    mapper["schemas/schema2.xsd"] = dedent(
        """
        <?xml version="1.0" encoding="UTF-8"?>
        <xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">
          <xsd:simpleType name="count">
            <xsd:restriction base="xsd:integer">
              <xsd:minInclusive value="0"/>
              <xsd:maxInclusive value="10"/>
            </xsd:restriction>
          </xsd:simpleType>
        </xsd:schema>
        """
    ).encode()

    schema_path = "schemas/root.xsd" if "/" not in path else "../schemas/root.xsd"
    mapper[path] = dedent(
        f"""
        <?xml version="1.0" encoding="UTF-8"?>
        <elements xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="schema {schema_path}">
          <summary>
            <quantity_a>1</quantity_a>
            <quantity_b>2</quantity_b>
          </summary>
          <count>3</count>
        </elements>
        """
    ).encode()

    expected = {
        "@xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "@xsi:schemaLocation": f"schema {schema_path}",
        "summary": {"quantity_a": 1, "quantity_b": 2},
        "count": 3,
    }

    return Container(mapper, path, expected)


def convert_type(t):
    def strip_namespace(name):
        return name.split("}", maxsplit=1)[1]

    if hasattr(t, "content"):
        # complex type
        return {"name": t.name, "type": "complex"}
    elif hasattr(t, "base_type"):
        # simple type, only restriction
        return {
            "name": t.name,
            "base_type": strip_namespace(t.base_type.name),
            "type": "simple",
        }


def convert_element(el):
    return {"name": el.name, "type": convert_type(el.type)}


def extract_schema_properties(schema):
    return SchemaProperties(
        [convert_element(v) for v in schema.root_elements],
        [convert_type(v) for v in schema.simple_types],
        [convert_type(v) for v in schema.complex_types],
    )


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


def test_schema_paths(schema_paths_setup):
    actual = xml.schema_paths(schema_paths_setup.mapper, schema_paths_setup.path)

    expected = schema_paths_setup.expected

    assert actual == expected


def test_open_schemas(schema_content_setup):
    container = schema_content_setup
    actual = xml.open_schema(container.mapper, container.path)
    expected = container.expected

    assert extract_schema_properties(actual) == expected


def test_read_xml(data_file_setup):
    container = data_file_setup

    actual = xml.read_xml(container.mapper, container.path)

    assert actual == container.expected
