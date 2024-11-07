import textwrap

from safe_rcm import xml

schemas = [
    textwrap.dedent(
        """\
        <?xml version="1.0" encoding="UTF-8"?>
        <xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">
        </xsd:schema>
        """.rstrip()
    ),
    textwrap.dedent(
        """\
        <?xml version="1.0" encoding="UTF-8"?>
        <xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">
            <xsd:include schemaLocation="schema1.xsd"/>
            <xsd:include schemaLocation="schema2.xsd"/>
        </xsd:schema>
        """.rstrip()
    ),
]


def test_remove_includes():
    expected = schemas[0]
    actual = xml.remove_includes(schemas[1])

    assert actual == expected
