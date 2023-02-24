import hypothesis.strategies as st
from hypothesis import given

from safe_rcm.product import utils

markers = st.characters()
marker = st.shared(markers, key="marker")


def marked_mapping(marker):
    values = st.just(None)

    unmarked_keys = st.text()
    marked_keys = st.builds(lambda k, m: m + k, unmarked_keys, marker)
    keys = st.one_of(unmarked_keys, marked_keys)

    return st.dictionaries(keys, values)


@given(marked_mapping(marker), marker)
def test_split_marked(mapping, marker):
    marked, unmarked = utils.split_marked(mapping, marker=marker)

    assert list(unmarked) == [key for key in mapping if not key.startswith(marker)]
