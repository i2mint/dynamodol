"""`.items()` / `.values()` on a wrapped store must honour the wrapper (dynamodol#1, dol#83)."""

import pytest
from dol import KeyCodecs, Pipe, filt_iter, wrap_kvs

import dynamodol
from dynamodol import DynamoDbBasePersister, DynamoDbPrefixReader

from test_offline import FakeDb, FakeTable, KEY_FIELDS, DATA_FIELDS, TABLE_NAME

ROWS = (
    {"pkey": "part1", "skey": "v1/alice/notes.txt", "value": "ALICE-SECRET"},
    {"pkey": "part1", "skey": "v1/bob/notes.txt", "value": "BOB-SECRET"},
)

# the standard dol idiom for scoping a store down to one tenant
alice_only = Pipe(filt_iter.prefixes("alice/"), KeyCodecs.prefixed("alice/"))


def _leaf(cls=DynamoDbPrefixReader):
    return cls(
        db=FakeDb(FakeTable(ROWS)),
        table_name=TABLE_NAME,
        key_fields=("pkey", "skey"),
        data_fields=DATA_FIELDS,
        partition="part1",
        prefix="v1/",
    )


@pytest.mark.parametrize(
    "mk", [lambda: alice_only(_leaf()), lambda: _leaf(alice_only(DynamoDbPrefixReader))]
)
def test_key_scoped_store_items_and_values_are_scoped(mk):
    s = mk()
    assert list(s) == ["notes.txt"]
    assert dict(s) == {"notes.txt": "ALICE-SECRET"}
    assert dict(s.items()) == dict(s)
    assert list(s.values()) == ["ALICE-SECRET"]
    assert ("notes.txt", "ALICE-SECRET") in s.items()
    assert "BOB-SECRET" not in s.values()


def test_value_codec_applies_to_values_and_items():
    s = wrap_kvs(_leaf(), obj_of_data=lambda v: {"text": v})
    assert s["alice/notes.txt"] == {"text": "ALICE-SECRET"}
    assert list(s.values()) == [{"text": "ALICE-SECRET"}, {"text": "BOB-SECRET"}]
    assert dict(s.items()) == dict(s)


def test_unwrapped_items_match_dict():
    s = _leaf()
    assert dict(s.items()) == dict(s)


def test_scan_fast_path_on_unwrapped_reader():
    s = _leaf()
    assert dict(dynamodol.scan_items(s)) == dict(s)
    assert list(dynamodol.scan_values(s)) == list(s.values())


def test_scan_refuses_wrapped_store():
    with pytest.raises(TypeError, match="unwrapped"):
        list(dynamodol.scan_items(alice_only(_leaf())))
    with pytest.raises(TypeError, match="unwrapped"):
        list(dynamodol.scan_values(alice_only(_leaf())))


def test_removed_methods_point_to_replacement():
    with pytest.raises(AttributeError, match="scan_items"):
        _leaf().iter_items
    with pytest.raises(AttributeError, match="scan_values"):
        alice_only(_leaf()).iter_values
    with pytest.raises(AttributeError, match="no attribute 'nope'"):
        _leaf().nope


def test_base_persister_scan():
    s = DynamoDbBasePersister(
        db=FakeDb(FakeTable(({"key": "k1", "value": "A"},))),
        table_name=TABLE_NAME,
        key_fields=KEY_FIELDS,
        data_fields=DATA_FIELDS,
    )
    assert list(dynamodol.scan_items(s)) == [("k1", "A")]
