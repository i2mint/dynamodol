"""Offline tests for dynamodol, using an in-memory stand-in for a boto3 table.

These tests exercise the store protocol (reads, membership views, deletes) with
no AWS credentials and no DynamoDB Local: ``FakeDb`` plays the role of the boto3
``dynamodb`` resource and ``FakeTable`` the role of a ``Table``.
"""

import pytest
from botocore.exceptions import ClientError

from dynamodol import DynamoDbBasePersister, DynamoDbPartitionPersister

TABLE_NAME = "test_table"
KEY_FIELDS = ("key",)
DATA_FIELDS = ("value",)

PARTITION_KEY_FIELDS = ("pkey", "skey")
PARTITION = "part1"

ONE_ITEM = ({"key": "k1", "value": "A"},)
ONE_PARTITIONED_ITEM = ({"pkey": PARTITION, "skey": "s1", "value": "A"},)


def _client_error(code, operation):
    """Make a botocore ``ClientError`` shaped the way boto3 raises one."""
    return ClientError(
        {"Error": {"Code": code, "Message": f"simulated {code}"}}, operation
    )


class FakeTable:
    """The subset of the boto3 ``Table`` API that dynamodol actually calls.

    Items are held in a list. ``delete_error``, when given, is raised by
    ``delete_item`` so backend failures can be exercised without a live table.
    """

    def __init__(self, items=(), *, delete_error=None, get_error=None):
        self.items = [dict(item) for item in items]
        self.delete_error = delete_error
        self.get_error = get_error

    @staticmethod
    def _matches(item, key):
        return all(item.get(k) == v for k, v in key.items())

    def get_item(self, Key, **kwargs):
        if self.get_error is not None:
            raise self.get_error
        for item in self.items:
            if self._matches(item, Key):
                return {"Item": dict(item)}
        return {}

    def scan(self, **kwargs):
        if kwargs.get("Select") == "COUNT":
            return {"Count": len(self.items)}
        return {"Items": [dict(item) for item in self.items]}

    def query(self, **kwargs):
        return self.scan(**kwargs)

    def put_item(self, Item):
        self.items = [item for item in self.items if not self._matches(item, Item)]
        self.items.append(dict(Item))

    def delete_item(self, Key, ReturnValues="NONE"):
        """Like boto3: an absent key is deleted silently; ``ALL_OLD`` returns the
        deleted item's ``Attributes`` (and nothing when there was none)."""
        if self.delete_error is not None:
            raise self.delete_error
        old = [item for item in self.items if self._matches(item, Key)]
        self.items = [item for item in self.items if not self._matches(item, Key)]
        if ReturnValues == "ALL_OLD" and old:
            return {"Attributes": dict(old[0])}
        return {}


class FakeDb:
    """A boto3 ``dynamodb`` resource stand-in serving a single ``FakeTable``.

    ``create_table`` always raises, which is the path dynamodol takes when the
    table already exists: it falls back to ``Table(table_name)``.
    """

    def __init__(self, table):
        self._table = table

    def create_table(self, **kwargs):
        raise _client_error("ResourceInUseException", "CreateTable")

    def Table(self, table_name):
        return self._table


def _mk_persister(items=(), *, delete_error=None, get_error=None):
    table = FakeTable(items, delete_error=delete_error, get_error=get_error)
    store = DynamoDbBasePersister(
        db=FakeDb(table),
        table_name=TABLE_NAME,
        key_fields=KEY_FIELDS,
        data_fields=DATA_FIELDS,
    )
    return store, table


def _mk_partition_persister(items=(), *, delete_error=None, get_error=None):
    table = FakeTable(items, delete_error=delete_error, get_error=get_error)
    store = DynamoDbPartitionPersister(
        db=FakeDb(table),
        table_name=TABLE_NAME,
        key_fields=PARTITION_KEY_FIELDS,
        data_fields=DATA_FIELDS,
        partition=PARTITION,
    )
    return store, table


# --- the library must not print --------------------------------------------


def test_getitem_prints_nothing(capsys):
    store, _ = _mk_persister(ONE_ITEM)
    assert store["k1"] == "A"
    assert capsys.readouterr().out == ""


def test_partition_getitem_prints_nothing(capsys):
    store, _ = _mk_partition_persister(ONE_PARTITIONED_ITEM)
    assert store["s1"] == "A"
    assert capsys.readouterr().out == ""


def test_iteration_prints_nothing(capsys):
    store, _ = _mk_persister(ONE_ITEM)
    assert list(store.items()) == [("k1", "A")]
    assert capsys.readouterr().out == ""


# --- membership on the values and items views -------------------------------


def test_values_view_contains():
    store, _ = _mk_persister(ONE_ITEM)
    assert "A" in store.values()
    assert "ZZ" not in store.values()


def test_items_view_contains():
    store, _ = _mk_persister(ONE_ITEM)
    assert ("k1", "A") in store.items()
    assert ("k1", "ZZ") not in store.items()
    assert ("no_such_key", "A") not in store.items()


def test_partition_views_contains():
    store, _ = _mk_partition_persister(ONE_PARTITIONED_ITEM)
    assert "A" in store.values()
    assert "ZZ" not in store.values()
    assert ("s1", "A") in store.items()
    assert ("s1", "ZZ") not in store.items()


# --- __delitem__ must not mask the backend error ----------------------------


def test_base_delitem_propagates_backend_error():
    error = _client_error("ProvisionedThroughputExceededException", "DeleteItem")
    store, _ = _mk_persister(ONE_ITEM, delete_error=error)
    with pytest.raises(ClientError):
        del store["k1"]


def test_partition_delitem_propagates_backend_error():
    error = _client_error("ProvisionedThroughputExceededException", "DeleteItem")
    store, _ = _mk_partition_persister(ONE_PARTITIONED_ITEM, delete_error=error)
    with pytest.raises(ClientError):
        del store["s1"]


@pytest.mark.parametrize(
    "mk_store,key",
    [(_mk_persister, "k1"), (_mk_partition_persister, "s1")],
)
def test_delitem_removes_the_item(mk_store, key):
    items = ONE_ITEM if mk_store is _mk_persister else ONE_PARTITIONED_ITEM
    store, table = mk_store(items)
    del store[key]
    assert table.items == []


# --- a missing key is a KeyError; a backend failure is not -------------------

_BOTH_STORES = pytest.mark.parametrize(
    "mk_store,items,key",
    [
        (_mk_persister, ONE_ITEM, "k1"),
        (_mk_partition_persister, ONE_PARTITIONED_ITEM, "s1"),
    ],
)


@_BOTH_STORES
def test_delitem_of_a_missing_key_raises_key_error(mk_store, items, key):
    """DynamoDB's DeleteItem succeeds silently on an absent key (``NoSuchKey`` is an
    S3 code, never a DynamoDB one), so ``del store[missing]`` used to do nothing."""
    store, table = mk_store(items)
    with pytest.raises(KeyError):
        del store["zzz"]
    assert len(table.items) == 1  # and the present item is untouched


@_BOTH_STORES
def test_getitem_propagates_backend_errors(mk_store, items, key):
    """A throttled or unauthorised read used to become a KeyError, so ``.get`` and
    the items view reported a present key as absent."""
    error = _client_error("ProvisionedThroughputExceededException", "GetItem")
    store, _ = mk_store(items, get_error=error)
    with pytest.raises(ClientError):
        store[key]
    with pytest.raises(ClientError):
        store.get(key)
    with pytest.raises(ClientError):
        (key, "A") in store.items()


@_BOTH_STORES
def test_a_key_the_schema_rejects_is_absent(mk_store, items, key):
    error = _client_error("ValidationException", "GetItem")
    store, _ = mk_store(items, get_error=error)
    with pytest.raises(KeyError):
        store[key]
    assert store.get(key, "DFLT") == "DFLT"


@_BOTH_STORES
def test_missing_key_lookups_stay_key_errors(mk_store, items, key):
    store, _ = mk_store(items)
    assert "zzz" not in store
    assert store.get("zzz", "DFLT") == "DFLT"
    with pytest.raises(KeyError):
        store["zzz"]
