"""Pagination of scan/query, and batched ``items()``/``values()`` (i2mint/dynamodol#4)."""

import pytest
from dol import KeyCodecs, Pipe, filt_iter, wrap_kvs

import dynamodol
from dynamodol import (
    DynamoDbBasePersister,
    DynamoDbPartitionPersister,
    DynamoDbPrefixReader,
)

from test_offline import DATA_FIELDS, KEY_FIELDS, TABLE_NAME, FakeDb, FakeTable

N = 250


def _rows(n=N):
    return [{"key": f"k{i:04d}", "value": f"v{i}"} for i in range(n)]


def _base(rows=None, *, page_size=None):
    table = FakeTable(_rows() if rows is None else rows, page_size=page_size)
    store = DynamoDbBasePersister(
        db=FakeDb(table), table_name=TABLE_NAME, key_fields=KEY_FIELDS, data_fields=DATA_FIELDS
    )
    return store, table


def _prefix_reader(n=N, *, page_size=None):
    rows = [
        {"pkey": "part1", "skey": f"v1/alice/{i:04d}", "value": f"A{i}"} for i in range(n)
    ] + [{"pkey": "part1", "skey": f"v1/bob/{i:04d}", "value": f"B{i}"} for i in range(n)]
    table = FakeTable(rows, page_size=page_size)
    store = DynamoDbPrefixReader(
        db=FakeDb(table),
        table_name=TABLE_NAME,
        key_fields=("pkey", "skey"),
        data_fields=DATA_FIELDS,
        partition="part1",
        prefix="v1/",
    )
    return store, table


def _count(table, op):
    return sum(1 for name, _ in table.calls if name == op)


# ---- pagination -------------------------------------------------------------------


def test_scan_reads_every_page():
    store, _ = _base(page_size=40)
    assert len(list(store)) == N == len(store)
    assert len(list(dynamodol.scan_items(store))) == N
    assert len(list(dynamodol.scan_values(store))) == N


def test_query_reads_every_page():
    store, table = _prefix_reader(page_size=60)
    assert len(list(store)) == 2 * N == len(store)  # the fake query does not filter


def test_partition_persister_pages():
    table = FakeTable(
        [{"pkey": "p", "skey": f"s{i:03d}", "value": "x"} for i in range(30)], page_size=7
    )
    store = DynamoDbPartitionPersister(
        db=FakeDb(table),
        table_name=TABLE_NAME,
        key_fields=("pkey", "skey"),
        data_fields=DATA_FIELDS,
        partition="p",
    )
    assert len(list(store)) == 30 == len(store)


# ---- batched items()/values() ------------------------------------------------------


def test_bare_items_and_values_use_batches_not_one_get_per_key():
    store, table = _base()
    expected = {r["key"]: r["value"] for r in _rows()}
    assert dict(store.items()) == expected
    assert sorted(store.values()) == sorted(expected.values())
    assert _count(table, "get_item") == 0
    assert _count(table, "batch_get_item") <= 2 * 3  # 250 keys = 3 calls, twice


@pytest.mark.parametrize(
    "mk",
    [
        lambda store: wrap_kvs(store, key_of_id=str.upper, id_of_key=str.lower),
        lambda store: wrap_kvs(store, obj_of_data=lambda v: v.upper()),
        lambda store: filt_iter(store, filt=lambda k: k.endswith("7")),
        lambda store: Pipe(
            filt_iter(filt=lambda k: k.endswith("7")),
            wrap_kvs(key_of_id=str.upper, id_of_key=str.lower),
        )(store),
    ],
    ids=["key codec", "value codec", "filt_iter", "filt + codec"],
)
def test_wrapped_items_are_correct_and_batched(mk):
    store, table = _base()
    view = mk(store)
    per_key = {k: view[k] for k in view}  # the one-get-per-key answer
    table.calls.clear()
    assert dict(view.items()) == per_key
    assert sorted(view.values()) == sorted(per_key.values())
    assert _count(table, "get_item") == 0 and per_key


def test_prefix_reader_scope_is_applied_through_batches():
    store, table = _prefix_reader()
    alice = Pipe(filt_iter.prefixes("alice/"), KeyCodecs.prefixed("alice/"))(store)
    table.calls.clear()
    got = dict(alice.items())
    assert len(got) == N and got["0007"] == "A7"
    assert all(v.startswith("A") for v in got.values())  # bob's rows never come back
    assert _count(table, "get_item") == 0


def test_an_item_the_batch_did_not_return_is_looked_up_singly():
    store, table = _base(rows=_rows(5))
    original = table.items

    def batch_drops_k2(RequestItems):
        ((name, request),) = RequestItems.items()
        found = [dict(i) for key in request["Keys"] for i in original if table._matches(i, key)]
        return {"Responses": {name: [i for i in found if i["key"] != "k0002"]}}

    store.db.batch_get_item = batch_drops_k2
    assert dict(store.items()) == {r["key"]: r["value"] for r in _rows(5)}
    assert ("get_item", {"key": "k0002"}) in table.calls


def test_a_key_deleted_after_the_batch_is_a_snapshot_but_plain_lookups_are_exact():
    store, table = _base(rows=_rows(5))
    for key, _ in store.items():  # the batch was read when the iteration began
        if key == "k0000":
            table.items = [i for i in table.items if i["key"] != "k0003"]
    with pytest.raises(KeyError):
        store["k0003"]  # a fresh lookup is exact


def test_batch_is_only_active_during_the_views_own_lookup():
    store, table = _base(rows=_rows(5))
    it = iter(store.items())
    next(it)
    assert store._prefetched_items is None  # nothing cached while the caller's code runs
    table.items = [dict(i, value="CHANGED") for i in table.items]
    assert store["k0001"] == "CHANGED"  # plain lookups are never served from the batch
    it.close()


def test_batch_get_validation_error_falls_back_to_plain_lookups():
    from test_offline import _client_error

    store, table = _base(rows=_rows(5))

    def refuse(RequestItems):
        raise _client_error("ValidationException", "BatchGetItem")

    store.db.batch_get_item = refuse
    assert dict(store.items()) == {r["key"]: r["value"] for r in _rows(5)}


def test_batch_get_backend_error_propagates():
    from test_offline import _client_error

    store, _ = _base(rows=_rows(5))

    def throttled(RequestItems):
        raise _client_error("ProvisionedThroughputExceededException", "BatchGetItem")

    store.db.batch_get_item = throttled
    with pytest.raises(Exception, match="Throughput"):
        list(store.items())


def test_items_are_read_lazily_not_all_keys_and_a_full_count_up_front():
    store, table = _base(rows=_rows(6), page_size=1)
    next(iter(store.items()))
    scans = [c for c in table.calls if c[0] == "scan"]
    assert len(scans) <= 6 and all(kw.get("Select") != "COUNT" for _, kw in scans)


def test_a_batch_in_one_thread_is_invisible_to_others_and_never_left_behind():
    import threading

    store, table = _base(rows=_rows(3))
    entered, release, seen = threading.Event(), threading.Event(), {}

    def hold():  # thread A: inside its lookup, holding its batch
        with store._using_items({("k0000",): {"key": "k0000", "value": "STALE"}}):
            entered.set()
            release.wait(5)

    t = threading.Thread(target=hold)
    t.start()
    entered.wait(5)
    seen["other_thread"] = store["k0000"]  # must not be served from A's batch
    release.set()
    t.join()
    assert seen["other_thread"] == "v0" and store._prefetched_items is None
    del store["k0000"]  # (the fake's put_item matches whole items, so replace by delete+put)
    store["k0000"] = "NEW"
    assert store["k0000"] == "NEW"
