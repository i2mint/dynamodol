"""DynamoDB (through boto3) with a simple (dict-like or list-like) interface"""

import boto3
import botocore.exceptions
from dataclasses import dataclass, field
from decimal import Decimal
from functools import wraps
from lazyprop import lazyprop
from typing import Any, Tuple
from collections.abc import Iterable, ItemsView, Mapping, ValuesView

import threading
from contextlib import contextmanager

from dol import KvReader, KvPersister, Store, inner_most_key


class NoSuchKeyError(KeyError):
    pass


#: Backend error codes that mean "the requested key does not exist".
NO_SUCH_KEY_ERROR_CODES = frozenset({"NoSuchKey"})


def is_no_such_key_error(error: BaseException) -> bool:
    """Tell whether ``error`` reports a missing key.

    Boto/botocore carry the backend's error code in
    ``error.response['Error']['Code']``. Note that exception *instances* never
    have a ``__name__`` (that lives on the class), so testing ``error.__name__``
    only raises ``AttributeError`` and hides the error being inspected.

    >>> is_no_such_key_error(ValueError("nope"))
    False
    >>> from botocore.exceptions import ClientError
    >>> is_no_such_key_error(
    ...     ClientError({"Error": {"Code": "NoSuchKey"}}, "DeleteItem")
    ... )
    True
    """
    response = getattr(error, "response", None)
    if isinstance(response, Mapping):
        error_info = response.get("Error") or {}
        if isinstance(error_info, Mapping):
            if error_info.get("Code") in NO_SUCH_KEY_ERROR_CODES:
                return True
    return type(error).__name__ in NO_SUCH_KEY_ERROR_CODES


#: Backend error codes that mean "this key cannot name an item" (wrong type, empty
#: string, wrong arity for the key schema): such a key is absent, not a backend failure.
KEY_CANNOT_NAME_AN_ITEM_ERROR_CODES = frozenset({"ValidationException"})


def get_item_or_raise(table, key, k, *, error_cls=NoSuchKeyError, **get_item_kwargs):
    """``table.get_item(Key=key)``'s ``Item``, raising ``error_cls`` only if it is absent.

    Backend failures (throttling, credentials, a missing table) propagate unchanged.
    A key the table's schema rejects outright is reported absent, as before.
    """
    try:
        response = table.get_item(Key=key, **get_item_kwargs)
    except Exception as e:
        code = (getattr(e, "response", None) or {}).get("Error", {}).get("Code")
        if code in KEY_CANNOT_NAME_AN_ITEM_ERROR_CODES:
            raise error_cls(f"Key not found: {k}") from e
        raise
    return _item_or_raise(response, k, error_cls=error_cls)


def _item_or_raise(get_item_response, k, *, error_cls=NoSuchKeyError):
    """The ``Item`` of a ``get_item`` response, or ``NoSuchKeyError`` if there is none.

    DynamoDB answers a lookup of an absent key with a response that simply has no
    ``Item`` -- that, and only that, is "missing". A throttled request, bad
    credentials or a missing table are backend errors and must surface as such, not
    as a ``KeyError`` that makes ``k in store`` and ``store.get(k)`` quietly say
    "absent".

    >>> _item_or_raise({"Item": {"key": "k1"}}, "k1")
    {'key': 'k1'}
    >>> _item_or_raise({}, "k1")
    Traceback (most recent call last):
      ...
    dynamodol.base.NoSuchKeyError: 'Key not found: k1'
    """
    if "Item" not in get_item_response:
        raise error_cls(f"Key not found: {k}")
    return get_item_response["Item"]


def raise_if_nothing_was_deleted(delete_item_response, k, *, error_cls=NoSuchKeyError):
    """Raise ``NoSuchKeyError`` if a ``ReturnValues='ALL_OLD'`` delete removed nothing.

    DynamoDB's ``DeleteItem`` succeeds silently on an absent key -- it never reports a
    ``NoSuchKey`` code (that is S3's) -- so the only sign the key was missing is that
    no old ``Attributes`` came back. ``del store[missing]`` must raise ``KeyError``.

    >>> raise_if_nothing_was_deleted({"Attributes": {"key": "k1"}}, "k1")
    >>> raise_if_nothing_was_deleted({}, "k1")
    Traceback (most recent call last):
      ...
    dynamodol.base.NoSuchKeyError: 'Key not found: k1'
    """
    if "Attributes" not in (delete_item_response or {}):
        raise error_cls(f"Key not found: {k}")


DFLT_TABLE_NAME = "dynamodol"
DFLT_KEY_FIELDS = ("key",)
DFLT_DATA_FIELDS = ("value",)

db_defaults = {
    "table_name": DFLT_TABLE_NAME,
    "key_fields": DFLT_KEY_FIELDS,
    "data_fields": DFLT_DATA_FIELDS,
    "projection": None,
}


def get_db(
    aws_access_key_id="",
    aws_secret_access_key="",
    aws_session_token="",
    region_name="",
    endpoint_url="http://localhost:8000",
):
    resource_kwargs = (
        {"region_name": region_name} if region_name else {"endpoint_url": endpoint_url}
    )
    if aws_access_key_id:
        resource_kwargs["aws_access_key_id"] = aws_access_key_id
        resource_kwargs["aws_secret_access_key"] = aws_secret_access_key
        if aws_session_token:
            resource_kwargs["aws_session_token"] = aws_session_token
    return boto3.resource("dynamodb", **resource_kwargs)


def decimal_to_float(x):
    if isinstance(x, str):
        return x
    if isinstance(x, Mapping):
        return {k: decimal_to_float(v) for k, v in x.items()}
    if isinstance(x, Iterable):
        return [decimal_to_float(v) for v in x]
    if isinstance(x, Decimal):
        return float(x)
    return x


def paginated(call, /, **kwargs):
    """Yield the ``Items`` of a ``scan``/``query`` across *all* its pages.

    DynamoDB returns at most ~1 MB per call and says where it stopped
    (``LastEvaluatedKey``); a single call silently truncates a bigger table.
    """
    while True:
        response = call(**kwargs)
        yield from response["Items"]
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            return
        kwargs = {**kwargs, "ExclusiveStartKey": last_key}


def paginated_count(call, /, **kwargs) -> int:
    """The ``Count`` of a ``Select='COUNT'`` ``scan``/``query`` summed over all its pages."""
    total = 0
    while True:
        response = call(**kwargs)
        total += response["Count"]
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            return total
        kwargs = {**kwargs, "ExclusiveStartKey": last_key}


BATCH_GET_MAX_KEYS = 100  # DynamoDB's limit for one BatchGetItem call


def _chunks(items, size):
    """Consecutive lists of at most ``size`` of the (lazily consumed) ``items``."""
    chunk = []
    for item in items:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _leaf_reader_of(store):
    """The ``DynamoDbBaseReader`` under any number of ``dol`` wrappers (``None`` if none)."""
    seen = set()
    while store is not None and id(store) not in seen:
        if isinstance(store, DynamoDbBaseReader):
            return store
        seen.add(id(store))
        store = getattr(store, "store", None)


class _BatchedViewMixin:
    """Iterate ``(key, value)`` pairs of the caller's store, a batch of keys per backend call.

    ``Mapping`` builds ``items()``/``values()`` from ``store[key]`` for every key, i.e. one
    ``GetItem`` per row. Here the keys of a batch are resolved to the reader's own keys
    (``dol.inner_most_key``), their items are read by one ``BatchGetItem``, and only while
    ``store[key]`` runs are lookups of those keys answered from them. The lookups still go
    through the caller's own store, so key/value codecs, ``filt_iter`` scopes and the
    reader's prefix/partition scoping keep applying (i2mint/dynamodol#4); nothing is
    cached while the caller's code runs between pairs. A key the batch did not return is
    looked up with ``GetItem`` as usual.
    """

    def _pairs(self):
        mapping = self._mapping
        reader = _leaf_reader_of(mapping)
        if reader is None:
            for key in mapping:
                yield key, mapping[key]
            return
        for keys in _chunks(mapping, BATCH_GET_MAX_KEYS):
            inner_keys = (
                keys
                if mapping is reader
                else [inner_most_key(mapping, key, default=None) for key in keys]
            )
            items = reader._batch_get(inner_keys)
            for key in keys:
                with reader._using_items(items):
                    value = mapping[key]
                yield key, value


class _BatchedItemsView(_BatchedViewMixin, ItemsView):
    def __iter__(self):
        yield from self._pairs()


class _BatchedValuesView(_BatchedViewMixin, ValuesView):
    def __iter__(self):
        for _, value in self._pairs():
            yield value


class _RemovedMethod:
    """Attribute that raises an ``AttributeError`` naming the method's replacement.

    A descriptor (rather than a ``__getattr__``) so it cannot mask unrelated
    ``AttributeError``s raised inside the reader's own properties.
    """

    def __init__(self, name, replacement):
        self.name, self.replacement = name, replacement

    def __get__(self, instance, owner=None):
        if instance is None:  # class access (dol's class-wrap enumerates dir(cls))
            return self
        raise AttributeError(
            f"`{self.name}` was removed: as a method it ran bound to the inner store, "
            f"so on a wrapped store (key codec, filt_iter...) it returned out-of-scope "
            f"keys and raw values (i2mint/dynamodol#1). `.items()` / `.values()` are "
            f"now correct on wrapped stores; for a single-scan fast path on an "
            f"unwrapped store use {self.replacement}."
        )


@dataclass
class DynamoDbBaseReader(KvReader):
    """A basic key-value reader for DynamoDb

    All properties will be filled in by defaults if not provided.

    :property db: A boto3 DynamoDB resource object.
    :property table_name: The name of the table to access.
    :property key_fields: A tuple of length 1 or 2 with the table's partition key and (if present) sort key
    :property data_fields: A tuple listing the data keys to retrieve with __getitem__.
        If data_fields is length 0, all of the keys and values of the document will be returned as a dict.
        If data_fields is length 1, the value of that field will be returned as a string.
        If data_fields is length 2 or greater, the values in those fields will be returned as a tuple.
    :property exclude_keys_on_read: If data_fields is empty, this flag specifies whether to exclude
        the partition key (and sort key if applicable) from the output dict.

    Keys are strings if the table has only a partition key, or tuples if the table has a partition key and a sort key.

    >>> from dynamodol.base import load_sample_data
    >>> load_sample_data()
    >>> reader = DynamoDbBaseReader()
    >>> reader[('part1', '01-01')]
    >>> ('a', 'bcde')

    MAJOR TODO: boto3 for DynamoDB casts all numbers to a Decimal type. We need to add a significant amount
    of mapping code to transform values between Decimal and Python int and float types when reading and writing.
    This library is currently only useful for tables that exclusively use string values.
    """

    db: Any = field(default=None)
    table_name: str = field(default=None)
    key_fields: tuple[str] = field(default=None)
    data_fields: tuple[str] = field(default=None)
    exclude_keys_on_read: bool = field(default=True)

    def __post_init__(self):
        for k, v in db_defaults.items():
            if getattr(self, k, None) is None:
                setattr(self, k, v)
        if not self.db:
            self.db = get_db()
        if isinstance(self.data_fields, str):
            self.data_fields = (self.data_fields,)
        if isinstance(self.key_fields, str):
            self.key_fields = (self.key_fields,)
        if isinstance(self.projection, list):
            self.projection = ",".join(self.projection)

    def __reversed__(self):
        return list(self)[::-1]

    @lazyprop
    def table(self):
        key_schema = [{"AttributeName": self.partition_key, "KeyType": "HASH"}]
        if self.sort_key:
            key_schema.append({"AttributeName": self.sort_key, "KeyType": "RANGE"})
        attribute_definition = [
            {"AttributeName": k, "AttributeType": "S"} for k in self.key_fields if k
        ]

        try:
            table = self.db.create_table(
                TableName=self.table_name,
                KeySchema=key_schema,
                AttributeDefinitions=attribute_definition,
                ProvisionedThroughput={
                    "ReadCapacityUnits": 5,
                    "WriteCapacityUnits": 5,
                },
            )
            # Wait until the table creation is complete.
            self.db.meta.client.get_waiter("table_exists").wait(
                TableName=self.table_name
            )
            print(f"Table {self.table_name} has been created.")
        except botocore.exceptions.ClientError as e:
            table = self.db.Table(self.table_name)
            pass
        return table

    @property
    def partition_key(self):
        return self.key_fields[0]

    @property
    def sort_key(self):
        if len(self.key_fields) < 2:
            return None
        return self.key_fields[1]

    def extract_obj_from_data(self, item):
        if self.data_fields:
            if len(self.data_fields) == 1:
                return item[self.data_fields[0]]
            return tuple([item[k] for k in self.data_fields])
        if self.exclude_keys_on_read:
            return {x: item[x] for x in item if x not in self.key_fields}
        return item

    def format_get_item(self, item):
        """TODO: replace with _id_of_key, etc."""
        obj = self.extract_obj_from_data(item)
        return decimal_to_float(obj)

    def format_get_key(self, item):
        """TODO: replace with _id_of_key, etc."""
        if len(self.key_fields) == 2:
            return item[self.key_fields[0]], item[self.key_fields[1]]
        return item[self.key_fields[0]]

    @property
    def _keys_expression(self):
        return {
            "ExpressionAttributeNames": {
                f"#{index}": key for index, key in enumerate(self.key_fields)
            },
            "ProjectionExpression": ", ".join(
                [f"#{i}" for i in range(len(self.key_fields))]
            ),
        }

    @property
    def _values_expression(self):
        if not self.data_fields:
            return {}
        return {
            "ExpressionAttributeNames": {
                f"#{index}": key for index, key in enumerate(self.data_fields)
            },
            "ProjectionExpression": ", ".join(
                [f"#{i}" for i in range(len(self.data_fields))]
            ),
        }

    @property
    def _keys_values_expression(self):
        if not self.data_fields:
            return {}
        all_fields = [*self.key_fields, *self.data_fields]
        return {
            "ExpressionAttributeNames": {
                f"#{index}": key for index, key in enumerate(all_fields)
            },
            "ProjectionExpression": ", ".join(
                [f"#{i}" for i in range(len(all_fields))]
            ),
        }

    ItemsView = _BatchedItemsView
    ValuesView = _BatchedValuesView
    _no_such_key_error = NoSuchKeyError  # what a missing key raises (subclasses: their own)

    def _primary_key(self, k) -> dict:
        """The item's primary key (``{key field: value}``) that the store key ``k`` names.

        Raises ``NoSuchKeyError`` for a key of the wrong shape.
        """
        try:
            _k = k
            if isinstance(k, str):
                if self.sort_key:
                    raise ValueError(
                        "If a sort key is defined, object keys must be tuples."
                    )
                _k = (k,)
            return {att: key for att, key in zip(self.key_fields, _k)}
        except (ValueError, TypeError) as e:
            raise NoSuchKeyError(f"Key not found: {k}") from e

    def _ident(self, primary_key):
        return tuple(primary_key[f] for f in self.key_fields)

    @property
    def _prefetched_items(self):
        """``{identity: item}`` while *this thread's* view looks up the items it just read."""
        return getattr(self.__dict__.setdefault("_local", threading.local()), "items", None)

    @_prefetched_items.setter
    def _prefetched_items(self, items):
        self.__dict__.setdefault("_local", threading.local()).items = items

    def _batch_get(self, keys) -> dict:
        """``{identity: item}`` for the store keys ``keys``, by ``BatchGetItem`` (<= 100 per call).

        Keys that name no item, or that the backend does not return, are simply absent
        (they are looked up one by one later).
        """
        wanted = {}
        for k in keys:
            try:
                primary_key = self._primary_key(k)
                wanted[self._ident(primary_key)] = primary_key
            except (NoSuchKeyError, KeyError, TypeError):
                continue
        items = {}
        for part in _chunks(wanted.values(), BATCH_GET_MAX_KEYS):
            request = {"Keys": part, **self._keys_values_expression}
            try:
                response = self.db.batch_get_item(RequestItems={self.table_name: request})
            except Exception as e:
                code = (getattr(e, "response", None) or {}).get("Error", {}).get("Code")
                if code in KEY_CANNOT_NAME_AN_ITEM_ERROR_CODES:
                    continue
                raise
            for item in response.get("Responses", {}).get(self.table_name, []):
                items[self._ident(item)] = item
        return items

    @contextmanager
    def _using_items(self, items):
        previous, self._prefetched_items = self._prefetched_items, items
        try:
            yield
        finally:
            self._prefetched_items = previous

    def _item_of_primary_key(self, primary_key, k):
        if self._prefetched_items:
            item = self._prefetched_items.get(self._ident(primary_key))
            if item is not None:
                return item
        return get_item_or_raise(
            self.table,
            primary_key,
            k,
            error_cls=self._no_such_key_error,
            **self._values_expression,
        )

    def __getitem__(self, k):
        primary_key = self._primary_key(k)
        return self.format_get_item(self._item_of_primary_key(primary_key, k))

    def _scan_records(self, projection):
        """The raw records for a ``projection`` (an ``Expression*`` kwargs dict)."""
        return list(paginated(self.table.scan, **projection))

    iter_items = _RemovedMethod("iter_items", "dynamodol.scan_items(store)")
    iter_values = _RemovedMethod("iter_values", "dynamodol.scan_values(store)")

    def __iter__(self):
        # This is extremely inefficient and should not be used with large tables in production
        for record in paginated(self.table.scan, **self._keys_expression):
            yield self.format_get_key(record)

    def __len__(self):
        # This is extremely inefficient and should not be used with large tables in production
        return paginated_count(self.table.scan, Select="COUNT")

    @staticmethod
    @wraps(get_db)
    def mk_db(**db_kwargs):
        return get_db(**db_kwargs)


class DynamoDbBasePersister(DynamoDbBaseReader, KvPersister):
    """
    A basic DynamoDb persister.
    >>> s = DynamoDbBasePersister(table_name=DFLT_TABLE_NAME, key_fields=DFLT_KEY_FIELDS, data_fields=())
    >>> k = '777' # Each collection will happily accept user-defined _key values.
    >>> v = {'val': 'bar'}
    >>> for _key in s:
    ...     del s[_key]
    ...
    >>> k in s
    False
    >>> len(s)
    0
    >>> s[k] = v
    >>> len(s)
    1
    >>> s[k]
    {'val': 'bar'}
    >>> s.get(k)
    {'val': 'bar'}
    >>> s.get('does_not_exist', {'default': 'val'})  # testing s.get with default
    {'default': 'val'}
    >>> list(s.values())
    [{'val': 'bar'}]
    >>> k in s  # testing __contains__ again
    True
    >>> del s[k]
    >>> len(s)
    0
    >>> s = DynamoDbPersister(table_name='dynamodol2', key_fields=('name',))
    >>> for _key in s:
    ...   del s[_key]
    >>> len(s)
    0
    >>> s['guido'] = {'yob': 1956, 'proj': 'python', 'bdfl': False}
    >>> s['guido']
    {'proj': 'python', 'yob': Decimal('1956'), 'bdfl': False}
    >>> s['vitalik'] = {'yob': 1994, 'proj': 'ethereum', 'bdfl': True}
    >>> s['vitalik']
    {'proj': 'ethereum', 'yob': Decimal('1994'), 'bdfl': True}
    >>> for key, val in s.items():
    ...   print(f"{key}: {val}")
    'vitalik': {'proj': 'ethereum', 'yob': Decimal('1994'), 'bdfl': True}
    'guido': {'proj': 'python', 'yob': Decimal('1956'), 'bdfl': False}
    """

    def __setitem__(self, k, v):
        if isinstance(k, str):
            if self.sort_key:
                raise ValueError(
                    "If a sort key is defined, object keys must be tuples."
                )
            else:
                k = (k,)
        key = {att: key for att, key in zip(self.key_fields, k)}
        if isinstance(v, dict):
            val = v
        else:
            if isinstance(v, str):
                v = (v,)
            val = {att: key for att, key in zip(self.data_fields, v)}

        self.table.put_item(Item={**key, **val})

    def __delitem__(self, k):
        try:
            if isinstance(k, str):
                if self.sort_key:
                    raise ValueError(
                        "If a sort key is defined, object keys must be tuples."
                    )
                k = (k,)
            key = {att: key for att, key in zip(self.key_fields, k)}
            response = self.table.delete_item(Key=key, ReturnValues="ALL_OLD")
        except Exception as e:
            if is_no_such_key_error(e):
                raise NoSuchKeyError(f"Key not found: {k}") from e
            raise
        raise_if_nothing_was_deleted(response, k)


def set_db_defaults(new_defaults: dict):
    """Sets global defaults for dynamodol so stores can be created without explicitly passing table details every time.

    :param new_defaults: A dict containing one or more of the following keys
        table_name: str - The name of the table
        key_fields: Tuple - A tuple of length 1 or 2 containing the partition key and (optional) sort key for the table
        data_fields: Tuple or None - A tuple of data fields to return from queries. If data_fields is None, data
                                     will be returned as dicts instead of tuples.
    """
    for k, v in new_defaults.items():
        db_defaults[k] = v


def load_sample_data():
    """For supporting doctests"""
    set_db_defaults(
        {
            "table_name": "sorted_table",
            "key_fields": ("partitionkey", "sortkey"),
            "data_fields": ("data", "moredata"),
            "partition": "part1",
        }
    )
    sorted_persister = DynamoDbBasePersister()
    for k in list(sorted_persister):
        del sorted_persister[k]
    sorted_persister[("part1", "sort2")] = ("val2", "moreval2", None)
    sorted_persister[("part1", "01-01")] = ("a", "bcde")
    sorted_persister[("part1", "01-02")] = ("c", "defg")
    sorted_persister[("part1", "01-03")] = ("e", "fghi")
    sorted_persister[("part1", "01-04")] = ("a", "cdef")
    sorted_persister[("part1", "02-01")] = ("g", "hijk")
    sorted_persister[("part1", "03-02")] = ("i", "jklm")
    sorted_persister[("part1", "04-03")] = ("k", "lmno")
    sorted_persister[("part2", "02-01")] = ("m", "nopq")
    sorted_persister[("part2", "03-02")] = ("o", "pqrs")
    sorted_persister[("part2", "04-03")] = ("q", "rstu")
    sorted_persister[("part3", "01-05")] = ("s", "tuvw")
    sorted_persister[("part3", "02-02")] = ("u", "vwxy")
    sorted_persister[("part3", "03-03")] = ("w", "xyza")


# TODO class DynamoDbStore(DynamoDbBasePersister, Store): ...


def _leaf_reader(store, *, fn):
    """``store`` if it is an unwrapped dynamodol reader (the single-scan fast path),
    ``None`` if it is any other mapping (a dol-wrapped store: use its own ``.items()`` /
    ``.values()``, which apply the wrapper's scope and codecs); ``TypeError`` otherwise."""
    if isinstance(store, DynamoDbBaseReader):
        return store
    if isinstance(store, Mapping):
        return None
    raise TypeError(
        f"`{fn}` needs a dynamodol reader (bare, or wrapped with a key codec, "
        f"filt_iter, value codec...), got {type(store).__name__}."
    )


def scan_items(store):
    """Iterate ``(key, value)`` pairs of a dynamodol store.

    A bare dynamodol reader is read in one scan: the opt-in fast path that
    ``store.items()`` deliberately is not, since ``items()`` batches ``BatchGetItem``
    (100 keys per call) so that it is correct under any dol wrapper. A *wrapped* store
    (key codec, ``filt_iter``, value codec...) has its own mapping, scope and decoding,
    which a free function cannot reproduce from the table: for it this is simply
    ``iter(store.items())``. Anything that is not a mapping raises ``TypeError``.

    >>> import dynamodol
    >>> dynamodol.scan_items(object())
    Traceback (most recent call last):
      ...
    TypeError: `scan_items` needs a dynamodol reader (bare, or wrapped with a key codec, filt_iter, value codec...), got object.
    """
    reader = _leaf_reader(
        store, fn="scan_items"
    )  # eager: fail at the call, not at first next()
    if reader is None:
        return iter(store.items())
    records = reader._scan_records(reader._keys_values_expression)
    return (
        (reader.format_get_key(record), reader.format_get_item(record))
        for record in records
    )


def scan_values(store):
    """Iterate the values of a dynamodol store (see `scan_items`: a wrapped store is
    iterated with its own ``.values()``)."""
    reader = _leaf_reader(store, fn="scan_values")
    if reader is None:
        return iter(store.values())
    records = reader._scan_records(reader._values_expression)
    return (reader.format_get_item(record) for record in records)
