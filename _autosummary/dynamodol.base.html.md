# dynamodol.base

DynamoDB (through boto3) with a simple (dict-like or list-like) interface

### Module Attributes

| [`NO_SUCH_KEY_ERROR_CODES`](#dynamodol.base.NO_SUCH_KEY_ERROR_CODES)             | Backend error codes that mean "the requested key does not exist".                                                                                                     |
|--------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`KEY_CANNOT_NAME_AN_ITEM_ERROR_CODES`](#dynamodol.base.KEY_CANNOT_NAME_AN_ITEM_ERROR_CODES) | Backend error codes that mean "this key cannot name an item" (wrong type, empty string, wrong arity for the key schema): such a key is absent, not a backend failure. |

### Functions

| `decimal_to_float`(x)                                                                              |                                                                                                                  |
|----------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|
| `get_db`([aws_access_key_id, ...])                                                                 |                                                                                                                  |
| [`get_item_or_raise`](#dynamodol.base.get_item_or_raise)(table, key, k, \*[, error_cls]) | `table.get_item(Key=key)`'s `Item`, raising `error_cls` only if it is absent.                                    |
| [`is_no_such_key_error`](#dynamodol.base.is_no_such_key_error)(error)                       | Tell whether `error` reports a missing key.                                                                      |
| [`load_sample_data`](#dynamodol.base.load_sample_data)()                                | For supporting doctests                                                                                          |
| [`raise_if_nothing_was_deleted`](#dynamodol.base.raise_if_nothing_was_deleted)(...[, error_cls])    | Raise `NoSuchKeyError` if a `ReturnValues='ALL_OLD'` delete removed nothing.                                     |
| [`set_db_defaults`](#dynamodol.base.set_db_defaults)(new_defaults)                     | Sets global defaults for dynamodol so stores can be created without explicitly passing table details every time. |

### Classes

| [`DynamoDbBasePersister`](#dynamodol.base.DynamoDbBasePersister)([db, table_name, ...])   | A basic DynamoDb persister.           |
|-------------------------------------------------------------------------------------------------|---------------------------------------|
| [`DynamoDbBaseReader`](#dynamodol.base.DynamoDbBaseReader)([db, table_name, ...])      | A basic key-value reader for DynamoDb |

### Exceptions

| [`NoSuchKeyError`](#dynamodol.base.NoSuchKeyError)   |    |
|-------------------------------------------------------------------|----|

### *class* dynamodol.base.DynamoDbBasePersister(db=None, table_name=None, key_fields=None, data_fields=None, exclude_keys_on_read=True)

Bases: [`DynamoDbBaseReader`](#dynamodol.base.DynamoDbBaseReader), `KvPersister`

A basic DynamoDb persister.

```pycon
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
```

### *class* dynamodol.base.DynamoDbBaseReader(db=None, table_name=None, key_fields=None, data_fields=None, exclude_keys_on_read=True)

Bases: `KvReader`

A basic key-value reader for DynamoDb

All properties will be filled in by defaults if not provided.

* **Property db:**
  A boto3 DynamoDB resource object.
* **Property table_name:**
  The name of the table to access.
* **Property key_fields:**
  A tuple of length 1 or 2 with the table’s partition key and (if present) sort key
* **Property data_fields:**
  A tuple listing the data keys to retrieve with \_\_getitem_\_.
  If data_fields is length 0, all of the keys and values of the document will be returned as a dict.
  If data_fields is length 1, the value of that field will be returned as a string.
  If data_fields is length 2 or greater, the values in those fields will be returned as a tuple.
* **Property exclude_keys_on_read:**
  If data_fields is empty, this flag specifies whether to exclude
  the partition key (and sort key if applicable) from the output dict.

Keys are strings if the table has only a partition key, or tuples if the table has a partition key and a sort key.

```pycon
>>> from dynamodol.base import load_sample_data
>>> load_sample_data()
>>> reader = DynamoDbBaseReader()
>>> reader[('part1', '01-01')]
>>> ('a', 'bcde')
```

MAJOR TODO: boto3 for DynamoDB casts all numbers to a Decimal type. We need to add a significant amount
of mapping code to transform values between Decimal and Python int and float types when reading and writing.
This library is currently only useful for tables that exclusively use string values.

#### *class* ItemsView(mapping)

Bases: [`ItemsView`](https://docs.python.org/3/library/collections.abc.html#collections.abc.ItemsView)

Items view backed by a single table scan (see `iter_items`).

#### *class* ValuesView(mapping)

Bases: [`ValuesView`](https://docs.python.org/3/library/collections.abc.html#collections.abc.ValuesView)

Values view backed by a single table scan (see `iter_values`).

#### format_get_item(item)

#### format_get_key(item)

### dynamodol.base.KEY_CANNOT_NAME_AN_ITEM_ERROR_CODES *= frozenset({'ValidationException'})*

Backend error codes that mean “this key cannot name an item” (wrong type, empty
string, wrong arity for the key schema): such a key is absent, not a backend failure.

### dynamodol.base.NO_SUCH_KEY_ERROR_CODES *= frozenset({'NoSuchKey'})*

Backend error codes that mean “the requested key does not exist”.

### *exception* dynamodol.base.NoSuchKeyError

Bases: [`KeyError`](https://docs.python.org/3/builtins/exceptions.html#KeyError)

### dynamodol.base.get_item_or_raise(table, key, k, \*, error_cls=<class 'dynamodol.base.NoSuchKeyError'>, \*\*get_item_kwargs)

`table.get_item(Key=key)`’s `Item`, raising `error_cls` only if it is absent.

Backend failures (throttling, credentials, a missing table) propagate unchanged.
A key the table’s schema rejects outright is reported absent, as before.

### dynamodol.base.is_no_such_key_error(error)

Tell whether `error` reports a missing key.

Boto/botocore carry the backend’s error code in
`error.response['Error']['Code']`. Note that exception *instances* never
have a `__name__` (that lives on the class), so testing `error.__name__`
only raises `AttributeError` and hides the error being inspected.

* **Return type:**
  [`bool`](https://docs.python.org/3/builtins/functions.html#bool)

```pycon
>>> is_no_such_key_error(ValueError("nope"))
False
>>> from botocore.exceptions import ClientError
>>> is_no_such_key_error(
...     ClientError({"Error": {"Code": "NoSuchKey"}}, "DeleteItem")
... )
True
```

### dynamodol.base.load_sample_data()

For supporting doctests

### dynamodol.base.raise_if_nothing_was_deleted(delete_item_response, k, \*, error_cls=<class 'dynamodol.base.NoSuchKeyError'>)

Raise `NoSuchKeyError` if a `ReturnValues='ALL_OLD'` delete removed nothing.

DynamoDB’s `DeleteItem` succeeds silently on an absent key – it never reports a
`NoSuchKey` code (that is S3’s) – so the only sign the key was missing is that
no old `Attributes` came back. `del store[missing]` must raise `KeyError`.

```pycon
>>> raise_if_nothing_was_deleted({"Attributes": {"key": "k1"}}, "k1")
>>> raise_if_nothing_was_deleted({}, "k1")
Traceback (most recent call last):
  ...
dynamodol.base.NoSuchKeyError: 'Key not found: k1'
```

### dynamodol.base.set_db_defaults(new_defaults)

Sets global defaults for dynamodol so stores can be created without explicitly passing table details every time.

* **Parameters:**
  **new_defaults** ([`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict)) – 

  A dict containing one or more of the following keys
  table_name: str - The name of the table
  key_fields: Tuple - A tuple of length 1 or 2 containing the partition key and (optional) sort key for the table
  data_fields: Tuple or None - A tuple of data fields to return from queries. If data_fields is None, data
  > will be returned as dicts instead of tuples.
