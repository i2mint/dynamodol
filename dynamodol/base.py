"""DynamoDB (through boto3) with a simple (dict-like or list-like) interface
"""
import boto3
import botocore.exceptions
from dataclasses import dataclass, field
from decimal import Decimal
from functools import wraps
from lazyprop import lazyprop
from typing import Any, Iterable, Mapping, Tuple

from dol import KvReader, KvPersister, Store, BaseValuesView, BaseItemsView


class NoSuchKeyError(KeyError):
    pass


DFLT_TABLE_NAME = 'dynamodol'
DFLT_KEY_FIELDS = ('key',)
DFLT_DATA_FIELDS = ('value',)

db_defaults = {
    'table_name': DFLT_TABLE_NAME,
    'key_fields': DFLT_KEY_FIELDS,
    'data_fields': DFLT_DATA_FIELDS,
    'projection': None,
}


def get_db(
    aws_access_key_id='',
    aws_secret_access_key='',
    aws_session_token='',
    region_name='',
    endpoint_url='http://localhost:8000',
):
    resource_kwargs = (
        {'region_name': region_name} if region_name else {'endpoint_url': endpoint_url}
    )
    if aws_access_key_id:
        resource_kwargs['aws_access_key_id'] = aws_access_key_id
        resource_kwargs['aws_secret_access_key'] = aws_secret_access_key
        if aws_session_token:
            resource_kwargs['aws_session_token'] = aws_session_token
    return boto3.resource('dynamodb', **resource_kwargs)


def decimal_to_float(x):
    print(f'x: {x}')
    if isinstance(x, str):
        return x
    if isinstance(x, Mapping):
        return {k: decimal_to_float(v) for k, v in x.items()}
    if isinstance(x, Iterable):
        return [decimal_to_float(v) for v in x]
    if isinstance(x, Decimal):
        return float(x)
    return x


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
    key_fields: Tuple[str] = field(default=None)
    data_fields: Tuple[str] = field(default=None)
    exclude_keys_on_read: bool = field(default=True)

    class ValuesView(BaseValuesView):
        def __contains__(self, v):
            return self._mapping.contains_value(v)

        def __iter__(self):
            return self._mapping.iter_values()

    class ItemsView(BaseItemsView):
        def __contains__(self, item):
            return self._mapping.contains_item(item)

        def __iter__(self):
            return self._mapping.iter_items()

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
            self.projection = ','.join(self.projection)

    def __reversed__(self):
        return list(self)[::-1]

    @lazyprop
    def table(self):
        key_schema = [{'AttributeName': self.partition_key, 'KeyType': 'HASH'}]
        if self.sort_key:
            key_schema.append({'AttributeName': self.sort_key, 'KeyType': 'RANGE'})
        attribute_definition = [
            {'AttributeName': k, 'AttributeType': 'S'} for k in self.key_fields if k
        ]

        try:
            table = self.db.create_table(
                TableName=self.table_name,
                KeySchema=key_schema,
                AttributeDefinitions=attribute_definition,
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5,
                },
            )
            # Wait until the table creation is complete.
            self.db.meta.client.get_waiter('table_exists').wait(
                TableName=self.table_name
            )
            print(f'Table {self.table_name} has been created.')
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
        print(f'obj: {obj}')
        return decimal_to_float(obj)

    def format_get_key(self, item):
        """TODO: replace with _id_of_key, etc."""
        if len(self.key_fields) == 2:
            return item[self.key_fields[0]], item[self.key_fields[1]]
        return item[self.key_fields[0]]

    @property
    def _keys_expression(self):
        return {
            'ExpressionAttributeNames': {
                f'#{index}': key for index, key in enumerate(self.key_fields)
            },
            'ProjectionExpression': ', '.join(
                [f'#{i}' for i in range(len(self.key_fields))]
            ),
        }

    @property
    def _values_expression(self):
        if not self.data_fields:
            return {}
        return {
            'ExpressionAttributeNames': {
                f'#{index}': key for index, key in enumerate(self.data_fields)
            },
            'ProjectionExpression': ', '.join(
                [f'#{i}' for i in range(len(self.data_fields))]
            ),
        }

    @property
    def _keys_values_expression(self):
        if not self.data_fields:
            return {}
        all_fields = [*self.key_fields, *self.data_fields]
        return {
            'ExpressionAttributeNames': {
                f'#{index}': key for index, key in enumerate(all_fields)
            },
            'ProjectionExpression': ', '.join(
                [f'#{i}' for i in range(len(all_fields))]
            ),
        }

    def __getitem__(self, k):
        try:
            _k = k
            if isinstance(k, str):
                if self.sort_key:
                    raise ValueError(
                        'If a sort key is defined, object keys must be tuples.'
                    )
                _k = (k,)
            _k = {att: key for att, key in zip(self.key_fields, _k)}
            response = self.table.get_item(Key=_k, **self._values_expression)
            item = response['Item']
            return self.format_get_item(item)
        except Exception as e:
            raise NoSuchKeyError(f'Key not found: {k}')

    def iter_items(self):
        response = self.table.scan(**self._keys_values_expression)
        yield from (
            (self.format_get_key(d), self.format_get_item(d)) for d in response['Items']
        )

    def iter_values(self):
        response = self.table.scan(**self._values_expression)
        yield from (self.format_get_item(d) for d in response['Items'])

    def __iter__(self):
        # This is extremely inefficient and should not be used with large tables in production
        response = self.table.scan(**self._keys_expression)
        yield from (self.format_get_key(d) for d in response['Items'])

    def __len__(self):
        # This is extremely inefficient and should not be used with large tables in production
        response = self.table.scan(Select='COUNT')
        return response['Count']

    @wraps(get_db)
    @staticmethod
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
                    'If a sort key is defined, object keys must be tuples.'
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
                        'If a sort key is defined, object keys must be tuples.'
                    )
                k = (k,)
            key = {att: key for att, key in zip(self.key_fields, k)}
            self.table.delete_item(Key=key)
        except Exception as e:
            if hasattr(e, '__name__'):
                if e.__name__ == 'NoSuchKey':
                    raise NoSuchKeyError(f'Key not found: {k}')
            raise


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
            'table_name': 'sorted_table',
            'key_fields': ('partitionkey', 'sortkey'),
            'data_fields': ('data', 'moredata'),
            'partition': 'part1',
        }
    )
    sorted_persister = DynamoDbBasePersister()
    for k in list(sorted_persister):
        del sorted_persister[k]
    sorted_persister[('part1', 'sort2')] = ('val2', 'moreval2', None)
    sorted_persister[('part1', '01-01')] = ('a', 'bcde')
    sorted_persister[('part1', '01-02')] = ('c', 'defg')
    sorted_persister[('part1', '01-03')] = ('e', 'fghi')
    sorted_persister[('part1', '01-04')] = ('a', 'cdef')
    sorted_persister[('part1', '02-01')] = ('g', 'hijk')
    sorted_persister[('part1', '03-02')] = ('i', 'jklm')
    sorted_persister[('part1', '04-03')] = ('k', 'lmno')
    sorted_persister[('part2', '02-01')] = ('m', 'nopq')
    sorted_persister[('part2', '03-02')] = ('o', 'pqrs')
    sorted_persister[('part2', '04-03')] = ('q', 'rstu')
    sorted_persister[('part3', '01-05')] = ('s', 'tuvw')
    sorted_persister[('part3', '02-02')] = ('u', 'vwxy')
    sorted_persister[('part3', '03-03')] = ('w', 'xyza')


# TODO class DynamoDbStore(DynamoDbBasePersister, Store): ...
