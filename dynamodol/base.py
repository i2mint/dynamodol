"""
dynamodb (through boto3) with a simple (dict-like or list-like) interface
"""
import traceback

import boto3
import botocore.exceptions
from dataclasses import dataclass, field
from functools import wraps
from lazyprop import lazyprop
from typing import Any, Tuple

from dol.base import KvReader, KvPersister


class NoSuchKeyError(KeyError):
    pass


def get_db(aws_access_key_id='',
    aws_secret_access_key='',
    aws_session_token='',
    region_name='',
    endpoint_url='http://localhost:8000'):
    resource_kwargs = {'region_name': region_name} if region_name else {'endpoint_url': endpoint_url}
    if aws_access_key_id:
        resource_kwargs['aws_access_key_id'] = aws_access_key_id
        resource_kwargs['aws_secret_access_key'] = aws_secret_access_key
        if aws_session_token:
            resource_kwargs['aws_session_token'] = aws_session_token
    return boto3.resource('dynamodb', **resource_kwargs)


@dataclass
class DynamoDbBaseReader(KvReader):
    db: Any = field(default=None)
    table_name: str = field(default='dynamodol')
    key_fields: Tuple[str] = field(default=('key', None))
    data_fields: Tuple[str] = field(default=('data',))
    exclude_keys_on_read: bool = field(default=True)
    projection: str = field(default=None)

    def __post_init__(self):
        if not self.db:
            self.db = get_db()
        if isinstance(self.data_fields, str):
            self.data_fields = (self.data_fields,)
        if isinstance(self.key_fields, str):
            self.key_fields = (self.key_fields,)
        if isinstance(self.projection, list):
            self.projection = ','.join(self.projection)

    @lazyprop
    def table(self):
        key_schema = [{'AttributeName': self.partition_key, 'KeyType': 'HASH'}]
        if self.sort_key:
            key_schema.append({'AttributeName': self.sort_key, 'KeyType': 'RANGE'})
        attribute_definition = [
            {'AttributeName': k, 'AttributeType': 'S'}
            for k in self.key_fields if k
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

    def format_get_item(self, item):
        if self.exclude_keys_on_read:
            return {x: item[x] for x in item if x not in self.key_fields}
        return item

    def __getitem__(self, k):
        try:
            if isinstance(k, str):
                if self.sort_key:
                    raise ValueError('If a sort key is defined, object keys must be tuples.')
                k = (k,)
            k = {att: key for att, key in zip(self.key_fields, k)}
            get_kwargs = {'Key': k}
            if self.projection:
                get_kwargs['ProjectionExpression'] = self.projection
            response = self.table.get_item(**get_kwargs)
            item = response['Item']
            return self.format_get_item(item)
        except Exception as e:
            raise NoSuchKeyError(f'Key not found: {k}')

    def __iter__(self):
        # This is extremely inefficient and should not be used with large tables in production
        scan_kwargs = {}
        if self.projection:
            scan_kwargs = {'ProjectionExpression': self.projection}
        response = self.table.scan(**scan_kwargs)
        yield from (self.format_get_response(d) for d in response['Items'])

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
    A basic DynamoDb via Boto3 persister.
    >>> s = DynamoDbPersister()
    >>> k = {'key': '777'} # Each collection will happily accept user-defined _key values.
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
    >>> s.get({'not': 'a key'}, {'default': 'val'})  # testing s.get with default
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
    >>> s[{'name': 'guido'}] = {'yob': 1956, 'proj': 'python', 'bdfl': False}
    >>> s[{'name': 'guido'}]
    {'proj': 'python', 'yob': Decimal('1956'), 'bdfl': False}
    >>> s[{'name': 'vitalik'}] = {'yob': 1994, 'proj': 'ethereum', 'bdfl': True}
    >>> s[{'name': 'vitalik'}]
    {'proj': 'ethereum', 'yob': Decimal('1994'), 'bdfl': True}
    >>> for key, val in s.items():
    ...   print(f"{key}: {val}")
    {'name': 'vitalik'}: {'proj': 'ethereum', 'yob': Decimal('1994'), 'bdfl': True}
    {'name': 'guido'}: {'proj': 'python', 'yob': Decimal('1956'), 'bdfl': False}
    """

    def __setitem__(self, k, v):
        if isinstance(k, str):
            if self.sort_key:
                raise ValueError('If a sort key is defined, object keys must be tuples.')
            else:
                k = (k,)
        key = {att: key for att, key in zip(self.key_fields, k)}
        if isinstance(v, str):
            v = (v,)
        val = {att: key for att, key in zip(self.data_fields, v)}

        self.table.put_item(Item={**key, **val})

    def __delitem__(self, k):
        try:
            if isinstance(k, str):
                if self.sort_key:
                    raise ValueError('If a sort key is defined, object keys must be tuples.')
                k = (k,)
            key = {att: key for att, key in zip(self.key_fields, k)}
            self.table.delete_item(Key=key)
        except Exception as e:
            if hasattr(e, '__name__'):
                if e.__name__ == 'NoSuchKey':
                    raise NoSuchKeyError(f'Key not found: {k}')
            raise
