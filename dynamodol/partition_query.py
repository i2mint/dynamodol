"""
enable queries by partition and sort keys
"""
from boto3.dynamodb.conditions import Key, Attr
from dataclasses import dataclass, field
from typing import Any

from dynamodol.base import DynamoDbBaseReader, DynamoDbBasePersister


class NoSuchKeyError(KeyError):
    pass

valid_key_operators = ['begins_with', 'between', 'gt', 'gte', 'lt', 'lte']
valid_attr_operators = [*valid_key_operators, 'contains', 'ne', 'exists', 'not_exists', 'is_in', 'size']
valid_size_operators = ['gt', 'gte', 'lt', 'lte', 'between', 'is_in']


def _apply_filter_method(filter_obj, operator, value):
    filter_method = getattr(filter_obj, operator[1:])
    if operator == '$between':
        if len(value) != 2:
            raise ValueError(f'Values for the $between operator must be iterables of length 2 (received {value}).')
        return filter_method(*value)
    return filter_method(value)

def _mk_query_from_dict_val(attr_or_key, query_val, is_key=False):
    """Transforms a MongoDB-like query dict into a DynamoDB Key or Attr condition object.
    Cannot currently handle any $and or $or conditions.

    :param attr_or_key: The name of the attribute or key
    :param query_val: The value to filter by; either a primitive to filter for equality, or a dict containing a logical operator
    :param is_key: Whether to create a Key condition

    TODO: handle complex logical expressions
    """
    filter_obj = Key(attr_or_key) if is_key else Attr(attr_or_key)
    if isinstance(query_val, (str, int, float, bool)):
        return filter_obj.eq(query_val)

    operator, value = list(query_val.items())[0]

    # for compatibility with MongoDB syntax
    if operator == '$in':
        operator = '$is_in'
    valid_operator_list = valid_key_operators if is_key else valid_attr_operators
    if operator[1:] not in valid_operator_list:
        raise ValueError(f'Operator {operator} is not valid for {"key" if is_key else "attribute"} queries.')
    if operator == '$exists':
        if value is False:
            return filter_obj.not_exists()
        else:
            return filter_obj.exists()
    if operator == '$size':
        filter_obj = filter_obj.size()
        if isinstance(value, int):
            return filter_obj.eq(value)
        else:
            size_operator, size_value = list(value.items())[0]
            if size_operator[1:] not in valid_size_operators:
                raise ValueError(f'Operator {size_operator} is not valid for size queries.')
            return _apply_filter_method(filter_obj, size_operator, size_value)
    return _apply_filter_method(filter_obj, operator, value)


# TODO unused; delete it, or use it?
# def _mk_attr_query_from_split_string(raw_query, attr_def):
#     """Make an attribute query from a raw query in the format [operator, *args]
#     """
#     operator = raw_query[0]
#     if operator not in valid_attr_operators:
#         raise ValueError(f'{operator} is not a valid attribute query operator.')
#     attr_method = getattr(attr_def, operator)
#     if operator == 'size':
#         return _mk_attr_query_from_split_string(raw_query[1:], attr_method())
#     if operator == 'exists' and raw_query[1] is False:
#         operator = 'not_exists'
#     if operator in ['exists', 'not_exists']:
#         return attr_method()
#     if len(raw_query) > 3 or len(raw_query) < 2 or \
#             (len(raw_query) == 3 and raw_query[0] != 'between') or \
#             (len(raw_query) == 2 and raw_query[0] == 'between'):
#         raise ValueError(
#             'attr_query must include an operator and one value, or the "between" operator and two values, separated by spaces')
#     return attr_method(*raw_query[1:])

@dataclass
class DynamoDbQueryReader(DynamoDbBaseReader):
    query: Any = field(default=None)
    key_query: Any = field(default=None)
    attr_query: Any = field(default=None)

    def __post_init__(self):
        DynamoDbBaseReader.__post_init__(self)
        if isinstance(self.query, dict):
            if not self.key_query:
                if self.partition_key in self.query:
                    self.key_query = Key(self.partition_key).eq(self.query[self.partition_key])
                    if self.sort_key in self.query:
                        self.key_query = self.key_query & _mk_query_from_dict_val(self.sort_key, self.query[self.sort_key], is_key=True)
                else:
                    self.key_query = None
            if not self.attr_query:
                for attr, val in self.query.items():
                    if attr == self.partition_key or (self.key_query and attr == self.sort_key):
                        continue
                    if attr not in self.key_fields:
                        new_query = _mk_query_from_dict_val(attr, val)
                        if self.attr_query:
                            self.attr_query = self.attr_query & new_query
                        else:
                            self.attr_query = new_query

    @property
    def filter_kwargs(self):
        result = dict(KeyConditionExpression=self.key_query)
        if self.attr_query:
            result['AttrConditionExpression'] = self.attr_query
        return result

    def __iter__(self):
        response = self.table.query(**self.filter_kwargs)
        yield from (
            {x: d[x] for x in d if x in self.key_fields} if self.exclude_keys_on_read else d
            for d in response['Items']
        )

    def __len__(self):
        response = self.table.query(**self.filter_kwargs, Select='COUNT')
        return response['Count']


@dataclass
class DynamoDbPartitionReader(DynamoDbQueryReader):
    partition: str = field(default="partition")

    def __post_init__(self):
        if len(self.key_fields) != 2:
            raise ValueError('DynamoDbPartitionReader must have a composite key of length 2 in the format (partition_key, sort_key)')
        self.key_query = Key(self.partition_key).eq(self.partition)
        DynamoDbQueryReader.__post_init__(self)

    def __getitem__(self, k):
        try:
            key = {self.partition_key: self.partition, self.sort_key: k}
            response = self.table.get_item(Key=key)
            item = response['Item']
            return self.format_get_item(item)
        except Exception as e:
            raise NoSuchKeyError(f'Key not found: {k}')


@dataclass
class DynamoDbPrefixReader(DynamoDbPartitionReader):
    prefix: str = field(default='')

    def __post_init__(self):
        DynamoDbPartitionReader.__post_init__(self)
        self.key_query = Key(self.partition_key).eq(self.partition) & Key(self.sort_key).begins_with(self.prefix)

    def __getitem__(self, k):
        try:
            key = {self.partition_key: self.partition, self.sort_key: self.prefix + k}
            print(f'key: {key}')
            response = self.table.get_item(Key=key)
            item = response['Item']
            return self.format_get_item(item)
        except Exception as e:
            raise NoSuchKeyError(f'Key not found: {k}')


class DynamoDbPartitionPersister(DynamoDbBasePersister, DynamoDbPartitionReader):
    def __setitem__(self, k, v):
        key = {self.partition_key: self.partition, self.sort_key: k}
        if isinstance(v, str):
            v = (v,)
            val = {att: key for att, key in zip(self.data_fields, v)}
        else:
            val = v

        self.table.put_item(Item={**key, **val})

    def __delitem__(self, k):
        key = {self.partition_key: self.partition, self.sort_key: k}
        try:
            self.table.delete_item(Key=key)
        except Exception as e:
            if getattr(e, '__name__') == 'NoSuchKey':
                raise NoSuchKeyError(f'Key not found: {k}')
            raise
