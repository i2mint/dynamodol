# dynamodol.partition_query

Stores for filtered data sets

### Classes

| [`DynamoDbPartitionPersister`](#dynamodol.partition_query.DynamoDbPartitionPersister)([db, table_name, ...])   |                                                                                     |
|------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------|
| [`DynamoDbPartitionReader`](#dynamodol.partition_query.DynamoDbPartitionReader)([db, table_name, ...])      | Reads from a partition of a DynamoDB table.                                         |
| [`DynamoDbPrefixReader`](#dynamodol.partition_query.DynamoDbPrefixReader)([db, table_name, ...])         | Reads from a partition of a DynamoDB table, with the sort key filtered by a prefix. |
| [`DynamoDbQueryReader`](#dynamodol.partition_query.DynamoDbQueryReader)([db, table_name, ...])          | Reads from a filtered subset of a DynamoDB table.                                   |

### Exceptions

| [`NoSuchKeyError`](#dynamodol.partition_query.NoSuchKeyError)   |    |
|-------------------------------------------------------------------|----|

### *class* dynamodol.partition_query.DynamoDbPartitionPersister(db=None, table_name=None, key_fields=None, data_fields=None, exclude_keys_on_read=True, query=None, key_query=None, attr_query=None, partition=None)

Bases: [`DynamoDbBasePersister`](dynamodol.base.html.md#dynamodol.base.DynamoDbBasePersister), [`DynamoDbPartitionReader`](#dynamodol.partition_query.DynamoDbPartitionReader)

### *class* dynamodol.partition_query.DynamoDbPartitionReader(db=None, table_name=None, key_fields=None, data_fields=None, exclude_keys_on_read=True, query=None, key_query=None, attr_query=None, partition=None)

Bases: [`DynamoDbQueryReader`](#dynamodol.partition_query.DynamoDbQueryReader)

Reads from a partition of a DynamoDB table.

```pycon
>>> from dynamodol.base import load_sample_data
>>> load_sample_data()
>>> partition_reader = DynamoDbPartitionReader(partition='part1')
>>> list(partition_reader)
[('part1', '01-01'),
 ('part1', '01-02'),
 ('part1', '01-03'),
 ('part1', '01-04'),
 ('part1', '02-01'),
 ('part1', '03-02'),
 ('part1', '04-03'),
 ('part1', 'sort2')]
>>> partition_reader[('part1', '01-01')]
```

#### format_get_key(item)

### *class* dynamodol.partition_query.DynamoDbPrefixReader(db=None, table_name=None, key_fields=None, data_fields=None, exclude_keys_on_read=True, query=None, key_query=None, attr_query=None, partition=None, prefix='')

Bases: [`DynamoDbPartitionReader`](#dynamodol.partition_query.DynamoDbPartitionReader)

Reads from a partition of a DynamoDB table, with the sort key filtered by a prefix.

```pycon
>>> from dynamodol.base import load_sample_data
>>> load_sample_data()
>>> prefix_reader = DynamoDbPrefixReader(key_fields=('partitionkey', 'sortkey'), table_name='sorted_table', partition='part1', prefix='01')
>>> list(prefix_reader)
[('part1', '01-01'),
 ('part1', '01-02'),
 ('part1', '01-03'),
 ('part1', '01-04')]
```

#### format_get_key(item)

### *class* dynamodol.partition_query.DynamoDbQueryReader(db=None, table_name=None, key_fields=None, data_fields=None, exclude_keys_on_read=True, query=None, key_query=None, attr_query=None)

Bases: [`DynamoDbBaseReader`](dynamodol.base.html.md#dynamodol.base.DynamoDbBaseReader)

Reads from a filtered subset of a DynamoDB table.

Every query must include the partition key (the first key field).

```pycon
>>> from dynamodol.base import load_sample_data
>>> load_sample_data()
```

Querying on the partition key and sort key

```pycon
>>> query_reader = DynamoDbQueryReader(query={'partitionkey': 'part1', 'sortkey': '04-03'})
>>> list(query_reader)
[('part1', '04-03')]
```

Querying on a partition and arbitrary attribute key.

```pycon
>>> query_reader = DynamoDbQueryReader(query={'partitionkey': 'part1', 'data': 'a'})
>>> list(query_reader)
[('part1', '01-01'), ('part1', '01-04')]
>>> query_reader[('part1', '01-04')]
```

A more complex query. Note that numerical values and numerical comparisons are not yet supported.

```pycon
>>> query = {'partitionkey': 'part2', 'sortkey': {'$gt': '02-01'}, 'moredata': {'$contains': 'r'}}
>>> query_reader = DynamoDbQueryReader(query=query)
>>> list(query_reader)
[('part2', '03-02'), ('part2', '04-03')]
```

### *exception* dynamodol.partition_query.NoSuchKeyError

Bases: [`KeyError`](https://docs.python.org/3/builtins/exceptions.html#KeyError)
