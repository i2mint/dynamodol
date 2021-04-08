
# dynamodol
dynamodb (through boto3) with a simple (dict-like or list-like) interface

To install:	```pip install dynamodol```

A basic DynamoDb via Boto3 persister demo:

```python
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
```
