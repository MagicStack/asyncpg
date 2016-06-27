import collections


Type = collections.namedtuple('Type', ['oid', 'name', 'kind', 'schema'])


Attribute = collections.namedtuple('Attribute', ['name', 'type'])
