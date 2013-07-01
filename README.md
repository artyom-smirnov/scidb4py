# scidbpy

Pure python SciDB client library implementation

## Runtime dependencies
* python >= 2.7?
* python-protobuf >= 2.4
* bitstring

# Build dependencies
* protobuf-compiler >= 2.4

## Installation
```sudo python setup.py install```

## Examples
```
from scidbpy import Connection
conn = Connection('localhost', 1239)
conn.open()
conn.prepare("select * from array(<a:int32>[x=0:3,2,0], '[0,1,2,3]')")
array = conn.execute()
array.fetch()
chunk = array.get_chunk(0)
conn.close()
```