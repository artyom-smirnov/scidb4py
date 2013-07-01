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

### Fetching array item-by-item (autofetching of next chunks)
```
from scidbpy import Connection
conn = Connection('localhost', 1239)
conn.open()
conn.prepare("select * from array(<a:int32>[x=0:3,2,0], '[0,1,2,3]')")
array = conn.execute()
while not array.end:
    print '%s - %s' % (array.get_coordinates(), array.get_item("a"))
    array.next_item(True)
conn.close()
```

### Fetching array chunk-by-chunk, item-by-item
```
from scidbpy import Connection
conn = Connection('localhost', 1239)
conn.open()
conn.prepare("select * from array(<a:int32 null>[x=0:2,3,0, y=0:2,3,0], '[[1,2,3][4,5,6][7,8,9]]')")
array = conn.execute()
while not array.end:
    while not array.chunk_end:
        print '%s - %s' % (array.get_coordinates(), array.get_item("a"))
        array.next_item()
    array.next_chunk()
conn.close()
```
