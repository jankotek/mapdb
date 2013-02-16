


Index File
----------

MapDB stores data in Data File. It maintains Index File to find record offset and size in Data File.
Index File is series of eight-byte longs, one per record. Each record is primary identified by *recid* (Record Identifier)
which is position in Index File.

Fetching record typically involves these steps:

1) calculate Index File offset from recid
2) fetch data file location from Index File
3) fetch binary data from Data File
4) deserialize record and return it

TODO how record is inserted
TODO free space management


=== Reserved Recids ===

Some record IDs are reserved for MapDB internal record. Newly created storage comes with those record preinitialized.
Here is list of reserved recids:

    0 - zero indicates 'null' value, is not used for any record
    1 - named directory, an HTreeMap<String,Long> where name is a key, value is a RECIDs of named record.
    2 - POJO serializer class structure information (class names, row names...)
    3 - not used yet
    4 - not used yet
    5 - not used yet
    6 - not used yet
    7 - not used yet

Recids accessible to user (returned by `Engine.put()`) start at `8`. You should not manipulate reserved recids unless
you know exactly what you are doing; For example after a class was renamed, you should update class name in class structure
info at recid `2`.

