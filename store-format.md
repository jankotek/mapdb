Store Format
=========

JDBM4 storage has two different areas: the index and data files. The index file is a list of 8-byte-long addresses pointing into the data file.
The data file contains raw record data.

Store internal information, such as the list of free records, is maintained using 'long stacks'.
It is a reverse linked list of 8-byte-longs storing addresses. It is stored in a data file, as any other records.

JDBM4 store is very minimalistic with little redundancy. Its design is focused on maximum speed and minimum overhead.
A lot of operations (transaction, locking) are done in memory.


Index File
-----------
The index file is a list of 8-byte-long addresses pointing to the data file.  Each address is identified by its position in the index file.
When `RecordManager` returns `recid`  it actually returns a location in the index file.

Each address is 8-byte-long, so the index file offset is calculated by multiplying recid with eight:

    index-file-offset = recid * 8

Some starting index positions are reserved for internal use. Those still contain 8-byte-long addresses, but point to
internal data, such as the list of free records.   `Recid` returned by `RecordManager` will be always greater than 2555.

    0  - file header and store format version 'JDBMXXXX'..
    1 - current size of data file (pointer to end of data file)
    2  - data address of 'long stack' containing free positions in the index file
    3 to 18 - reserved addresses for JDBM internal objects, such as Serializer or Name Table.
    19 - unreserved address (feel free to use it for your own stuff)
     20 to 2555 - data addresses of free data records (see next chapters)
     2556 to infinity - contains recids of user records.

8-byte-long addresses stored in the index file have two parts: The first 2 bytes is the data record size and the last 6 bytes are the offset in the data file.
JDBM store currently supports a record of maximum size 64KB (2^16), some workaround for bigger records will come in the future.
And the entire store has maximum size 256 TeraBytes (2^48).

Some addresses in the index file may have 0 value. This indicates that the record with the given recid has been deleted, or not yet claimed.

Data File
----------
The data file has no structure, it just contains records followed by other records. There are no metadata, checksums or separators.
All structural information is stored in the index file.

 The first 8 bytes of the data file are reserved for the file header and store format version in format 'JDBMXXXX'.
 This is to prevent zero from being a valid address.

Long Stack
-------------
Long Stack is reverse linked list of 8-byte-longs implemented on top of the record store. JDBM uses it to store its internal information such as a list of free records.  It supports only two operations (`pop` and `take`), which add and remove a number from the head of the list.
Numbers are inserted and removed in LIFO (Last In, First Out) fashion.

It  is very low-level and tightly integrated into RecordManager, to minimise the number of IO operations. Each operation typically requires only
9 bytes to be read and written. It also takes nearly constant time and is not affected by store fragmentation

Each Long Stack has:

**Long Stack Recid** in Index File which contains address to Head Record. Each Long Stack is identified by this recid.

**Head  Record** containing most recently inserted  numbers. It is located in the Data File. It is referenced from the Index File.

**Previous Records** contains previously inserted numbers. It is located in the Data File and is basically a Linked List of records starting at Head Record.

Numbers are grouped into records, each containing 100 numbers. Records are chained, as the reverse-linked list starting at Head Record.

When a number is inserted into the record and it overflows 100, a new Head Record is created. This new Head Record contains the address to the Previous Record.
 Also the address in the Index File is updated to point to the new Head Record.

 Each Long Stack Record has the following structure:

    byte 0 - how many numbers are inserted in this record
    byte 1 - unused
    byte   2 to  7 - offset of Previous Record in the Data File
    byte  7 to  15 - first number on this record
    byte 16 to 23 - second number on this record....


Free positions in Index File
--------------------------------
When a record is deleted, the `RecordManager` sets the given position in the Data File to zero. Recently released recid are stored in  Long Stack with recid 2 for reuse.

List of free data records
-----------------------------
The List of free data records is stored in multiple Long Stacks determined by the record size. When a data record with size N is released, it is added to Long Stack N.
When a data record is allocated, it has to look at Long Stack N, if it contains any free record to use.
It does not have to make a linear scan across the list of all free data records.

TODO describe this more.

Defrag
--------
Defragmentation recreates store in new file. It traverse Index File, reads addresses and insert data records into new RecordManager.
New file is then moved over the old file.







