

Store direct
------------

### Index value

from end:
- last bit is for single bit parity
- 3 bits record type

record types:
- 0 - small record
- 1 - large record with root
- 2 - record stored in index value, next for bits (from end) indicate record size. 8 is null
- XX - external file, linked record, etc ????


Small record
- 48 bits (minus 4) is offset
- 16 bits

large record with root
- 48 bits (minus 4) is offset
- 16 bits

format of large record root
- 2 bytes number of links
- followed by cycle of
    - 2 bytes record size
    - 6 bytes offset (aligned to 16)
    - previous 8 bytes have 1 bit parity

