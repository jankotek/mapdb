HTreeMap
=========
HTreeMap (aka HashMap) is one of `Map`s offered by MapDB. It has great performance with large keys. It also offers entry expiration if time-to-live or maximal size is exceeded.

HTreeMap is *segmented Hash Tree*. Most hash collections use an array for hash table, it requires copying all data when hash table is resized. HTreeMap uses auto-expanding 4 level tree, so it never needs resizing. 

To support concurrency HTreeMap is split to 16 independent segments, each with separate read-write lock. `ConcurrentHashMap` works similar way. Number of segments (sometimes called concurrency factor) is hard wired in design and can not be changed.  

HTreeMap optionally supports entry expiration based on three criteria: maximal size, time-to-live since last modification and time-to-live since last access. Expired entries are automatically removed. This feature uses FIFO queue, each segment has independent expiration queue. Priority per entry can not be set.

Parameters
----------------
HTreeMap has number of parameters to tune its performance. Number of segments (aka concurrency factor) is hard-coded in design 16 and can not be changed. Other params can be set  only once when map is created.

TODO enum HTreeMap params
 * key serializer
 * value serializer
 * keep counter
 * TODO value outside nodes
 * value creator (get returns it instead of null when not found)
 * expire max size
 * expire ttl after update
 * expire ttl after read

Performance
--------------------
HTreeMap has one major advantage for using with large keys. Unlike BTreeMap it only stores hash codes in tree nodes. Each lookup on BTreeMap deserializes number of tree nodes together with their keys.

TODO link to performance test, compare with BTreeMap

On other side HTreeMap has limited concurrency factor to 16, so its writes wont scale over 4 CPU cores. It uses read-write locks, so read operations are not affected. However in practice disk IO is more likely to be bottleneck.
