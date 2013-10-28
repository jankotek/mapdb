Changelog
============

Version 0.9.7 (2013-10-28)
--------------------------

Store format is not backward compatible.
Fixed locking issues on Windows. Concurrent Transactions (TxMaker) reworked and finally fixed.
Added `DBMaker.fullChunkAllocationEnable()` to enable disk space allocation in 1GB  chunks.
In-memory store now can be compacted. Fixed race condition in `BTreeMap.put()`.

Changes:

 * Rework integer/long serialization.
 * Fix #214: Queues now implement `BlockingQueue` interface
 * Refactor `DBMaker` so it uses properties. Easy to load/save config.
 * TxMaker reworked, fixed concurrency issue.
 * StoreDirect & WAL use stricter locking.
 * Fix #218 and #192, locking issues on Windows during compaction solved.
 * Added Tuple comparators.
 * Fixed several issues in Data Pump.
 * Fix #187, Reference to named objects/collections should be serializable
 * BTreeMap: fix #209, put operation was not thread safe.


Version 0.9.6 (2013-09-27)
--------------------------
Concurrent Transactions (TxMaker) almost fixed. Backward incompatible store format change.
Snapshots are no longer enabled by default.


Open issues:

 * Fix #201: failing test suggests that Concurrent Transactions contains race condition.

Changes:

 * Concurrent Transactions were broken and are now completely re-written.
 * Snapshots are no longer enabled by default. `DbMaker.snapshotDisable()` replaced by `DbMaker.snapshotEnable()`
 * StoreDirect now has checksum which refuses to reopen incorrectly closed stores. In result stores created with 0.9.5-
 can not be open.
 * Store now supports recid preallocation, this leads to faster insert.
 * Fixed performance issue with batch imports
 * Fixed performance issues in free space management
 * Volume has lighter exception handling, result is small speed improvement
 * StoreHeap rewritten. Now it has full transactions.
 * Changes in locking to make it more robust and prevent deadlocks
 * Java Assertions used instead of `IllegalArgumentException` and `InternalError`. Please use `-ea` JVM switch when running MapDB
 * SerializerBase: various optimizations so methods fits into JIT limits


Version 0.9.5 (2013-08-26)
--------------------------
Bugfixes from previous release. Fixed data corruption bugs, upgrade strongly recommended.

Changes:

 * Fix #177: broken compression
 * Fix data corruption with disabled transactions
 * CRC32 replaced with faster Adler32, **store which uses checksum is no backward compatible**
 * Fix #167: Add DB.exists() method to check if named record/collection
 * Fix #167:  Add a makeOrGet to DB Collection maker API.
 * StoreWAL:  fix some TOMBSTONE details
 * Bind: Add methods to find subsets on composite sets


Version 0.9.4 (2013-08-09)
--------------------------
**No backward compability** with previous versions. Some parts were completely rewritten for better free space management.
Many small improvements.

Changes:

 * HTreeMap now supports automatic LRU eviction based on size or access time.
 * DB TreeMap, TreeSet and HashMap now uses builder class.
 * Reworked SerializerBase
 * Reworked Serializer implementations
 * Checksum, Compression and Encryption integrated into store, now much faster
 * Add `.sizeLong()` into HTreeMap and BTreeMap.
 * Fixed data corruption in HTreeMap
 * Rewritten space reclaim algorithm
 * Store now has maximal size limit
 * `DBMaker.writeAheadLogDisable()` renamed to `DBMaker.transactionDisable()`
 * TxMaker is now concurrent
 * BTreeMap now supports descending maps



Version 0.9.3 (2013-06-02)
--------------------------

CRITICAL upgrade urgency. This release fixes number of critical bugs in Write Ahead Log.
It also adds support for advanced Java Serialization, which was reported many times as a bug.

Changes:

 * FIX Issue #17 - Serializer fails in some cases (writeExternal and readExternal methods)
 * FIX Issue #136 & #132 -  Data corruption in Write Ahead Log after rollback or reopen.
 * FIX Issue #137 - Deadlock while closing AsyncWriteEngine Credit Jan Sileny
 * FIX Issue #139 - rolled back TX should not throw exception on close.
 * FIX Issue #135 - SerializerPojo registered classes problem. Credit Jan Sileny
 * ADD `DBMaker.syncOnCommitDisable()` parameter
 * ADD all stuff in `DataOutput2` and `DataInput2` is public. It also extends In/OutputStream now.



Version 0.9.2 (2013-05-19)
--------------------------

CRITICAL upgrade urgency. This release fixes some critical bugs. It also improves performance and introduces Data Pump.

Open Issues:

 * Issue #17 - Serializer fails in some cases (writeExternal and readExternal methods)

Changes:

 * FIX Issue #119 - BTreeMap did not released locks with multiple transactions
 * FIX Issue #125 - calling close twice failed.
 * FIX race condition in HTreeMap
 * ADD `ByteBuffer` now uses `duplicate()` instead of synchronization. Better concurrency
 * ADD Issue #123 - Replace RandomAccessFile by FileChannel and improve performance on 32bit systems.
 * ADD Delta Packing for tuples
 * ADD better serialization for small strings
 * ADD improve Javadoc, use Pegdown Doclet so Javadoc can be written in markdown
 * ADD reuse DataOutput instances, performance improvement
 * ADD datapump to create BTreeMap from large unsorted data set in linear time. Checkout `Huge_Insert` example
 * ADD improve AsyncWriteEngine performance by removing Write Queue



Version 0.9.1 (2013-04-14)
--------------------------

CRITICAL upgrade urgency. This release fixes number of critical bugs from first release, including data store corruption and crashes.

Open issues:

 * Issue #119 -  BTreeMap (TreeMap) may not release all locks and consequently crash.
   This is unconfirmed and hard to replicate concurrent bug.
   I temporarily added assertion which slows down BTreeMap updates, but helps to diagnose this problem
 * Issue #118 - StoreWAL fails to create log for unknown reasons and crashes. Not reproduced yet, need to investigate.

Changes:

 * FIX #111 - Compaction fails with large data sets
 * FIX - BTreeKeySerializer.ZERO_OR_POSITIVE_INT was broken
 * FIX #89 - StoreAppend reopen failed
 * FIX #112 - Compaction fails with WAL enabled
 * FIX #114 - RandomAccessFile fails with WAL
 * FIX #113 - MemoryMappedFile was not unlocked on Windows after DB close
 * FIX - rewrite AsynwWriteEngine, fix many concurrent bugs
 * FIX - Files were not synced on DB.close(). Possible data loss.
 * FIX - free space reuse did not worked in StoreDirect and StoreWAL. Storage file grown infinitely with each update.
 * FIX #116 - HTreeMap.isEmpty returned wrong result.
 * FIX #121 - WAL could get corrupted in some cases.
 * ADD - basic benchmark
 * ADD - error message if file rename fails after compaction finishes
 * ADD - #119 BTreeMap locking could not be fixed, I added assertion to help diagnose issue. Small performance drop on BTreeMap updates.
 * ADD - performance improvement if Snapshot engine is not used.


Version 0.9.0 (2013-04-01)
-------------------------

First release with stable API and storage format.



Upgrade urgency levels:
----------------------

* LOW:      No need to upgrade unless there are new features you want to use.
* MODERATE: Program an upgrade of the DB engine, but it's not urgent.
* HIGH:     There is a critical bug that may affect a subset of users. Upgrade!
* CRITICAL: There is a critical bug affecting MOST USERS. Upgrade ASAP.

