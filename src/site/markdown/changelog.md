Changelog
============

Version 1.0.4 (2014-06-26)
--------------------------
Fixed transaction file locking on Windows.   

Changes:

 * Fix #326, #327, #346 and  #323: Transaction log was not unlocked on Windows, causing various issues. Kudos to Rémi Alvergnat for discovering and fixing it.  
 * Fix #335: Ensures that file resources are always released on close. Kudos to Luke Butters. 

Version 1.0.3 (2014-06-08)
--------------------------
Fixed new space allocation problem, file now increases in 1MB increments. Updated copyright info and added notice.txt  

Changes:

 * Fix #338 Excess storage size on Memory mapped files
 * Add notice.txt with list of copyright holders
 * Updated javadocs

Open problems:

 * Open #304 and #283: BTreeMap fails under concurrent access.  Unconfirmed and can not reproduce. It needs more investigation.
 * Documentation

Version 1.0.2 (2014-06-02)
--------------------------
Fixed `Serializer.CompressionWrapper()`, this bug does not affect `DBMaker.compressionEnable()`

Changes:

 * Fix #321: Small behaviour regression in BTreeMap Pump
 * Fix #332: `Serializer.CompressionWrapper()` decompressed wrong data. Reverted some optimization which caused this issue.

Open problems:

 * Open #304 and #283: BTreeMap fails under concurrent access.  Unconfirmed and can not reproduce. It needs more investigation.
 * Documentation


Version 1.0.1 (2014-05-05)
--------------------------
Fixed MRU cache and BTree Pump Presort.

Changes:

 * Fix #320: BTreeMap pump presort fails
 * Fix #319: ClassCastException in the Cache.LRU

Open problems:

 * Open #304 and #283: BTreeMap fails under concurrent access.  Unconfirmed and can not reproduce. It needs more investigation.
 * Documentation


Version 1.0.0 (2014-04-27)
--------------------------
Fixed a few minor problems. Lot of code cleanups.

This is first stable release with long term support. Thanks to everyone who helped to get MapDB this far.

Changes:

 * Fix #315: DB.delete(name) deletes substring matches
 * SerializerPojo: add interceptors to alter serialized objects

Open problems:

 * Open #304 and #283: BTreeMap fails under concurrent access.  Unconfirmed and can not reproduce. It needs more investigation.
 * Documentation


Version 0.9.13 (2014-04-16)
--------------------------
There was another problem with mmap files larger than 2GB.

This is yet another release candidate for 1.0.0. Stable release should follow in 9 days if no problems are found.

Changes:

 * Fix #313: mmap files larger than 2GB could not be created

Open problems:

 * Open #304 and #283: BTreeMap fails under concurrent access: . Unconfirmed and needs more investigation.
 * Documentation


Version 0.9.12 (2014-04-15)
--------------------------
Previous release was broken, store larger than 16 MB or 2 GB could not be created, that is fixed now.
This release also brings number of small cleanups and improved memory consumption.

The store format has changed yet again in backward incompatible way. The chunk (slice) size is now 1 MB.

This is yet another release candidate for 1.0.0. Stable release should follow in 10 days if no problems are found.

Changes:

 * Format change! Chunk (slice) size reduced from 16MB to 1MB, solved many Out Of Memory errors.
 * Fix #313: mmap files larger than 2GB could not be created
 * Fix #308: ArrayIndexOutOfBoundsException if store is larger 16MB.
 * Fix #312: error while opening db with readonly
 * Fix #304: BTreeMap.replace() fails under concurrent access
 * Large scale code cleanup before 1.0.0 freeze and release
 * DBMaker: rename `syncOnCommitDisable()` to `commitFileSyncDisable()`
 * DBMaker: add `newHeapDB()` option, this store does not use serialization and is almost as fast as java collections

Open problems:

 * Open #304 and #283: BTreeMap fails under concurrent access: . Unconfirmed and needs more investigation.
 * Documentation


Version 0.9.11 (2014-03-24)
--------------------------

This fixes serious race condition for in-memory store. Also there is fix for secondary collections containing
wrong values. And finally all file locking problems on Windows should be solved.

As result the store format was completely changed. There is no backward compatibility with previous releases.
MapDB now allocates memory in 16MB chunks (slices), so new empty database will always consume a few MB of memory/disk space.

This is last 0.9.x release, next release will be 1.0.0.

Changes:

 * Fix #303 and #302: There was race condition in Volumes, which caused data corruption under concurrent access.
 * Fix #252 and #274: File locking on Windows is now completely solved. We no longer use overlapping ByteBuffers
 which were source of errors.
 * Fix #297: BTreeMap modification listeners received wrong key. As result secondary collections could contain wrong data.
 * Fix #300: `Queue.offer()` should return false, not throw an  `IllegalStateException()`. Not really isssue since MapDB does not have queues with limited size yet.
 * Engine: add close listener, to prevent NPE on shutdown in HTreeMap Cache
 * Maven: do not run tests in parallel, it causes out of memory errors
 * StoreWAL: do not delete log file after every commit, keep it around. This should speedup commits a lot
 * Volume: mmap file chunks (slices) were synced multiple times, causing slow sync and commits
 * Volume: change 'chunk size' (slice size) from 1GB to 16MB and disable incremental allocation.
 * DBMaker: The 'full chunk allocation' option was removed and is now on by default.
 * DBMaker: method `newDirectMemoryDB()` replaced with `newMemoryDirectDB()`
 * Fun: Added Tuple5 and Tuple6 support


Open problems:

 * Open #304 and #283: BTreeMap fails under concurrent access: . Unconfirmed and needs more investigation.
 * Documentation



Version 0.9.10 (2014-02-18)
--------------------------

Yet another bug fix release before 1.0. There is fix for serious data corruption with disabled transactions.
Async-Writer queue is no longer unbounded to prevent memory leaks. In-memory cache is now much easier to use
with memory size limit, checkout `Map cache = DBMaker.newCache(sizeLimitInGB)`

Changes:

 * Fix #261: SerializerPojo could cause data corruption with transaction disabled.
 * Fix #281: txMaker.makeTx().snapshot() does not work.
 * Fix #280: Check for parent folder when opening file db.
 * Fix #288: syncOnCommitDisable() does not work at WAL
 * Fix #276: In-memory cache based on HTreeMap now has memory size limit. Checkout `Map cache = DBMaker.newCache(sizeLimitInGB)`
 * Fix #282: DB.createXXX() does not throw exception if collection already exists.
 * Fix #275: AsyncWrite fails with OOM error, Async Write Queue has now limited size
 * Fix #272: Memory leak when using closeOnJvmShutdown (eg. any tmp map)
 * BTreeMap.containsKey is now  faster with valuesOutsideNodes
 * Store: Fix invalid checksum computation with compress enabled

Open problems:

 * Documentation
 * Small performance issues



Version 0.9.9 (2014-01-29)
--------------------------

This release should be release candidate for 1.0. However serious issues are still being discovered,
and documentation is not in releasable state. From now on I will probably roll out 0.9.10, 11, 12 and so
every week after every major bugfix. 1.0 should be released in a few weeks after bugs 'go away' and
documentation is ready.

This release fixes broken TxMaker, concurrent transactions would always generate false modification conflict.
TreeSet in BTreeMap was also seriously broken, it would not handle deletes, I had to change TreeSet format to fix it.
Write Ahead Transactions were broken and could sometime corrupt log, solution requires WAL format change.
Also compaction on store was broken.


Changes:

 * Fix #259: BTreeMap & TreeSet returns incorrect values after entries were deleted.
 * Fix #258: StoreWAL: rewrite LongStack to solve misaligned page sizes.
 * Fix #262: TxMaker concurrent transaction always fails with conflict
 * Fix #265: Compaction was broken
 * Fix #268: Pump.buildTreeMap does not set a default comparator
 * Fix #266: Serialization fail on Advanced Enums
 * Fix #264: Fix NPA if store fails to open
 * BTreeMap: add meta-information to BTree nodes to support counted BTree and per-node aggregations in future.

Open problems:

 * Open #261: SerializerPojo causes data corruption under some conditions. This is not yet confirmed and can not be reproduced.
        https://github.com/jankotek/MapDB/issues/261




Version 0.9.8 (2013-12-30)
--------------------------

This release is considered 'beta', API and store format should be now frozen.
Append-Only store and Store Pump are not part of MapDB for now. Random Access File
is enabled by default.

This release changes store format and is not backward compatible. There are
also several API changes. Also some new features are added.

Changes:

 * Append-Only store was postponed to 1.1 release. All methods are not public now.
 * Pump between stores was postponed to 1.1 release. All methods are not public now.
 * Random Access File is now default option. Memory Mapped Files can be enabled with `DBMaker.mmapFileEnable()`
 * Refactor: Utils class removed
 * Refactor: `Bind.findValsX()` renamed to `Fun.filter()`
 * StoreDirect and WAL format changes.
 * Jar is now annotated as OSGIi bundle, some classloader fixes.
 * StoreWAL commit speedup
 * Pump sorting now handles duplicates.
 * Fix #247: could not reopen collections with size counter.
 * Fix #249: SerializerPojo was not rolled back.
 * Non-existing DB.getXX() on read-only store now returns readonly empty collection
 * BTreeKeySerializer now supplies serializers
 * Serializer gives fixed size hint
 * Bind: add reverse binding and secondary keys for maps
 * Adler32 checksum replaced with stronger CRC32.
 * Fix #237, StoreAppend dont close volume on corrupted file
 * Fix #237, assertion fails with archived records
 * HTreeMap: use Hasher for collection hashes.
 * Fix #232: POJO serialization broken on complex object graphs
 * Fix #229: compression was not working.
 * `DB.createTreeMap()` and `DB.createHashMap()` now uses builder


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

