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

