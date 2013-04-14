
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

