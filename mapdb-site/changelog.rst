Changelog for 2.X releases
============================


2.0-beta12 released (2015-11-26)
---------------------------------

.. post:: 2015-11-26
   :tags: release
   :author: Jan

There are no breaking changes. Fixed data corruption in large stores.

Changes:

- Fix `#635 <https://github.com/jankotek/mapdb/issues/635>`_: Commit on large store could cause data corruption in index table.

- Fix `#634 <https://github.com/jankotek/mapdb/issues/634>`_: Empty commit with Append store would log warning and could cause data corruption

- Fix `#633 <https://github.com/jankotek/mapdb/issues/633>`_: Compaction on large store fails. (thanks Matthew Schirmer for fixing it)

- DBMaker: add ``fileMmapPreclearDisable()`` option to speedup growth of memory mapped files.



2.0-beta11 released (2015-11-18)
---------------------------------

.. post:: 2015-11-18
   :tags: release
   :author: Jan

There are no breaking changes. Fixed data corruption bug and concurrency issue in ``StoreAppend``

Changes:

- ``StoreAppend`` had data corruption issue. Files larger than 16MB would get corrupted. This issue might have affected ``StoreWAL`` as well, but it is now fixed for both cases.

- ``StoreAppend`` had race condition. Fixed with temporary hot fix, which lowers performance. Working on profer fix.

- Small API changes and Javadoc fixes for better 1.0 compatibility


2.0-beta10 released (2015-10-30)
---------------------------------

.. post:: 2015-10-30
   :tags: release
   :author: Jan

There is **breaking storage format change**. This version will fail to open store created by 2.0-beta8 and previous versions.

Changes:

- Write Ahead Log was refactored into dedicated class. ``StoreWAL`` and ``StoreAppend`` now share this component.

- WAL rewritten, now uses checksums

- WAL has soft replay, commits will be faster once this feature is fully enabled.

- `StoreWAL`` and ``StoreAppend`` now passes crash resistance test (``kill -9``).

- Fixed unit tests on Windows

- Updated maven plugins and automated release script.

- 4 new release flavours (mapdb-renamed, mapdb-noassert, mapdb-nounsafe, mapdb-debug)

- ``StoreDirect`` long stack uses better compression for longs.

- ``StoreDirect`` improved memory allocator

- Fix #583. HTreeMap valueCreator was used, even if value existed.

2.0-beta9 released (2015-10-30)
---------------------------------

This release does not exist, number was used to test release script.

2.0-beta8 released (2015-09-28)
---------------------------------

.. post:: 2015-09-28
   :tags: release
   :author: Jan

There is **breaking storage format change**. This version will fail to open store created by 2.0-beta7

Changes:

- Breaking storage format change

- Fixed random data corruption which was affecting Titan and other users.

- Fixed RandomAccessFile to always fully read ``byte[]``


2.0-beta7 released (2015-09-18)
---------------------------------

.. post:: 2015-09-18
   :tags: release
   :author: Jan

There is **storage format change**: ``TreeSet`` has different format. And ``byte[]`` used as key in BTreeMap
has format change due to changed comparator.

Changes:

- ``TreeSet`` is faster and more space efficient, but that causes breaking change in storage format.

- ``Fun.BYTE_ARRAY_COMPARATOR`` comparator treated negative byte as smaller than positive byte. New comparator treats
  content of ``byte[]`` as unsigned, so 0xFF is bigger than 0x0F. Needed for better compatibility and string handling.
  This causes breaking change if ``byte[]`` is used as a key in sorted map/set.

- Fix `#561 <https://github.com/jankotek/mapdb/issues/561>`_, queues fails after compaction, when their preallocated recid disappears

- Fix `#562 <https://github.com/jankotek/mapdb/issues/562>`_, BTreeMap/HTreeMap: make KeySet public and add sizeLong() method.

- Fix `#468 <https://github.com/jankotek/mapdb/issues/468>`_, Queues: NPE on createCircularQueue

- Fix `#541 <https://github.com/jankotek/mapdb/issues/541>`_, BTreeMap, HTreeMap: make maps and sets serializable using java serialization.

- Modify POJO serialization to handle object ``writeReplace()`` method. See `#495 <https://github.com/jankotek/mapdb/issues/495>`_

- Performance: HTreeMap code sliced into several smaller methods, more JIT friendly.

- BTreeMap: fix composite keys, fix ``TreeMap_Composite_Key`` example

- TxEngine: fix null handling in CAS.

- DBMaker: fileMmapEnableIfSupported() does not support 64bit Windows anymore

- StoreWAL: compaction broken, remove compaction if transactions are enabled.

- Volume: fix clear method

- Fix `#581 <https://github.com/jankotek/mapdb/issues/581>`_, BTreeMap: get() did not followed link, was broken under concurrent update.


2.0-beta6 released (2015-08-18)
---------------------------------

.. post:: 2015-08-18
   :tags: release
   :author: Jan

There is **storage format change**: array hashing has changed. If you use any array such as ``Object[]``, ``byte[]``,
``long[]``, etc... as key in ``HashMap`` it is not readable in new version.

Hashing in Java is broken. ``Arrays.hash()`` and ``String.hashCode()`` returns too many collisions.
This version replaces Java hashing with XXHash and other improved algorithms.

This version also fixes number of bugs. ``BTreeMap`` with ``valuesOutsideNodesEnable()`` had storage space leak,
now it uses much less space and is faster with updates.

Changes:

- Hash code calculation has changed in: ``Serializer.OBJECT_ARRAY``, ``Serializer.BYTE_ARRAY``, ``Serializer.BYTE_ARRAY_NOSIZE``,
  ``Serializer.CHAR_ARRAY``, ``Serializer.INT_ARRAY``, ``Serializer.LONG_ARRAY``, ``Serializer.DOUBLE_ARRAY``,
  ``Serializer.FLOAT_ARRAY``, ``Serializer.SHORT_ARRAY`` and ``Serializer.RECID_ARRAY``,

- Hash did not changed in ``Serializer.STRING``, it still uses ``String.hashCode()``. But this
  hash is broken and for HashMap Key Serializer one should use new ``Serializer.STRING_XXHASH``

- XXHash64 hash from LZ4-Java project was integrated into MapDB (Volume, DataIO, UnsafeStuff...). Will be fully utilized in next release

- Some serializers now use Hash Seed. That is better protection from Hash Collision attack.

- There is new experimental ``StoreArchive``. It is faster, uses less space, but only readonly storage.
  Its not finished, and next release WILL change its storage format. For details checkout
  `bug report <https://github.com/jankotek/mapdb/issues/93>`_

- Fixed `#403 <https://github.com/jankotek/mapdb/issues/403>`_. BTreeMap: storage space leak with valuesOutsideNodesEnable()
  Old external values were not deleted on update and removal from BTreeMap. Now this case is much faster on updates

- Fixed `#430 <https://github.com/jankotek/mapdb/issues/430>`_. Fun: Fun.filter should use the comparator of the filtered set.

- Fixed `#546 <https://github.com/jankotek/mapdb/issues/546>`_. Rewrote persistent Serializers in ``DB``. Fixed some warnings.

- Added ``Store.fileLoad()`` to pre-cache file content of mmap files

- Added ``DBMaker.CC()`` to access Compiler Config at runtime.

- Fixed `#385 <https://github.com/jankotek/mapdb/issues/385>`_. Untrusted serializers are now limited and can not read beyong record size.

- Fixed `#556 <https://github.com/jankotek/mapdb/issues/556>`_. CircularQueue fails to take 1 element if queue is of size 4 and not full.

- Started work on BTreeMap online compaction, no working code yet. See
  `#545 <https://github.com/jankotek/mapdb/issues/545>`_ and `#97 <https://github.com/jankotek/mapdb/issues/97>`_

2.0 beta5 released (2015-08-12)
---------------------------------

.. post:: 2015-08-12
   :tags: release
   :author: Jan

Added incremental backups. Less fragmentation. Custom class loaders.

Changes:

- MapDB now has full and incremental backups. Checkout examples for details:
  `full <https://github.com/jankotek/mapdb/blob/master/src/test/java/examples/Backup.java>`_ and
  `incremental <https://github.com/jankotek/mapdb/blob/master/src/test/java/examples/Backup_Incremental.java>`_

- Fixed `#555 <https://github.com/jankotek/mapdb/issues/555>`_. Class Loader used by POJO serialization is now customizable.
  Checkout ``DBMaker.serializerClassLoader()`` and ``DBMaker.serializerRegisterClass()`` methods

- Reuse recid is now enabled by default. This causes smaller fragmentation.

Version 2.0-beta4 (2015-08-03)
-----------------------------------

Improvements in crash recovery. Reworked HTreeMap expiration based on store size. Add store allocation options.

Changes:

- Improved crash recovery with mmap files.

- Store now reports free space correctly.

- HTreeMap expiration based on maximal storage size now works much better. Checkout
  `example <https://github.com/jankotek/mapdb/blob/master/src/test/java/examples/CacheOffHeapAdvanced.java>`_
  for details.

- Add ``DBMaker.allocateStartSize()`` and ``DBMaker.allocateIncrement()`` options to control initial store size
  and how store size increments.

- StoreDirect and StoreWAL had bug within compaction. That is now fixed.

- Optimize RandomAccessFile and mmap file Volumes. IO should be bit faster.

- Fixed POJO serialization on Android 4.2+ devices.


Version 2.0-beta3 (2015-07-23)
-----------------------------------

Bug fix in Write Ahead Log. Added file locking. Crash recovery improved but still needs more testing.
Not sure if disk compaction and commits works on Windows with mmap files.

Changes:

- Fixed issue in Write-Ahead-Log. Single record modified by many commit would not be persisted after full replay.

- Added file locking to prevent multiple processes opening the same store. By default it uses ``FileChannel.lock()``
  There are two new options: ``DBMaker.fileLockDisable()`` to disable locking, and ``

- many new stress tests

- Fixed: StoreAppend.get() throws ``NullPointerException`` with transaction disabled.

- More changes into mmap files. Improve handling in case of low-disk space.

Version 2.0-beta2 (2015-07-09)
-------------------------------------
Lot of bugfixing. Cleaner Hack for mmap files is disabled now. It is recommended **not to use mmap files on Windows
for now, until we do proper investigation**.
This release also provides ``mapdb-renamed`` maven package, with package name renamed to ``org.mapdb20``

Improved crash recovery for Write Ahead Log and Append Only stores. In some cases MapDB would not replay log,
on start after JVM crash. This could potentially lead to data corruption. Crash recovery is still not perfect
and will need future improvements.

Memory mapped files could cause JVM crash, for details see `Issue 442 <https://github.com/jankotek/mapdb/issues/442>`_.
Crash would happen if write to mapped ``ByteBuffer`` would fail for some reasons (empty disk space).
It could also happen if unmapped buffer was accessed.

There were number of changes to solve this issue. Most importantly now **MapDB 2.0 now has cleaner hack
disabled by default**. File handles are not released until Garbage Collection occur. This might
cause file handle leaks. On Windows it prevents compaction and commits, since old file is locked and can not be renamed.
There is new option ``DBMaker.fileMmapCleanerHackEnable()`` to enable Cleaner Hack and release file handle
when DB is closed.

Other changes:

- Race condition between  ``StoreDirect.put()`` and ``StoreDirect.compact()`` is now fixed,
  for details see `Issue 542 <https://github.com/jankotek/mapdb/issues/542>`_. As result
  StoreDirect is now exclusively locked during compaction. With transaction disabled data can not be read or updated,
  while compaction is running.
  Performance improvement should be in next release.

- Build script now produces separate jar file with package renamed to ``org.mapdb20`` and Maven name changed to
  ``mapdb-renamed``. This should make it easy to use multiple versions in single JVM and migrate data between them.

- ``Serializer.JAVA`` serializer did not work. Fixed #536.

- ``Bind.histogram()`` would not recreate empty secondary set. Fix #453.

- HTreeMap: fix #538, NullPointerException when ``HTreeMap.get()`` was called with non existing key while overflow was enabled

- Fix custom serializers ignored on map creation #540

Version 2.0-beta1 (2015-06-31)
-------------------------------------

Storage format and API freeze. Fixed concurrent race conditions and crashes. Storage format has changed since Alpha3.
More details about this release in the `blog post <http://kotek.net/blog/MapDB_2_beta_1>`_

List of possible problems:

- Crash recovery for Write-Ahead-Log and compaction is not completely verified. Data should be safe, but recovery might require user intervention to delete some old files etc.

- MMap files could `cause JVM to crash <https://github.com/jankotek/mapdb/issues/442>`_. Older 1.0 branch also has this bug, it should be fixed in two weeks in 1.0.8 and 2.0-beta2.

- AppendOnly store does not have compaction. It also needs more testing for crash recovery.

- Several performance optimizations are disabled. Stability over speed is preferred. Many parts could be 4x faster, but those optimizations are postponed to 2.1 release. However 2.0 is still much faster compared to 1.0 release.


Version 2.0-alpha3 (2015-06-16)
-------------------------------------

Last unstable version before beta release.


Changelog for 1.X releases
===========================

Version 1.0.8 (2015-07-09)
-------------------------------------

Fixed several bugs.

Changes:

 - Memory Mapped files could cause JVM crash (``~StubRoutines::jlong_disjoint_arraycopy``).
   For details see `Issue 442 <https://github.com/jankotek/mapdb/issues/442>`_.
   This was linked to ByteBuffer Cleaner Hack which unmaps buffer when file is closed,
   rather than waiting to Garbage Collection. Cleaner Hack was disabled by default in 2.0.
   In 1.0 it is left enabled for compatibility reason. There is new setting
   ``DBMaker.mmapFileCleanerHackDisable()`` to disable it, in case you experience problems.

 - Fixed `#452 <https://github.com/jankotek/MapDB/issues/452>`_: ``pumpSource()`` would fails with empty iterator

 - Fixed `#453 <https://github.com/jankotek/MapDB/issues/453>`_: ``Bind.histogram()`` does not recreate content if secondary collection is empty

 - Fixed `#362 <https://github.com/jankotek/MapDB/issues/362>`_: failing unit tests on Windows

 - Fixed `#517 <https://github.com/jankotek/MapDB/issues/517>`_: DB: non serializable serializer could leave name catalog in semi-locked state

 - Fixed `#513 <https://github.com/jankotek/MapDB/issues/513>`_: Atomic.Var: store does not allow ``null`` values. Change initial value from `null` to empty string `""`.

 - Fixed `#523 <https://github.com/jankotek/MapDB/issues/523>`_: Read-only mmap file not unmapped after close.

 - Fixed `#534 <https://github.com/jankotek/MapDB/issues/534>`_: BTreeMap: IndexOutOfBoundsException under concurrent access

 - ``mapdb-renamed`` was update to 1.0.8. There is script to make release semi-automated

Open issues

 - there are reported isses with data corruption in Write-Ahead-Log.
   Most notably `#509 <https://github.com/jankotek/MapDB/issues/509>`_ and `#515 <https://github.com/jankotek/MapDB/issues/515>`_.
   I can not reproduce it yet, but working on fixing those.

 - List of `open issues in 1.0 branch <https://github.com/jankotek/mapdb/labels/1.0>`_.


Version 1.0.7 (2015-02-19)
--------------------------

Fixed bugs in Write-Ahead-Log and ``HTreeMap`` entry expiration.

Changes:

- Fixed serializer for newer android versions. `Link <https://github.com/koa/MapDB/commit/da938caac36f807c9f737ec6b06c7b4d72a91a2a>`_

- Fixed `#443 <https://github.com/jankotek/MapDB/issues/443>`_ In-memory compaction does not delete temp files

- Fixed `#442 <https://github.com/jankotek/MapDB/issues/442>`_ DirectByteBuffer unmapping and Async Write could cause JVM crash on compaction and commit

- Fixed `#419 <https://github.com/jankotek/MapDB/issues/419>`_ DB.getHashSet() does not restore expiration settings

- Fixed `#418 <https://github.com/jankotek/MapDB/issues/418>`_ HTreeMap expiration was broken

- Fixed `#400 <https://github.com/jankotek/MapDB/issues/400>`_ HTreeMap.get() resets TTL to zero in some cases

- Fixed `#417 <https://github.com/jankotek/MapDB/issues/417>`_ Infinite loop in Store.calculateStatistics()

- Fixed `#374 <https://github.com/jankotek/MapDB/issues/374>`_ Map value creator is never called!

- Fixed `#414 <https://github.com/jankotek/MapDB/issues/414>`_ Snapshots were not working under some conditions

- Fixed `#381 <https://github.com/jankotek/MapDB/issues/381>`_ WAL corruption with deletes

- Fixed `#364 <https://github.com/jankotek/MapDB/issues/364>`_ WAL corruption with async writes

- Fixed `#373 <https://github.com/jankotek/MapDB/issues/373>`_ SerializerPojo throws NotSerializableException for Class field

- Fixed `#445 <https://github.com/jankotek/MapDB/issues/445>`_ Race condition in Hashtable cache caused ClassCastException

Open issues:

- `A few <https://github.com/jankotek/MapDB/labels/1.0>`_ I could not replicate.

Version 1.0.6 (2014-08-07)
--------------------------

Fixed problem in transaction log replay after unclean shutdown WAL
checksum was broken, so it was disabled.

Changes:

-  Fix #359: WAL log replay could fail after unclean shutdown
-  Workaround #366: WAL checksum was broken, disable WAL checksum.

Version 1.0.5 (2014-07-15)
--------------------------

Fixed transaction log replay failure. Fixed race condition in async
writes. Some methods changed from protected to public, to allow external
access.

Changes:

-  Fix #346: WAL log corruption when killing the mapdb process.
   Discarding corrupted log was not reliable.
-  Fix #356: ``asyncWriteEnable()`` had race condition in record
   preallocation. Could result to data loss. Feature is too complex to
   fix, so was removed. Expect minor performance regression.
-  Fix DB: TreeMap Pump keyExtractor was not used. Would cause problem
   with Tuple2 pairs
-  Fix #358: set correct hasher when open exist hash tree map
-  Atomic classes now expose recid via public ``getRecid()`` method
-  DB now exposes Name Catalog via public methods. External libraries
   can manipulate catalog content.

Version 1.0.4 (2014-06-26)
--------------------------

Fixed transaction file locking on Windows.

Changes:

-  Fix #326, #327, #346 and #323: Transaction log was not unlocked on
   Windows, causing various issues. Kudos to Rémi Alvergnat for
   discovering and fixing it.
-  Fix #335: Ensures that file resources are always released on close.
   Kudos to Luke Butters.

Version 1.0.3 (2014-06-08)
--------------------------

Fixed new space allocation problem, file now increases in 1MB
increments. Updated copyright info and added notice.txt

Changes:

-  Fix #338 Excess storage size on Memory mapped files
-  Add notice.txt with list of copyright holders
-  Updated javadocs

Open problems:

-  Open #304 and #283: BTreeMap fails under concurrent access.
   Unconfirmed and can not reproduce. It needs more investigation.
-  Documentation

Version 1.0.2 (2014-06-02)
--------------------------

Fixed ``Serializer.CompressionWrapper()``, this bug does not affect
``DBMaker.compressionEnable()``

Changes:

-  Fix #321: Small behaviour regression in BTreeMap Pump
-  Fix #332: ``Serializer.CompressionWrapper()`` decompressed wrong
   data. Reverted some optimization which caused this issue.

Open problems:

-  Open #304 and #283: BTreeMap fails under concurrent access.
   Unconfirmed and can not reproduce. It needs more investigation.
-  Documentation

Version 1.0.1 (2014-05-05)
--------------------------

Fixed MRU cache and BTree Pump Presort.

Changes:

-  Fix #320: BTreeMap pump presort fails
-  Fix #319: ClassCastException in the Cache.LRU

Open problems:

-  Open #304 and #283: BTreeMap fails under concurrent access.
   Unconfirmed and can not reproduce. It needs more investigation.
-  Documentation

Version 1.0.0 (2014-04-27)
--------------------------

Fixed a few minor problems. Lot of code cleanups.

This is first stable release with long term support. Thanks to everyone
who helped to get MapDB this far.

Changes:

-  Fix #315: DB.delete(name) deletes substring matches
-  SerializerPojo: add interceptors to alter serialized objects

Open problems:

-  Open #304 and #283: BTreeMap fails under concurrent access.
   Unconfirmed and can not reproduce. It needs more investigation.
-  Documentation

Version 0.9.13 (2014-04-16)
---------------------------

There was another problem with mmap files larger than 2GB.

This is yet another release candidate for 1.0.0. Stable release should
follow in 9 days if no problems are found.

Changes:

-  Fix #313: mmap files larger than 2GB could not be created

Open problems:

-  Open #304 and #283: BTreeMap fails under concurrent access: .
   Unconfirmed and needs more investigation.
-  Documentation

Version 0.9.12 (2014-04-15)
---------------------------

Previous release was broken, store larger than 16 MB or 2 GB could not
be created, that is fixed now. This release also brings number of small
cleanups and improved memory consumption.

The store format has changed yet again in backward incompatible way. The
chunk (slice) size is now 1 MB.

This is yet another release candidate for 1.0.0. Stable release should
follow in 10 days if no problems are found.

Changes:

-  Format change! Chunk (slice) size reduced from 16MB to 1MB, solved
   many Out Of Memory errors.
-  Fix #313: mmap files larger than 2GB could not be created
-  Fix #308: ArrayIndexOutOfBoundsException if store is larger 16MB.
-  Fix #312: error while opening db with readonly
-  Fix #304: BTreeMap.replace() fails under concurrent access
-  Large scale code cleanup before 1.0.0 freeze and release
-  DBMaker: rename ``syncOnCommitDisable()`` to
   ``commitFileSyncDisable()``
-  DBMaker: add ``newHeapDB()`` option, this store does not use
   serialization and is almost as fast as java collections

Open problems:

-  Open #304 and #283: BTreeMap fails under concurrent access: .
   Unconfirmed and needs more investigation.
-  Documentation

Version 0.9.11 (2014-03-24)
---------------------------

This fixes serious race condition for in-memory store. Also there is fix
for secondary collections containing wrong values. And finally all file
locking problems on Windows should be solved.

As result the store format was completely changed. There is no backward
compatibility with previous releases. MapDB now allocates memory in 16MB
chunks (slices), so new empty database will always consume a few MB of
memory/disk space.

This is last 0.9.x release, next release will be 1.0.0.

Changes:

-  Fix #303 and #302: There was race condition in Volumes, which caused
   data corruption under concurrent access.
-  Fix #252 and #274: File locking on Windows is now completely solved.
   We no longer use overlapping ByteBuffers which were source of errors.
-  Fix #297: BTreeMap modification listeners received wrong key. As
   result secondary collections could contain wrong data.
-  Fix #300: ``Queue.offer()`` should return false, not throw an
   ``IllegalStateException()``. Not really isssue since MapDB does not
   have queues with limited size yet.
-  Engine: add close listener, to prevent NPE on shutdown in HTreeMap
   Cache
-  Maven: do not run tests in parallel, it causes out of memory errors
-  StoreWAL: do not delete log file after every commit, keep it around.
   This should speedup commits a lot
-  Volume: mmap file chunks (slices) were synced multiple times, causing
   slow sync and commits
-  Volume: change 'chunk size' (slice size) from 1GB to 16MB and disable
   incremental allocation.
-  DBMaker: The 'full chunk allocation' option was removed and is now on
   by default.
-  DBMaker: method ``newDirectMemoryDB()`` replaced with
   ``newMemoryDirectDB()``
-  Fun: Added Tuple5 and Tuple6 support

Open problems:

-  Open #304 and #283: BTreeMap fails under concurrent access: .
   Unconfirmed and needs more investigation.
-  Documentation

Version 0.9.10 (2014-02-18)
---------------------------

Yet another bug fix release before 1.0. There is fix for serious data
corruption with disabled transactions. Async-Writer queue is no longer
unbounded to prevent memory leaks. In-memory cache is now much easier to
use with memory size limit, checkout
``Map cache = DBMaker.newCache(sizeLimitInGB)``

Changes:

-  Fix #261: SerializerPojo could cause data corruption with transaction
   disabled.
-  Fix #281: txMaker.makeTx().snapshot() does not work.
-  Fix #280: Check for parent folder when opening file db.
-  Fix #288: syncOnCommitDisable() does not work at WAL
-  Fix #276: In-memory cache based on HTreeMap now has memory size
   limit. Checkout ``Map cache = DBMaker.newCache(sizeLimitInGB)``
-  Fix #282: DB.createXXX() does not throw exception if collection
   already exists.
-  Fix #275: AsyncWrite fails with OOM error, Async Write Queue has now
   limited size
-  Fix #272: Memory leak when using closeOnJvmShutdown (eg. any tmp map)
-  BTreeMap.containsKey is now faster with valuesOutsideNodes
-  Store: Fix invalid checksum computation with compress enabled

Open problems:

-  Documentation
-  Small performance issues

Version 0.9.9 (2014-01-29)
--------------------------

This release should be release candidate for 1.0. However serious issues
are still being discovered, and documentation is not in releasable
state. From now on I will probably roll out 0.9.10, 11, 12 and so every
week after every major bugfix. 1.0 should be released in a few weeks
after bugs 'go away' and documentation is ready.

This release fixes broken TxMaker, concurrent transactions would always
generate false modification conflict. TreeSet in BTreeMap was also
seriously broken, it would not handle deletes, I had to change TreeSet
format to fix it. Write Ahead Transactions were broken and could
sometime corrupt log, solution requires WAL format change. Also
compaction on store was broken.

Changes:

-  Fix #259: BTreeMap & TreeSet returns incorrect values after entries
   were deleted.
-  Fix #258: StoreWAL: rewrite LongStack to solve misaligned page sizes.
-  Fix #262: TxMaker concurrent transaction always fails with conflict
-  Fix #265: Compaction was broken
-  Fix #268: Pump.buildTreeMap does not set a default comparator
-  Fix #266: Serialization fail on Advanced Enums
-  Fix #264: Fix NPA if store fails to open
-  BTreeMap: add meta-information to BTree nodes to support counted
   BTree and per-node aggregations in future.

Open problems:

-  Open #261: SerializerPojo causes data corruption under some
   conditions. This is not yet confirmed and can not be reproduced.
   https://github.com/jankotek/MapDB/issues/261

Version 0.9.8 (2013-12-30)
--------------------------

This release is considered 'beta', API and store format should be now
frozen. Append-Only store and Store Pump are not part of MapDB for now.
Random Access File is enabled by default.

This release changes store format and is not backward compatible. There
are also several API changes. Also some new features are added.

Changes:

-  Append-Only store was postponed to 1.1 release. All methods are not
   public now.
-  Pump between stores was postponed to 1.1 release. All methods are not
   public now.
-  Random Access File is now default option. Memory Mapped Files can be
   enabled with ``DBMaker.mmapFileEnable()``
-  Refactor: Utils class removed
-  Refactor: ``Bind.findValsX()`` renamed to ``Fun.filter()``
-  StoreDirect and WAL format changes.
-  Jar is now annotated as OSGIi bundle, some classloader fixes.
-  StoreWAL commit speedup
-  Pump sorting now handles duplicates.
-  Fix #247: could not reopen collections with size counter.
-  Fix #249: SerializerPojo was not rolled back.
-  Non-existing DB.getXX() on read-only store now returns readonly empty
   collection
-  BTreeKeySerializer now supplies serializers
-  Serializer gives fixed size hint
-  Bind: add reverse binding and secondary keys for maps
-  Adler32 checksum replaced with stronger CRC32.
-  Fix #237, StoreAppend dont close volume on corrupted file
-  Fix #237, assertion fails with archived records
-  HTreeMap: use Hasher for collection hashes.
-  Fix #232: POJO serialization broken on complex object graphs
-  Fix #229: compression was not working.
-  ``DB.createTreeMap()`` and ``DB.createHashMap()`` now uses builder

Version 0.9.7 (2013-10-28)
--------------------------

Store format is not backward compatible. Fixed locking issues on
Windows. Concurrent Transactions (TxMaker) reworked and finally fixed.
Added ``DBMaker.fullChunkAllocationEnable()`` to enable disk space
allocation in 1GB chunks. In-memory store now can be compacted. Fixed
race condition in ``BTreeMap.put()``.

Changes:

-  Rework integer/long serialization.
-  Fix #214: Queues now implement ``BlockingQueue`` interface
-  Refactor ``DBMaker`` so it uses properties. Easy to load/save config.
-  TxMaker reworked, fixed concurrency issue.
-  StoreDirect & WAL use stricter locking.
-  Fix #218 and #192, locking issues on Windows during compaction
   solved.
-  Added Tuple comparators.
-  Fixed several issues in Data Pump.
-  Fix #187, Reference to named objects/collections should be
   serializable
-  BTreeMap: fix #209, put operation was not thread safe.

Version 0.9.6 (2013-09-27)
--------------------------

Concurrent Transactions (TxMaker) almost fixed. Backward incompatible
store format change. Snapshots are no longer enabled by default.

Open issues:

-  Fix #201: failing test suggests that Concurrent Transactions contains
   race condition.

Changes:

-  Concurrent Transactions were broken and are now completely
   re-written.
-  Snapshots are no longer enabled by default.
   ``DbMaker.snapshotDisable()`` replaced by
   ``DbMaker.snapshotEnable()``
-  StoreDirect now has checksum which refuses to reopen incorrectly
   closed stores. In result stores created with 0.9.5- can not be open.
-  Store now supports recid preallocation, this leads to faster insert.
-  Fixed performance issue with batch imports
-  Fixed performance issues in free space management
-  Volume has lighter exception handling, result is small speed
   improvement
-  StoreHeap rewritten. Now it has full transactions.
-  Changes in locking to make it more robust and prevent deadlocks
-  Java Assertions used instead of ``IllegalArgumentException`` and
   ``InternalError``. Please use ``-ea`` JVM switch when running MapDB
-  SerializerBase: various optimizations so methods fits into JIT limits

Version 0.9.5 (2013-08-26)
--------------------------

Bugfixes from previous release. Fixed data corruption bugs, upgrade
strongly recommended.

Changes:

-  Fix #177: broken compression
-  Fix data corruption with disabled transactions
-  CRC32 replaced with faster Adler32, **store which uses checksum is no
   backward compatible**
-  Fix #167: Add DB.exists() method to check if named record/collection
-  Fix #167: Add a makeOrGet to DB Collection maker API.
-  StoreWAL: fix some TOMBSTONE details
-  Bind: Add methods to find subsets on composite sets

Version 0.9.4 (2013-08-09)
--------------------------

**No backward compability** with previous versions. Some parts were
completely rewritten for better free space management. Many small
improvements.

Changes:

-  HTreeMap now supports automatic LRU eviction based on size or access
   time.
-  DB TreeMap, TreeSet and HashMap now uses builder class.
-  Reworked SerializerBase
-  Reworked Serializer implementations
-  Checksum, Compression and Encryption integrated into store, now much
   faster
-  Add ``.sizeLong()`` into HTreeMap and BTreeMap.
-  Fixed data corruption in HTreeMap
-  Rewritten space reclaim algorithm
-  Store now has maximal size limit
-  ``DBMaker.writeAheadLogDisable()`` renamed to
   ``DBMaker.transactionDisable()``
-  TxMaker is now concurrent
-  BTreeMap now supports descending maps

Version 0.9.3 (2013-06-02)
--------------------------

CRITICAL upgrade urgency. This release fixes number of critical bugs in
Write Ahead Log. It also adds support for advanced Java Serialization,
which was reported many times as a bug.

Changes:

-  FIX Issue #17 - Serializer fails in some cases (writeExternal and
   readExternal methods)
-  FIX Issue #136 & #132 - Data corruption in Write Ahead Log after
   rollback or reopen.
-  FIX Issue #137 - Deadlock while closing AsyncWriteEngine Credit Jan
   Sileny
-  FIX Issue #139 - rolled back TX should not throw exception on close.
-  FIX Issue #135 - SerializerPojo registered classes problem. Credit
   Jan Sileny
-  ADD ``DBMaker.syncOnCommitDisable()`` parameter
-  ADD all stuff in ``DataIO.ByteArrayDataOutput`` and ``DataInput2`` is
   public. It also extends In/OutputStream now.

Version 0.9.2 (2013-05-19)
--------------------------

CRITICAL upgrade urgency. This release fixes some critical bugs. It also
improves performance and introduces Data Pump.

Open Issues:

-  Issue #17 - Serializer fails in some cases (writeExternal and
   readExternal methods)

Changes:

-  FIX Issue #119 - BTreeMap did not released locks with multiple
   transactions
-  FIX Issue #125 - calling close twice failed.
-  FIX race condition in HTreeMap
-  ADD ``ByteBuffer`` now uses ``duplicate()`` instead of
   synchronization. Better concurrency
-  ADD Issue #123 - Replace RandomAccessFile by FileChannel and improve
   performance on 32bit systems.
-  ADD Delta Packing for tuples
-  ADD better serialization for small strings
-  ADD improve Javadoc, use Pegdown Doclet so Javadoc can be written in
   markdown
-  ADD reuse DataOutput instances, performance improvement
-  ADD datapump to create BTreeMap from large unsorted data set in
   linear time. Checkout ``Huge_Insert`` example
-  ADD improve AsyncWriteEngine performance by removing Write Queue

Version 0.9.1 (2013-04-14)
--------------------------

CRITICAL upgrade urgency. This release fixes number of critical bugs
from first release, including data store corruption and crashes.

Open issues:

-  Issue #119 - BTreeMap (TreeMap) may not release all locks and
   consequently crash. This is unconfirmed and hard to replicate
   concurrent bug. I temporarily added assertion which slows down
   BTreeMap updates, but helps to diagnose this problem
-  Issue #118 - StoreWAL fails to create log for unknown reasons and
   crashes. Not reproduced yet, need to investigate.

Changes:

-  FIX #111 - Compaction fails with large data sets
-  FIX - BTreeKeySerializer.ZERO\_OR\_POSITIVE\_INT was broken
-  FIX #89 - StoreAppend reopen failed
-  FIX #112 - Compaction fails with WAL enabled
-  FIX #114 - RandomAccessFile fails with WAL
-  FIX #113 - MemoryMappedFile was not unlocked on Windows after DB
   close
-  FIX - rewrite AsynwWriteEngine, fix many concurrent bugs
-  FIX - Files were not synced on DB.close(). Possible data loss.
-  FIX - free space reuse did not worked in StoreDirect and StoreWAL.
   Storage file grown infinitely with each update.
-  FIX #116 - HTreeMap.isEmpty returned wrong result.
-  FIX #121 - WAL could get corrupted in some cases.
-  ADD - basic benchmark
-  ADD - error message if file rename fails after compaction finishes
-  ADD - #119 BTreeMap locking could not be fixed, I added assertion to
   help diagnose issue. Small performance drop on BTreeMap updates.
-  ADD - performance improvement if Snapshot engine is not used.

Version 0.9.0 (2013-04-01)
--------------------------

First release with stable API and storage format.

Upgrade urgency levels:
-----------------------

-  LOW: No need to upgrade unless there are new features you want to
   use.
-  MODERATE: Program an upgrade of the DB engine, but it's not urgent.
-  HIGH: There is a critical bug that may affect a subset of users.
   Upgrade!
-  CRITICAL: There is a critical bug affecting MOST USERS. Upgrade ASAP.

