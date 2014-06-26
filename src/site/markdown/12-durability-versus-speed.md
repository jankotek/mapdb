Durability and speed
========================

There are several configuration options to make compromises between durability and speed. 
You may choose consistency, disk access patterns, commit type, flush type and so on. 

Transactions disabled
-----------------------

If process dies in middle of write, storage files might become inconsistent. 
For example pointer was updated with new location, where new data were not written yet. 
For this MapBD storage is protected by write-ahead-log (WAL) which replies commits in atomic fashion.
WAL is reliable and simple, and is used by many databases such as Posgresql or Mysql.

However WAL is slow, data has to be copied and synced multiple times. 
You may optionally disable WAL by disabling transactions: `DBMaker.transactionDisable()`. 
In this case you **must** correctly close store before JVM shutdown, or you loose all your data.
You may also use shutdown hook to close database before JVM exits, however this does not protect your data if JVM crashes or is killed:

```java
  DB db = DBMaker
    .transactionDisable()
    .closeOnJvmShutdown()
    .make()
```

Transaction disable (also called direct mode) will apply all changes directly into storage file.
Combine this with in-memory store or mmap files and you get very fast storage. Typical use is in scenario
where data does not have to be persisted between JVM restarts or can be easily recreated: off-heap caches. 

Other important usage is for initial data import. Transactions and no-transactions share the same 
storage format (except WAL) so one can import data very fast with transactions disabled. 
Once import is finished, store gets reopened with transactions enabled.

With transactions disabled you loose rollback capability, `db.rollback()` will throw an exception. 
`db.commit()` will have nothing to commit (all data are already stored), so it does next best thing:
Commit tries to flush all write caches and synchronizes storage files. So if you call `db.commit()`
and do not make any more writes, your store should be safe (no data loss) in case of JVM crash.


Memory mapped files (mmap)
----------------------------

MapDB was designed from ground to take advantage of mmap files. 
However mmap files are limited to 2GB by addressing limit 32bit JVM.
Mmap have lot of nasty effects on 32bit JVM, so by default we use slower and safer disk access mode called Random-Access-File (RAF).

Mmap files are much faster compared to RAF. Exact speed bonus depends on operating system and disk case management, 
but is typically between 10% and 300%. 

Memory mapped files are activated with `DBMaker.mmapFileEnable()` setting. 

One can also activate mmap files only if 64bit platform is detected: `DBMaker.mmapFileEnableIfSupported()`. 

And finally you can take advantage of mmap files in 32bit platforms by using mmap file only for small but frequently used part of storage: `DBMaker.mmapFileEnableIfSupported()`

Mmap files are highly dependent on operating system. For example on Windows you can not delete mmap file while it is locked by JVM.
If Windows JVM dies without closing mmap file, you have to restart Windows to release file lock. 

CRC32 checksums
------------------

You may want to protect your data from disk corruption. MapDB optionally supports CRC32 checksums. In this case each record stores extra 4 bytes
which contains its CRC32 checksum. If data are somehow modified or corrupted (file system or storage error), next read will fail with an exception. 
This gives early warning and prevents db from returning wrong data. 

CRC32 checksum has to be calculated on each put/modify/get operation so this option has some performance overhead. Write-ahead-log uses
CRC32 checksum by default. 

Checksum is activated by this setting: `DBMaker.checksumEnable()`. It affect storage format, so once activate you always have to reopen store 
with this setting. Also checksum can not be latter activate once store was created without it. 

TODO: CRC32 serializer link

TODO: WAL and CRC32 disable

TODO: WAL flush on commit

TODO async file sync, use futures?

