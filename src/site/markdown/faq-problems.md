Troubleshooting questions
===========================

MapDB uses chained exception so user does not have to write try catch blocks.
IOException is usually wrapped in IOError which is unchecked. So please always check root exception in the chain.

java.lang.OutOfMemoryError: Map failed
-------------------------------------
MapDB can not map file into memory. Make sure you are using latest JVM (7+).
This is common problem on Windows (use Linux). Workaround is to use slower RandomAccessFile mode: `DBMaker.newRandomAccessFileDB(file)`
	

InternalError, Error, AssertionFailedError, IllegalArgumentException, StackOverflowError and so on
-------------------------------------------------------------------------------------------------
There was an problem in MapDB. Maybe store was corrupted thanks to an internal error or disk failure.
Disabling cache  `DBMaker.cacheDisable()` or async writes `DBMaker.asyncWriteDisable()` may workaround the problem.
Please [create new bug report](https://github.com/jankotek/MapDB/issues/new) with code to reproduce this issue.

OutOfMemoryError: GC overhead limit exceeded
------------------------------------------------
Your app is creating new object instances faster then GC can collect them. This assertion is triggered if Garbage Collection consumes 98% of all CPU time.
Tune Instance Cache settings and optimize your serialization. 
You may also increase heap size by `-Xmx3G` switch. Workaround is to disable this assertion by JVM switch `-XX:-UseGCOverheadLimit`

Can not delete or rename db files on Windows
------------------------------------
MapDB uses memory mapped files, Windows locks them exclusively and prevents deletion. Solution is to close MapDB properly before JVM exits. 
Make sure you have lattest JVM.  Workaround: Use slower RandomAccessFile mode: `DBMaker.newRandomAccessFileDB(file)`
Also restarting Windows may help. 


Computer becomes very slow during DB writes
-------------------------------------------
MapDB uses most of  available CPU to speedup writes. Try to insert Thread.sleep(1) into your code, or set lower thread priority.

File locking, OverlappingFileLockException, some IOErrors
------------------------------------------------------------
You are trying to open file already opened by another MapDB. Make sure that you `DB.close()` store correctly.
Some operating systems (Windows) may leave file lock even after JVM is terminated.
You may also try to open database in read-only mode.

Strange behavior in collections
----------------------------------
Maps and Sets in MapDB should be drop-in replacement for `java.util` collections. So any significant difference is a bug.
Please [create new bug report](https://github.com/jankotek/MapDB/issues/new) with code to reproduce issue.

Hard to replicate issues
-----------------------------
If you ran into hard to replicate problem (race condition, sneak data corruption),
I would strongly suggest to record JVM execution with [Chronon](http://www.chrononsystems.com/learn-more/products-overview) 
and submit the record together with a bug report. Thanks to Chronon we can replicate and fix issues at light speed.

