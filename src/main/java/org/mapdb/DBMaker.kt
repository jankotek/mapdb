package org.mapdb

import org.mapdb.volume.*
import java.io.File


/**
 * <p>
 * A builder class to create and open new database and individual collections.
 * It has several static factory methods.
 * Method names depends on type of storage it opens.
 * {@code DBMaker}is typically used this way
 * </p>
 *
 * <pre>
 *  DB db = DBMaker
 *      .memoryDB()             //static method
 *      .transactionEnable()    //configuration option
 *      .make()                 //opens db
 * </pre>
 *
 *
 *
 * @author Jan Kotek
 */
//TODO unsafe
//TODO appendFileDB
//TODO archiveFileDB
//TODO factory methods for hashMap, treeMap, cache etc??
object DBMaker{

    enum class StoreType{
        onheap, directbuffer, bytearray,
        fileRaf, fileMMap, fileChannel
    }

    /**
     * Creates new database in temporary folder. Files are deleted after store was closed
     */
    @JvmStatic fun tempFileDB(): Maker {
        //TODO on unix this file should be deleted just after it was open, verify compaction, rename etc
        val file = File.createTempFile("mapdb","temp")
        file.delete()
        file.deleteOnExit()
        return fileDB(file).fileDeleteAfterClose()
    }

    @JvmStatic fun fileDB(file:String): Maker {
        return Maker(StoreType.fileRaf, file = file)
    }

    @JvmStatic fun fileDB(file: File): Maker {
        return fileDB(file.path)
    }

    /**
     * Creates new in-memory database which stores all data on heap without serialization.
     * This mode should be very fast, but data will affect Garbage Collector the same way as traditional Java Collections.
     */
    @JvmStatic fun heapDB(): Maker {
        return Maker(StoreType.onheap)
    }

    /**
     * Creates new in-memory database. Changes are lost after JVM exits.
     * This option serializes data into {@code byte[]},
     * so they are not affected by Garbage Collector.
     */
    @JvmStatic fun memoryDB(): Maker {
        return Maker(StoreType.bytearray)
    }

    /**
     * <p>
     * Creates new in-memory database. Changes are lost after JVM exits.
     * </p><p>
     * This will use {@code DirectByteBuffer} outside of HEAP, so Garbage Collector is not affected
     * You should increase ammount of direct memory with
     * {@code -XX:MaxDirectMemorySize=10G} JVM param
     * </p>
     */
    @JvmStatic fun memoryDirectDB(): Maker {
        return Maker(StoreType.directbuffer)
    }


    @JvmStatic fun volumeDB(volume: Volume, volumeExists: Boolean): Maker {
        return Maker(_storeType = null, _customVolume =volume, _volumeExist =volumeExists)
    }


    @JvmStatic fun memoryShardedHashSet(concurrency:Int): DB.HashSetMaker<Any?> {
        val db = DB(store = StoreOnHeap(), storeOpened = false, isThreadSafe = true)
        return DB.HashSetMaker<Any?>(db,"map",storeFactory = {
                StoreDirect.make(isThreadSafe = false)
            })
            .layout(concurrency = concurrency, dirSize = 1.shl(CC.HTREEMAP_DIR_SHIFT), levels = CC.HTREEMAP_LEVELS)
    }

    @JvmStatic fun heapShardedHashSet(concurrency:Int): DB.HashSetMaker<Any?> {
        val db = DB(store = StoreOnHeap(), storeOpened = false, isThreadSafe = true)
        return DB.HashSetMaker<Any?>(db,"map",storeFactory = {
                StoreOnHeap(isThreadSafe = false)
            })
            .layout(concurrency = concurrency, dirSize = 1.shl(CC.HTREEMAP_DIR_SHIFT), levels = CC.HTREEMAP_LEVELS)
    }

    @JvmStatic fun memoryShardedHashMap(concurrency:Int): DB.HashMapMaker<*,*> {
        val db = DB(store = StoreOnHeap(), storeOpened = false, isThreadSafe = true)
        return DB.HashMapMaker<Any,Any>(db,"map",storeFactory = {
                StoreDirect.make(isThreadSafe = false)
            })
            .layout(concurrency = concurrency, dirSize = 1.shl(CC.HTREEMAP_DIR_SHIFT), levels = CC.HTREEMAP_LEVELS)
    }

    @JvmStatic fun heapShardedHashMap(concurrency:Int): DB.HashMapMaker<*,*> {
        val db = DB(store = StoreOnHeap(), storeOpened = false, isThreadSafe = true)
        return DB.HashMapMaker<Any,Any>(db,"map",storeFactory = {
                StoreOnHeap(isThreadSafe = false)
            })
            .layout(concurrency = concurrency, dirSize = 1.shl(CC.HTREEMAP_DIR_SHIFT), levels = CC.HTREEMAP_LEVELS)
    }

    class Maker(
            private var _storeType: StoreType?,
            private val _customVolume: Volume?=null,
            private val _volumeExist:Boolean?=null,
            private val file:String?=null){

        private var _allocateStartSize:Long = 0L
        private var _allocateIncrement:Long = 0L
        private var _transactionEnable = false
        private var _fileDeleteAfterClose = false
        private var _fileDeleteAfterOpen = false
        private var _isThreadSafe = true
        private var _concurrencyScale: Int = 1.shl(CC.STORE_DIRECT_CONC_SHIFT)
        private var _cleanerHack = false
        private var _classLoader = Thread.currentThread().contextClassLoader

        private var _fileMmapPreclearDisable = false
        private var _fileLockWait = 0L
        private var _fileMmapfIfSupported = false
        private var _closeOnJvmShutdown = 0
        private var _readOnly = false
        private var _checksumStoreEnable = false
        private var _checksumHeaderBypass = false

        fun transactionEnable():Maker{
            _transactionEnable = true
            return this
        }

        fun allocateStartSize(size:Long):Maker{
            _allocateStartSize = size
            return this
        }

        fun allocateIncrement(incrementSize:Long):Maker{
            _allocateIncrement = incrementSize;
            return this
        }

        fun classLoader(classLoader:ClassLoader):Maker{
            _classLoader = classLoader
            return this
        }

        @Deprecated(message="method renamed to `fileDeleteAfterClose()`")
        fun deleteFilesAfterClose():Maker{
            _fileDeleteAfterClose = true
            return this
        }

        fun fileDeleteAfterClose():Maker{
            _fileDeleteAfterClose = true
            return this
        }

        fun fileDeleteAfterOpen():Maker{
            _fileDeleteAfterOpen = true
            return this
        }

        /**
         * Enables background executor
         *
         * @return this builder
         */
        fun executorEnable():Maker{
            return this
        }

        //TODO cacheExecutor
        //TODO metrics executor
        //TODO store executor

        //TODO cache settings

        /**
         * <p>
         * Disable concurrency locks. This will make MapDB thread unsafe. It will also disable any background thread workers.
         * </p><p>
         *
         * <b>WARNING: </b> this option is dangerous. With locks disabled multi-threaded access could cause data corruption and causes.
         * MapDB does not have fail-fast iterator or any other means of protection
         * </p>
         *
         * @return this builder
         */
        fun concurrencyDisable():Maker{
            this._isThreadSafe = false
            return this
        }

        /**
         * <p>
         * Sets concurrency scale. More locks means better scalability with multiple cores, but also higher memory overhead
         * </p><p>
         *
         * This value has to be power of two, so it is rounded up automatically.
         * </p>
         *
         * @return this builder
         */
        fun concurrencyScale(segmentCount:Int):Maker{
            this._concurrencyScale = segmentCount
            return this;
        }


        // TODO single lock
//        /**
//         * <p>
//         * Disables double read-write locks and enables single read-write locks.
//         * </p><p>
//         *
//         * This type of locking have smaller overhead and can be faster in mostly-write scenario.
//         * </p>
//         * @return this builder
//         */
//        public Maker lockSingleEnable() {
//            props.put(Keys.lock, Keys.lock_single);
//            return this;
//        }

        protected fun assertFile(){
            if((_storeType in arrayOf(StoreType.fileRaf, StoreType.fileMMap, StoreType.fileChannel)).not())
                throw DBException.WrongConfiguration("File related options are not allowed for in-memory store")
        }

        /**
         * <p>
         * Enables Memory Mapped Files, much faster storage option. However on 32bit JVM this mode could corrupt
         * your DB thanks to 4GB memory addressing limit.
         * </p><p>
         *
         * You may experience {@code java.lang.OutOfMemoryError: Map failed} exception on 32bit JVM, if you enable this
         * mode.
         * </p>
         */
        fun fileMmapEnable():Maker{
            assertFile()
            _storeType = StoreType.fileMMap
            return this;
        }

        /**
         * <p>
         * Enables cleaner hack to close mmaped files and `DirectByteBuffers` at `DB.close()`, rather than at Garbage Collection.
         * See relevant <a href="http://bugs.java.com/view_bug.do?bug_id=4724038">JVM bug</a>.
         * Please note that this option closes files, but could cause all sort of problems,
         * including JVM crash.
         * </p><p>
         * Memory mapped files in Java are not unmapped when file closes.
         * Unmapping happens when {@code DirectByteBuffer} is garbage collected.
         * Delay between file close and GC could be very long, possibly even hours.
         * This causes file descriptor to remain open, causing all sort of problems:
         * </p><p>
         * On Windows opened file can not be deleted or accessed by different process.
         * It remains locked even after JVM process exits until Windows restart.
         * This is causing problems during compaction etc.
         * </p><p>
         * On Linux (and other systems) opened files consumes file descriptor. Eventually
         * JVM process could run out of available file descriptors (couple of thousands)
         * and would be unable to open new files or sockets.
         * </p><p>
         * On Oracle and OpenJDK JVMs there is option to unmap files after closing.
         * However it is not officially supported and could result in all sort of strange behaviour.
         * In MapDB it was linked to <a href="https://github.com/jankotek/mapdb/issues/442">JVM crashes</a>,
         * and was disabled by default in MapDB 2.0.
         * </p>
         * @return this builder
         */
        fun cleanerHackEnable():Maker{
            _cleanerHack = true
            return this;
        }


        /**
         * <p>
         * Disables preclear workaround for JVM crash. This will speedup inserts on mmap files, if store is expanded.
         * As sideffect JVM might crash if there is not enough free space.
         * TODO document more, links
         * </p>
         * @return this builder
         */
        fun fileMmapPreclearDisable():Maker{
            _fileMmapPreclearDisable = true
            return this;
        }

        /**
         * <p>
         * MapDB needs exclusive lock over storage file it is using.
         * When single file is used by multiple DB instances at the same time, storage file gets quickly corrupted.
         * To prevent multiple opening MapDB uses {@link FileChannel#lock()}.
         * If file is already locked, opening it fails with {@link DBException.FileLocked}
         * </p><p>
         * In some cases file might remain locked, if DB is not closed correctly or JVM crashes.
         * This option disables exclusive file locking. Use it if you have troubles to reopen files
         *
         * </p>
         * @return this builder
         */
        fun fileLockDisable():Maker{
            assertFile()
            _fileLockWait = -1
            return this;
        }

        fun fileLockWait(timeout:Long):Maker{
            assertFile()
            _fileLockWait = timeout
            return this
        }

        fun fileLockWait():Maker = fileLockWait(Long.MAX_VALUE)



        /**
         * Enables store wide checksum. Entire file is covered by 64bit checksum to catch possible data corruption.
         * This could be slow, since entire file is traversed to calculate checksum on store open, commit and close.
         */
        fun checksumStoreEnable():Maker{
            _checksumStoreEnable = true
            return this
        }


        /**
         * MapDB detects unclean shutdown (and possible data corruption) by Header Checksum.
         * This checksum becomes invalid if store was modified, but not closed correctly.
         * In that case MapDB will throw an exception and will refuse to open the store.
         * <p/>
         * This setting will bypass Header Checksum check when store is opened.
         * So if store is corrupted, it will still allow you to open it and recover your data.
         * Invalid Header Checksum will not throw an exception, but will log an error in console.
         */
        fun checksumHeaderBypass():Maker{
            _checksumHeaderBypass = true
            return this
        }

        /**
         * Enable Memory Mapped Files only if current JVM supports it (is 64bit).
         */
        fun fileMmapEnableIfSupported():Maker{
            assertFile()
            _fileMmapfIfSupported = true
            return this;
        }

        /**
         * Enable FileChannel access. By default MapDB uses {@link java.io.RandomAccessFile}.
         * which is slower and more robust. but does not allow concurrent access (parallel read and writes). RAF is still thread-safe
         * but has global lock.
         * FileChannel does not have global lock, and is faster compared to RAF. However memory-mapped files are
         * probably best choice.
         */
        fun fileChannelEnable():Maker{
            assertFile()
            _storeType = StoreType.fileChannel
            return this;
        }

        /**
         * Adds JVM shutdown hook and closes DB just before JVM;
         *
         * @return this builder
         */
        fun closeOnJvmShutdown():Maker{
            _closeOnJvmShutdown = 1
            return this;
        }


        /**
         * Adds JVM shutdown hook and closes DB just before JVM.
         * This is similar to `closeOnJvmShutdown()`, but DB is referenced with `WeakReference` from shutdown hook
         * and can be GCed. That might prevent memory leaks under some conditions, but does not guarantee DB will be actually closed.
         *
         * `DB.close()` removes DB object from shutdown hook, so DB object can be GCed after close, even with regular
         *
         *
         * @return this builder
         */
        fun closeOnJvmShutdownWeakReference():Maker{
            _closeOnJvmShutdown = 2
            return this;
        }

        /**
         * Open store in read-only mode. Any modification attempt will throw
         * <code>UnsupportedOperationException("Read-only")</code>
         *
         * @return this builder
         */
        fun readOnly():Maker{
            _readOnly = true
            return this
        }

        fun make():DB{
            var storeOpened = false

            val concShift = DataIO.shift(DataIO.nextPowTwo(_concurrencyScale))

            var storeType2 = _storeType
            if(_fileMmapfIfSupported && DataIO.JVMSupportsLargeMappedFiles()){
                storeType2 = StoreType.fileMMap
            }

            var volfab = when(storeType2){
                StoreType.onheap -> null
                StoreType.bytearray -> ByteArrayVol.FACTORY
                StoreType.directbuffer -> if(_cleanerHack) ByteBufferMemoryVol.FACTORY_WITH_CLEANER_HACK else ByteBufferMemoryVol.FACTORY
                StoreType.fileRaf -> RandomAccessFileVol.FACTORY
                StoreType.fileChannel -> FileChannelVol.FACTORY
                StoreType.fileMMap -> MappedFileVol.MappedFileFactory(_cleanerHack, _fileMmapPreclearDisable)
                null -> VolumeFactory.wrap(_customVolume!!, _volumeExist!!)
            }

            if(_readOnly && volfab!=null && volfab.handlesReadonly().not())
                volfab = ReadOnlyVolumeFactory(volfab)

            var store = if(_storeType == StoreType.onheap){
                    if(_readOnly)
                        StoreReadOnlyWrapper(StoreOnHeap())
                    else
                        StoreOnHeap()
                }else {
                    storeOpened = volfab!!.exists(file)
                    if (_transactionEnable.not() || _readOnly) {
                       StoreDirect.make(file = file, volumeFactory = volfab,
                               fileLockWait = _fileLockWait,
                               allocateIncrement = _allocateIncrement,
                               allocateStartSize = _allocateStartSize,
                               isReadOnly = _readOnly,
                               fileDeleteAfterClose = _fileDeleteAfterClose,
                               fileDeleteAfterOpen = _fileDeleteAfterOpen,
                               concShift = concShift,
                               checksum = _checksumStoreEnable,
                               isThreadSafe = _isThreadSafe ,
                               checksumHeaderBypass = _checksumHeaderBypass)
                    } else {
                        if(_checksumStoreEnable)
                            throw DBException.WrongConfiguration("Checksum is not supported with transaction enabled.")
                       StoreWAL.make(file = file, volumeFactory = volfab,
                               fileLockWait = _fileLockWait,
                               allocateIncrement = _allocateIncrement,
                               allocateStartSize = _allocateStartSize,
                               fileDeleteAfterClose = _fileDeleteAfterClose,
                               fileDeleteAfterOpen = _fileDeleteAfterOpen,
                               concShift = concShift,
                               checksum = _checksumStoreEnable,
                               isThreadSafe = _isThreadSafe ,
                               checksumHeaderBypass = _checksumHeaderBypass)
                    }
                }

            return DB(store=store, storeOpened = storeOpened, isThreadSafe = _isThreadSafe, shutdownHook = _closeOnJvmShutdown, classLoader = _classLoader )
        }
    }

}
