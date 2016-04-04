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
        onheap, directbuffer, bytearray, ondisk
    }

    /**
     * Creates new database in temporary folder. Files are deleted after store was closed
     */
    @JvmStatic fun tempFileDB(): Maker {
        //TODO on unix this file should be deleted just after it was open, verify compaction, rename etc
        val file = File.createTempFile("mapdb","temp")
        file.delete()
        file.deleteOnExit()
        return fileDB(file).deleteFilesAfterClose()
    }

    @JvmStatic fun fileDB(file:String): Maker {
        return Maker(StoreType.ondisk, file = file)
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


    @JvmStatic fun onVolume(volume: Volume, volumeExists: Boolean): Maker {
        return Maker(storeType = StoreType.directbuffer, volume=volume, volumeExist=volumeExists)
    }


    @JvmStatic fun memoryShardedHashSet(concurrency:Int): DB.HashSetMaker<*> =
            DB(store = StoreDirect.make(),storeOpened = false, isThreadSafe = true)
                    .hashSet("map")
                    .storeFactory{i->
                        StoreDirect.make(isThreadSafe = false)
                    }
                    .layout(concurrency=concurrency, dirSize = 1.shl(CC.HTREEMAP_DIR_SHIFT), levels = CC.HTREEMAP_LEVELS)

    @JvmStatic fun heapShardedHashSet(concurrency:Int): DB.HashSetMaker<*> =
            DB(store = StoreOnHeap(),storeOpened = false, isThreadSafe = true)
                    .hashSet("map")
                    .storeFactory{i->
                        StoreOnHeap(isThreadSafe = false)
                    }
                    .layout(concurrency=concurrency, dirSize = 1.shl(CC.HTREEMAP_DIR_SHIFT), levels = CC.HTREEMAP_LEVELS)


    @JvmStatic fun memoryShardedHashMap(concurrency:Int): DB.HashMapMaker<*,*> =
            DB(store = StoreDirect.make(),storeOpened = false, isThreadSafe = true)
                    .hashMap("map")
                    .storeFactory{i->
                        StoreDirect.make(isThreadSafe = false)
                    }
                    .layout(concurrency=concurrency, dirSize = 1.shl(CC.HTREEMAP_DIR_SHIFT), levels = CC.HTREEMAP_LEVELS)

    @JvmStatic fun heapShardedHashMap(concurrency:Int): DB.HashMapMaker<*,*> =
            DB(store = StoreOnHeap(),storeOpened = false, isThreadSafe = true)
                    .hashMap("map")
                    .storeFactory{i->
                        StoreOnHeap(isThreadSafe = false)
                    }
                    .layout(concurrency=concurrency, dirSize = 1.shl(CC.HTREEMAP_DIR_SHIFT), levels = CC.HTREEMAP_LEVELS)


    class Maker(
            private val storeType:StoreType,
            private val volume: Volume?=null,
            private val volumeExist:Boolean?=null,
            private val file:String?=null){

        private var _allocateStartSize:Long = 0L
        private var _transactionEnable = false
        private var _deleteFilesAfterClose = false
        private var _isThreadSafe = true
        private var _concurrencyScale: Int = 1.shl(CC.STORE_DIRECT_CONC_SHIFT)

        fun transactionEnable():Maker{
            _transactionEnable = true
            return this
        }

        fun allocateStartSize(size:Long):Maker{
            _allocateStartSize = size
            return this
        }

        fun deleteFilesAfterClose():Maker{
            _deleteFilesAfterClose = true
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


        fun make():DB{
            var storeOpened = false

            val concShift = DataIO.shift(DataIO.nextPowTwo(_concurrencyScale))


            val volfab = when(storeType){
                StoreType.onheap -> null
                StoreType.bytearray -> ByteArrayVol.FACTORY
                StoreType.directbuffer -> ByteBufferMemoryVol.FACTORY //TODO cleaner hack
                StoreType.ondisk -> RandomAccessFileVol.FACTORY //TODO mmap, filechannel etc
            }

            val store = if(storeType== StoreType.onheap){
                    StoreOnHeap()
                }else {
                    storeOpened = volfab!!.exists(file)
                    if (_transactionEnable.not()) {
                       StoreDirect.make(file = file, volumeFactory = volfab!!,
                               allocateStartSize = _allocateStartSize,
                               deleteFilesAfterClose = _deleteFilesAfterClose,
                               concShift = concShift,
                               isThreadSafe = _isThreadSafe )
                    } else {
                       StoreWAL.make(file = file, volumeFactory = volfab!!,
                               allocateStartSize = _allocateStartSize,
                               deleteFilesAfterClose = _deleteFilesAfterClose,
                               concShift = concShift,
                               isThreadSafe = _isThreadSafe )
                    }
                }
            //
//            val store = when(storeType){
//                StoreType.onheap -> StoreOnHeap()
//                StoreType.directbuffer -> {
//                    val volumeFactory =
//                            if(volume==null){
//                                if(file==null) CC.DEFAULT_MEMORY_VOLUME_FACTORY else CC.DEFAULT_FILE_VOLUME_FACTORY
//                            }else {
//                                VolumeFactory.wrap(volume, volumeExist!!)
//                            }
//                    if(_transactionEnable.not())
//                        StoreDirect.make(volumeFactory=volumeFactory, allocateStartSize=_allocateStartSize, deleteFilesAfterClose = _deleteFilesAfterClose)
//                    else
//                        StoreWAL.make(volumeFactory=volumeFactory, allocateStartSize=_allocateStartSize, deleteFilesAfterClose = _deleteFilesAfterClose)
//                }
//                StoreType.ondisk -> {
//                    val volumeFactory = MappedFileVol.FACTORY
//                    storeOpened = volumeFactory.exists(file)
//                    if(_transactionEnable.not())
//                        StoreDirect.make(file=file, volumeFactory=volumeFactory, allocateStartSize=_allocateStartSize, deleteFilesAfterClose = _deleteFilesAfterClose)
//                    else
//                        StoreWAL.make(file=file, volumeFactory=volumeFactory, allocateStartSize=_allocateStartSize, deleteFilesAfterClose = _deleteFilesAfterClose)
//                }
//            }

            return DB(store=store, storeOpened = storeOpened, isThreadSafe = _isThreadSafe)
        }
    }

}