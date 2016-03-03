package org.mapdb

import org.mapdb.volume.MappedFileVol
import org.mapdb.volume.Volume
import org.mapdb.volume.VolumeFactory

/**
 * Initializes DB object
 */
object DBMaker{

    enum class StoreType{
        onheap, direct, ondisk
    }

    @JvmStatic fun fileDB(file:String): Maker {
        return Maker(StoreType.ondisk, file = file)
    }


    @JvmStatic fun heapDB(): Maker {
        return Maker(StoreType.onheap)
    }

    @JvmStatic fun memoryDB(): Maker {
        return Maker(StoreType.direct)
    }


    @JvmStatic fun onVolume(volume: Volume, volumeExists: Boolean): Maker {
        return Maker(storeType = StoreType.direct, volume=volume, volumeExist=volumeExists)
    }


    @JvmStatic fun memoryShardedHashSet(concurrency:Int): DB.HashSetMaker<*> =
            DB(store = StoreDirect.make(),storeOpened = false)
                    .hashSet("map")
                    .storeFactory{i->
                        StoreDirect.make(isThreadSafe = false)
                    }
                    .layout(concurrency=concurrency, dirSize = 1.shl(CC.HTREEMAP_DIR_SHIFT), levels = CC.HTREEMAP_LEVELS)

    @JvmStatic fun heapShardedHashSet(concurrency:Int): DB.HashSetMaker<*> =
            DB(store = StoreOnHeap(),storeOpened = false)
                    .hashSet("map")
                    .storeFactory{i->
                        StoreOnHeap(isThreadSafe = false)
                    }
                    .layout(concurrency=concurrency, dirSize = 1.shl(CC.HTREEMAP_DIR_SHIFT), levels = CC.HTREEMAP_LEVELS)


    @JvmStatic fun memoryShardedHashMap(concurrency:Int): DB.HashMapMaker<*,*> =
            DB(store = StoreDirect.make(),storeOpened = false)
                    .hashMap("map")
                    .storeFactory{i->
                        StoreDirect.make(isThreadSafe = false)
                    }
                    .layout(concurrency=concurrency, dirSize = 1.shl(CC.HTREEMAP_DIR_SHIFT), levels = CC.HTREEMAP_LEVELS)

    @JvmStatic fun heapShardedHashMap(concurrency:Int): DB.HashMapMaker<*,*> =
            DB(store = StoreOnHeap(),storeOpened = false)
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

        fun allocateStartSize(size:Long):Maker{
            _allocateStartSize = size
            return this
        }

        fun make():DB{
            val store = when(storeType){
                StoreType.onheap -> StoreOnHeap()
                StoreType.direct -> {
                    val volumeFactory =
                            if(volume==null){
                                if(file==null) CC.DEFAULT_MEMORY_VOLUME_FACTORY else CC.DEFAULT_FILE_VOLUME_FACTORY
                            }else {
                                VolumeFactory.wrap(volume, volumeExist!!)
                            }
                    StoreDirect.make(volumeFactory=volumeFactory, allocateStartSize=_allocateStartSize)
                }
                StoreType.ondisk -> {
                    val volumeFactory = MappedFileVol.FACTORY
                    StoreDirect.make(file=file, volumeFactory=volumeFactory, allocateStartSize=_allocateStartSize)
                }
            }

            return DB(store=store, storeOpened = false)
        }
    }

}