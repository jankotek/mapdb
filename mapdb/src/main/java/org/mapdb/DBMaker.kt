package org.mapdb

/**
 * Initializes DB object
 */
object DBMaker{

    enum class StoreType{
        onheap, direct
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

    //TODO convert user facing shifts into roundUp numbers
    @JvmStatic fun memorySegmentedHashMap(concShift:Int): DB.HashMapMaker<*,*> =
            DB(store = StoreDirect.make(),storeOpened = false)
                    .hashMap("map")
                    .storeFactory{i->
                        StoreDirect.make()
                    }
                    .layout(concShift = concShift, levels=4, dirShift = 7)

    @JvmStatic fun heapSegmentedHashMap(concShift:Int): DB.HashMapMaker<*,*> =
            DB(store = StoreOnHeap(),storeOpened = false)
                    .hashMap("map")
                    .storeFactory{i->
                        StoreOnHeap()
                    }
                    .layout(concShift = concShift, levels=4, dirShift = 7)


    class Maker(
            private val storeType:StoreType,
            private val volume:Volume?=null,
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
                            }else{
                                Volume.VolumeFactory.wrap(volume, volumeExist!!)
                            }
                    StoreDirect.make(volumeFactory=volumeFactory, allocateStartSize=_allocateStartSize)
                }
            }

            return DB(store=store, storeOpened = false)
        }
    }

}