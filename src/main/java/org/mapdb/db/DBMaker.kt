package org.mapdb.db

object DBMaker {
    //TODO conflict with maker classes in DB

    @JvmStatic
    fun memoryDB(): MemoryMaker {
        return MemoryMaker()
    }


    class MemoryMaker(){
        fun make(): DB {
            return DB.newOnHeapSerDB().make()
        }

    }
}
