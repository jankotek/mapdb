package org.mapdb

import org.mapdb.list.LinkedList
import org.mapdb.serializer.Serializer
import org.mapdb.store.MutableStore
import org.mapdb.store.Store
import org.mapdb.store.StoreOnHeap
import org.mapdb.store.StoreOnHeapSer
import org.mapdb.util.getBooleanOrDefault
import java.io.Closeable
import java.util.*

/** Main class for accessing MapDB */
class DB(val store: Store): Closeable {

    fun <E> linkedList(name: String, serializer: Serializer<E>) =  LinkedListMaker(db=this, serializer=serializer)

    override fun close() {
        store.close();
    }


    companion object {

        private val TRUE = "true"
        private val FALSE = "false"


        @JvmStatic fun newOnHeapDB():Maker{
            val maker = Maker()
            maker.props[ConfigKey.storeType.name] = ConfigVal.onHeap.name
            return maker
        }

        @JvmStatic fun newOnHeapSerDB():Maker{
            val maker = Maker()
            maker.props[ConfigKey.storeType.name] = ConfigVal.onHeapSer.name
            return maker
        }

    }

    private enum class ConfigKey{
        file,
        storeType,
        threadSafe
    }

    private enum class ConfigVal{
        onHeap, onHeapSer
    }

    class Maker{
        val props = TreeMap<String,String>()

        fun threadSafeDisable():Maker{
            props[ConfigKey.threadSafe.name] = FALSE
            return this
        }

        fun threadSafe(threadSafe:Boolean):Maker{
            props[ConfigKey.threadSafe.name] = threadSafe.toString()
            return this
        }

        fun make(): DB {

            val threadSafe = props.getBooleanOrDefault(ConfigKey.threadSafe.name, true)

            val store = when(props[ConfigKey.storeType.name]){
                ConfigVal.onHeap.name -> StoreOnHeap(isThreadSafe=threadSafe)
                ConfigVal.onHeapSer.name -> StoreOnHeapSer(isThreadSafe=threadSafe)
                else -> throw DBException.WrongConfig("unknown store type")
            }
            return DB(store)
        }
    }

    class LinkedListMaker<T>(
            private val db:DB,
            private val serializer: Serializer<T>) {
        val props = TreeMap<String,String>()

        fun make(): LinkedList<T> {
            return LinkedList(store= db.store as MutableStore, serializer = serializer)
        }

    }

}


