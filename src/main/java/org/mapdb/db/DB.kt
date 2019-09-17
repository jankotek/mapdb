package org.mapdb.db

import com.google.common.cache.CacheBuilder
import org.checkerframework.checker.units.qual.K
import org.mapdb.DBException
import org.mapdb.list.LinkedList
import org.mapdb.ser.Serializer
import org.mapdb.ser.Serializers
import org.mapdb.store.*
import org.mapdb.util.dataAssert
import org.mapdb.util.getBooleanOrDefault
import java.io.Closeable
import java.io.File
import java.nio.file.Path
import java.util.*
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentNavigableMap
import java.util.concurrent.ConcurrentSkipListMap

/** Main class for accessing MapDB */
class DB(val store: MutableStore): Closeable {

    companion object {

        private val TRUE = "true"
        private val FALSE = "false"


        @JvmStatic
        fun newOnHeapDB(): Maker {
            val maker = Maker()
            maker.props[ConfigKey.storeType.name] = ConfigVal.onHeap.name
            return maker
        }

        @JvmStatic
        fun newOnHeapSerDB(): Maker {
            val maker = Maker()
            maker.props[ConfigKey.storeType.name] = ConfigVal.onHeapSer.name
            return maker
        }

    }

    private enum class ConfigKey {
        file,
        storeType,
        threadSafe
    }

    private enum class ConfigVal {
        onHeap, onHeapSer, fileAppend
    }

    class Maker {
        val props = TreeMap<String, String>()

        fun threadSafeDisable(): Maker {
            props[ConfigKey.threadSafe.name] = FALSE
            return this
        }

        fun threadSafe(threadSafe: Boolean): Maker {
            props[ConfigKey.threadSafe.name] = threadSafe.toString()
            return this
        }


        fun txBlock(): Maker {
            //TODO transactions
            return this
        }
        fun make(): DB {

            val threadSafe = props.getBooleanOrDefault(ConfigKey.threadSafe.name, true)

            fun file(): Path {
                val p = props[ConfigKey.file.name] ?: throw DBException.WrongConfig("File not provided")
                return File(p).toPath()
            }

            val store = when (props[ConfigKey.storeType.name]) {
                ConfigVal.onHeap.name -> StoreOnHeap(isThreadSafe = threadSafe)
                ConfigVal.onHeapSer.name -> StoreOnHeapSer(isThreadSafe = threadSafe)
                ConfigVal.fileAppend.name -> StoreAppend(file = file(), isThreadSafe = threadSafe)
                else -> throw DBException.WrongConfig("unknown store type")
            }

            if (store.isEmpty() && store is MutableStore) {
                //fill recids
                for (recid in 1..Recids.RECID_MAX_RESERVED) {
                    val recid2 = store.preallocate()
                    dataAssert(recid == recid2)
                }
            }

            return DB(store)
        }

        fun readOnly(): Maker {
            //FIXME readme
            return this
        }


        companion object {
            fun appendFile(f: File): Maker {
                val maker = Maker()
                maker.props[ConfigKey.file.name] = f.path
                maker.props[ConfigKey.storeType.name] = ConfigVal.fileAppend.name
                return maker
            }

            fun heap(): Maker {
                val maker = Maker()
                maker.props[ConfigKey.storeType.name] = ConfigVal.onHeap.name
                return maker
            }


            fun heapSer(): Maker {
                val maker = Maker()
                maker.props[ConfigKey.storeType.name] = ConfigVal.onHeapSer.name
                return maker
            }


        }
    }

    class LinkedListMaker<T>(
            private val db: DB,
            private val serializer: Serializer<T>) {
        val props = TreeMap<String, String>()

        fun make(): LinkedList<T> {
            return LinkedList(store = db.store as MutableStore, serializer = serializer)
        }

    }


    fun <E> linkedList(name: String, serializer: Serializer<E>) = LinkedListMaker(db = this, serializer = serializer)

    override fun close() {
        store.close();
    }


    //TODO make private
    internal val instances = CacheBuilder.newBuilder().weakValues().build<String, Any>()

    private val paramSerializer = Serializers.JAVA as Serializer<Map<String, Map<String, String>>>


    fun paramsLoad(): Map<String, Map<String, String>> =
            store.get(Recids.RECID_NAME_PARAMS, paramSerializer)
                    ?: TreeMap()

    fun paramsSave(params: Map<String, Map<String, String>>) {
        for ((k, v) in params) {
            assert(k.length > 0)
            assert(v.size > 0)
        }
        (store as MutableStore).update(Recids.RECID_NAME_PARAMS, paramSerializer, params)
    }

    fun <T> queue(name: String, serializer: Serializer<T>): QueueMaker<T> {
        return QueueMaker<T>(this, name, serializer)
    }

    object ParamNames {

        val recid = "recid"
        val serializer = "serializer"
        val format = "format"
    }

    val serializerNames: Map<String, Serializer<*>> = Serializers::class.java.fields.filter { it.name != "INSTANCE" }.map { f ->
        Pair("Serializers." + f.name, f.get(null) as Serializer<*>)
    }.toMap()
    val serializerInstances: Map<Serializer<*>, String> = serializerNames.map { p -> Pair(p.value, p.key) }.toMap()

    fun serializerName(serializer: Serializer<*>): String? {
        return serializerInstances[serializer]
    }

    fun serializerByName(name: String): Serializer<*>? {
        return serializerNames[name]
    }

    fun <E> serializerForClass(clazz: Class<E>): Serializer<E> {
        val s = when(clazz){
            String::class.java-> Serializers.STRING
            ByteArray::class.java -> Serializers.BYTE_ARRAY
            java.lang.Integer::class.java -> Serializers.INTEGER
            java.lang.Long::class.java -> Serializers.LONG
            else -> throw DBException.WrongConfig("No registered serializer for class: "+clazz.name)
            //TODO config for fallback (default serialzier), or register 'catch all' serializer for java.lang.Object
        }
        return s as Serializer<E>
    }



    fun tx(o: Runnable){
        //TODO tx
        o.run()
    }

    fun <E> tx(o: Callable<E>): E {
        //TODO tx
        //TODO make sure it is not embeddable
        //TODO in separate interface
        return o.call()
    }

    fun <K,V> treeMap(test: String, keySer:Serializer<K>,
                      valueSer:Serializer<V>): NavigableMapMaker<K,V> {
        return NavigableMapMaker<K,V>()
    }


    class NavigableMapMaker<K,V> {
        fun make():ConcurrentNavigableMap<K,V> = ConcurrentSkipListMap() //TODO mock treemap
    }

}
