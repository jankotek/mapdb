package org.mapdb

import com.google.common.cache.CacheBuilder
import org.mapdb.list.LinkedList
import org.mapdb.queue.LinkedQueue
import org.mapdb.serializer.Serializer
import org.mapdb.serializer.Serializers
import org.mapdb.store.*
import org.mapdb.util.dataAssert
import org.mapdb.util.getBooleanOrDefault
import java.io.Closeable
import java.io.File
import java.nio.file.Path
import java.util.*

/** Main class for accessing MapDB */
class DB(val store: Store): Closeable {

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


        companion object {
            fun appendFile(f: File): DB.Maker {
                val maker = DB.Maker()
                maker.props[ConfigKey.file.name] = f.path
                maker.props[ConfigKey.storeType.name] = ConfigVal.fileAppend.name
                return maker
            }

            fun heap(): DB.Maker {
                val maker = DB.Maker()
                maker.props[ConfigKey.storeType.name] = ConfigVal.onHeap.name
                return maker
            }


            fun heapSer(): DB.Maker {
                val maker = DB.Maker()
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


    private val instances = CacheBuilder.newBuilder().weakValues().build<String, Any>()

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


    class QueueMaker<T>(private val db: DB, private val name: String, private val serializer: Serializer<*>) {

        enum class Order {
            FIFO, LIFO
        }


        fun make(): Queue<T> {
            val params = db.paramsLoad()
            var qp = params[name]

            if (qp == null) {
                //create new
                val qp2 = LinkedQueue.createWithParams(db.store as MutableStore, serializer)
                val params2 = TreeMap(params)
                qp2[DB.ParamNames.serializer] = db.serializerName(serializer)!!
                params2[name] = qp2
                qp = qp2
                db.paramsSave(params2)

            }
            if (qp[DB.ParamNames.serializer] != db.serializerName(serializer))
                throw DBException.WrongSerializer("Wrong serializer, expected ${qp[DB.ParamNames.serializer]} user supplied $serializer")


            return db.instances.get(name, {
                LinkedQueue.openWithParams(db.store, serializer as Serializer<T>, qp)
            }) as Queue<T>

        }
    }

}
