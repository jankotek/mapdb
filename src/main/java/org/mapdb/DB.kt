package org.mapdb

import org.mapdb.store.Store
import org.mapdb.store.StoreOnHeap
import org.mapdb.util.getBooleanOrDefault
import java.util.*

/** Main class for accessing MapDB */
class DB(val store: Store) {

companion object {

    enum class ConfigKey{
        file,
        storeType,
        threadSafe
    }

    enum class ConfigVal{
        onHeap,
    }

    private val TRUE = "true"
    private val FALSE = "false"


    @JvmStatic fun newOnHeapDB():Maker{
        val maker = Maker()
        maker.props[ConfigKey.storeType.name] = ConfigVal.onHeap.name
        return maker
    }

    class Maker{
        val props = TreeMap<String,String>()

        fun threadSafeDisable():Maker{
            props[ConfigKey.threadSafe.name] = TRUE
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
                else -> throw DBException.WrongConfig("unknown store type")
            }
            return DB(store)
        }
    }
}

}