package org.mapdb

import java.util.*
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock

/**
 * [ArrayList] like structure backed by tree
 */
//TODO this is just broken
class IndexTreeList<E>(
    val isThreadSafe:Boolean,
    val maxSize:Long,
    val serializer:Serializer<E>,
    val store:Store,
    rootRecid:Long?
) {


    companion object{
        /** constructor with default values */
        fun <E> make(
                isThreadSafe:Boolean=true,
                maxSize:Long = 0xFFFFFF,
                serializer:Serializer<E> = Serializer.JAVA as Serializer<E>,
                store:Store = StoreTrivial(),
                rootRecid:Long? = null

        ):IndexTreeList<E>{
            return IndexTreeList<E>(
                isThreadSafe = isThreadSafe,
                maxSize = maxSize,
                serializer = serializer,
                store = store,
                rootRecid = rootRecid
            )
        }
    }


    internal val lock: ReadWriteLock? = Utils.newReadWriteLock(isThreadSafe)

    val rootSerializer = Serializer.LONG_ARRAY

    val rootRecid = rootRecid?:
        store.put(LongArray(maxSize.toInt()*2), rootSerializer)


    operator fun get(index:Int) = get(index.toLong())

    fun get(index:Long):E?{
        Utils.lockRead(lock) {
            val dir = store.get(rootRecid, rootSerializer)!!
            if(dir.size<index)
                return null;
            if(dir[index.toInt()*2]!=index)
                return null
            val recid = dir[index.toInt()*2+1]
            if(recid==0L)
                return null;
            return store.get(dir[index.toInt()], serializer);
        }
    }

    operator fun set(index:Int, value:E) = set(index.toLong(),value)

    fun set(index:Long, value:E):E?{
        Utils.lockWrite(lock) {
            var dir = store.get(rootRecid, rootSerializer)!!

            if(dir[index.toInt()]==0L){
                val dir2 = dir.clone()
                dir2[index.toInt()] = store.put(value, serializer)
                store.update(rootRecid, dir2, rootSerializer)
                return null;
            }else{
                val ret = store.get(dir[index.toInt()], serializer);
                store.update(dir[index.toInt()], value, serializer)
                return ret
            }
        }
    }

}