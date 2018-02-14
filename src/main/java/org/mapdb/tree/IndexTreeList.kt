package org.mapdb.tree

import org.eclipse.collections.api.map.primitive.MutableLongLongMap
import org.mapdb.*
import org.mapdb.serializer.Serializer
import org.mapdb.util.*
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock


/**
 * [ArrayList] like structure backed by tree
 */
class IndexTreeList<E> (
        val store:Store,
        val serializer: Serializer<E>,
        val map: MutableLongLongMap,
        counterRecid:Long,
        val isThreadSafe:Boolean
        ) : AbstractList<E?>() {

    private val counter = Atomic.Long(store, counterRecid, true)

    val lock = if(isThreadSafe) ReentrantReadWriteLock() else null

    override fun add(element: E?): Boolean {
        lock.lockWrite{
            val index = counter.getAndIncrement()
            val recid = store.put(element, serializer)
            map.put(index, recid)
            return true
        }
    }

    override fun add(index: Int, element: E?) {
        lock.lockWrite{
            checkIndex(index)
            //make space
            for (i in size - 1 downTo index) {
                val recid = map.get(i.toLong())
                if (recid == 0L)
                    continue
                map.remove(i.toLong())
                map.put((i + 1).toLong(), recid)
            }
            counter.increment()

            val recid = map[index.toLong()]
            if (recid == 0L) {
                map.put(index.toLong(), store.put(element, serializer))
            } else {
                store.update(recid, element, serializer)
            }
        }
    }

    override fun clear() {
        lock.lockWrite{
            counter.set(0)
            //TODO iterate over map and clear in in single pass if IndexTreeLongLongMap
            map.forEachValue { recid -> store.delete(recid, serializer) }
            map.clear()
        }
    }

    override fun removeAt(index: Int): E? {
        lock.lockWrite{
            checkIndex(index)
            val recid = map[index.toLong()]
            val ret = if (recid == 0L) {
                null;
            } else {
                val ret = store.get(recid, serializer)
                store.delete(recid, serializer)
                map.remove(index.toLong())
                ret
            }
            //move down rest of the list
            for (i in index + 1 until size) {
                val recid2 = map.get(i.toLong())
                if (recid2 == 0L)
                    continue;
                map.remove(i.toLong())
                map.put((i - 1).toLong(), recid2)
            }
            counter.decrement()
            return ret;
        }
    }

    override fun set(index: Int, element: E?): E? {
        lock.lockWrite{
            checkIndex(index)
            val recid = map[index.toLong()]
            if (recid == 0L) {
                map.put(index.toLong(), store.put(element, serializer))
                return null
            } else {
                val ret = store.get(recid, serializer)
                store.update(recid, element, serializer)
                return ret
            }
        }
    }

    fun checkIndex(index:Int){
        if(index<0 || index>=size)
            throw IndexOutOfBoundsException()
    }

    override fun get(index: Int): E? {
        lock.lockRead{
            checkIndex(index)

            val recid = map[index.toLong()]
            if (recid == 0L) {
                return null;
            }
            return store.get(recid, serializer)
        }
    }

    override fun isEmpty(): Boolean {
        return size==0
    }

    //TODO PERF iterate over Map and fill gaps, should be faster. But careful if map is HashMap or not sorted other way
    override fun iterator(): MutableIterator<E?> {
        return object:MutableIterator<E?>{

            @Volatile var index = 0;
            @Volatile var indexToRemove:Int?=null;
            override fun hasNext(): Boolean {
                lock.lockRead {
                    return index < this@IndexTreeList.size
                }
            }

            override fun next(): E? {
                lock.lockRead{
                    if (index >= this@IndexTreeList.size)
                        throw NoSuchElementException()
                    indexToRemove = index
                    val ret = this@IndexTreeList[index]
                    index++;
                    return ret;
                }
            }

            override fun remove() {
                lock.lockWrite{
                    removeAt(indexToRemove ?: throw IllegalStateException())
                    index--
                    indexToRemove = null
                }
            }

        }
    }


    override val size: Int
        get() = counter.get().toInt()
}