package org.mapdb.btree

import org.mapdb.DBException
import org.mapdb.ser.ArrayListSerializer
import org.mapdb.ser.PairSer
import org.mapdb.ser.Serializer
import org.mapdb.store.MutableStore
import org.mapdb.util.JavaUtils.COMPARABLE_COMPARATOR
import java.util.*
import kotlin.NoSuchElementException
import kotlin.collections.ArrayList

class BTreeMap<K,V>(
        private val store: MutableStore,
        private val rootRecid:Long,
        private val keySer:Serializer<K>,
        private val valSer:Serializer<V>
): AbstractMutableMap<K, V>() {


    private val ser = ArrayListSerializer(PairSer(keySer,valSer))

    private fun loadRoot():ArrayList<Pair<K,V>> = store.get(rootRecid, ser) ?: throw DBException.RecordNotFound()

    override val size: Int
        get() = loadRoot().size

    override fun containsKey(key: K): Boolean {
        return loadRoot().find { keySer.equals(it.first, key) } != null
    }

    override fun containsValue(value: V): Boolean {
        return loadRoot().find { valSer.equals(it.second, value) } != null
    }

    override fun get(key: K): V? {
        return loadRoot().find { keySer.equals(it.first, key) }?.second
    }

    override fun clear() {
        store.update(rootRecid, ser, ArrayList(0))
    }

    private val pairComparator = object: Comparator<Pair<K, V?>>{
        override fun compare(o1: Pair<K, V?>?, o2: Pair<K, V?>?): Int {
            val key1 = o1!!.first as Comparable<*>
            val key2 = o2!!.first as Comparable<*>
            return Objects.compare(key1,key2, COMPARABLE_COMPARATOR)
        }

    }

    override fun put(key: K, value: V): V? {
        val list = ArrayList(loadRoot())
        val pair = Pair(key, value)
        val index = Collections.binarySearch(list, pair, pairComparator)

        var oldVal:V? = null
        if(index>=0){
            oldVal = list[index].second
            list[index] = pair
        }else{
            list.add(-index-1, pair)
        }

        store.update(rootRecid, ser, list)
        return oldVal
    }

    private fun indexOfKey(list:ArrayList<Pair<K,V>>, key:K):Int{
        val pair = Pair(key, null)
        return Collections.binarySearch(list, pair, pairComparator)
    }

    override fun remove(key: K): V? {
        val list = ArrayList(loadRoot())
        val index = indexOfKey(list, key)
        if(index<0)
            return null

        val oldVal = list.removeAt(index).second
        store.update(rootRecid, ser, list)
        return oldVal
    }


    override val entries: MutableSet<MutableMap.MutableEntry<K, V>> = object: java.util.AbstractSet<MutableMap.MutableEntry<K, V>>() {
        override fun iterator(): MutableIterator<MutableMap.MutableEntry<K, V>> {
            return object: MutableIterator<MutableMap.MutableEntry<K, V>>{

                var first = true;
                var curPair: Pair<K,V>? = null
                var lastKey:K? = null

                var deleted = true

                fun advanceIfNeeded(){
                     if(first){
                        first = false
                        val list = loadRoot()
                        curPair = if(list.isEmpty()) null else list.first()
                        return
                    }
                    val lastKey2 = lastKey

                    if(curPair !=null || lastKey2 == null)
                        return;
                    //find next key
                    val list = loadRoot()
                    var index = indexOfKey(list, lastKey2)
                    if(index<0)
                        //key was deleted
                        index = -index-1
                    else
                        index++

                    if(index>=list.size){
                        //finished
                        lastKey = null
                        curPair = null
                    }else {
                        lastKey = list[index].first
                        curPair = list[index]
                    }
                }

                override fun hasNext(): Boolean {
                    advanceIfNeeded()
                    return curPair!=null
                }

                override fun next(): MutableMap.MutableEntry<K, V> {
                    advanceIfNeeded()
                    val pair = curPair ?: throw NoSuchElementException()
                    curPair = null
                    deleted = false
                    lastKey = pair.first

                    return MapEntry(pair.first, this@BTreeMap)
                }

                override fun remove() {
                    val p = lastKey
                    if(deleted || p == null)
                        throw IllegalStateException()
                    deleted = true
                    this@BTreeMap.remove(p)
                }

            }
        }

        override val size: Int
            get() = this@BTreeMap.size

    }




    companion object{
        fun <K,V> empty(store: MutableStore, keySer: Serializer<K>, valSer:Serializer<V>):BTreeMap<K,V>{
            val root = ArrayList<K>(0)
            val rootRecid = store.put(root, ArrayListSerializer(keySer));
            return BTreeMap(store=store, rootRecid = rootRecid, keySer=keySer, valSer=valSer)
        }

    }
}

class MapEntry<K, V>(override val key: K, private val map: MutableMap<K, V>) : MutableMap.MutableEntry<K, V> {

    override val value: V
        get() = map.get(key)!!

    override fun setValue(newValue: V): V {
        return map.put(key, newValue!!)!!
    }

    override fun hashCode(): Int {
        //TODO MapEntry hash code from serializer
        return key.hashCode() xor value.hashCode()
    }

    override fun equals(other: Any?): Boolean {
        if(!(other is MapEntry<*, *>))
            return false
        //TODO MapEntry equals with serializer
        return Objects.equals(key, other.key) && Objects.equals(value, other.value)
    }
}
