package org.mapdb

import org.eclipse.collections.api.LazyLongIterable
import org.eclipse.collections.api.LongIterable
import org.eclipse.collections.api.RichIterable
import org.eclipse.collections.api.bag.MutableBag
import org.eclipse.collections.api.bag.primitive.MutableLongBag
import org.eclipse.collections.api.block.function.primitive.*
import org.eclipse.collections.api.block.predicate.primitive.LongLongPredicate
import org.eclipse.collections.api.block.predicate.primitive.LongPredicate
import org.eclipse.collections.api.block.procedure.Procedure
import org.eclipse.collections.api.block.procedure.primitive.LongLongProcedure
import org.eclipse.collections.api.block.procedure.primitive.LongProcedure
import org.eclipse.collections.api.collection.primitive.ImmutableLongCollection
import org.eclipse.collections.api.collection.primitive.MutableLongCollection
import org.eclipse.collections.api.iterator.MutableLongIterator
import org.eclipse.collections.api.map.primitive.ImmutableLongLongMap
import org.eclipse.collections.api.map.primitive.LongLongMap
import org.eclipse.collections.api.map.primitive.MutableLongLongMap
import org.eclipse.collections.api.set.MutableSet
import org.eclipse.collections.api.set.primitive.ImmutableLongSet
import org.eclipse.collections.api.set.primitive.LongSet
import org.eclipse.collections.api.set.primitive.MutableLongSet
import org.eclipse.collections.api.tuple.primitive.LongLongPair
import org.eclipse.collections.impl.bag.mutable.HashBag
import org.eclipse.collections.impl.bag.mutable.primitive.LongHashBag
import org.eclipse.collections.impl.factory.Sets
import org.eclipse.collections.impl.factory.primitive.LongSets
import org.eclipse.collections.impl.lazy.AbstractLazyIterable
import org.eclipse.collections.impl.lazy.primitive.LazyLongIterableAdapter
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap
import org.eclipse.collections.impl.primitive.AbstractLongIterable
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet
import org.eclipse.collections.impl.set.mutable.primitive.SynchronizedLongSet
import org.eclipse.collections.impl.set.mutable.primitive.UnmodifiableLongSet
import org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples
import org.eclipse.collections.impl.utility.internal.primitive.LongIterableIterate
import org.mapdb.IndexTreeListJava.*
import java.io.IOException
import java.util.*

/**
 * Primitive Sorted Map<Long,Long.
 *
 * Is not thread safe!!!
 */
public class IndexTreeLongLongMap(
    val store:Store,
    val rootRecid:Long,
    val dirShift:Int,
    val levels:Int,
    val collapseOnRemove:Boolean
): AbstractLongIterable(), MutableLongLongMap {


    companion object{
        /** constructor with default values */
        fun make(
                store:Store = StoreTrivial(),
                rootRecid:Long = store.put(dirEmpty(), dirSer),
                dirShift: Int = CC.INDEX_TREE_LONGLONGMAP_DIR_SHIFT,
                levels:Int = CC.INDEX_TREE_LONGLONGMAP_LEVELS,
                collapseOnRemove: Boolean = true
        ) = IndexTreeLongLongMap(
                store = store,
                rootRecid = rootRecid,
                dirShift = dirShift,
                levels = levels,
                collapseOnRemove = collapseOnRemove
        )
    }

    override fun put(key: Long, value: Long) {
        assertKey(key)
        treePut(dirShift, rootRecid, store, levels, key, value);
    }

    override fun get(key: Long): Long {
        assertKey(key)
        return treeGet(dirShift, rootRecid, store, levels, key)
    }

    override fun remove(key: Long) {
        assertKey(key)
        if(collapseOnRemove)
            treeRemoveCollapsing(dirShift, rootRecid, store, levels,true, key, null)
        else
            treeRemove(dirShift, rootRecid, store, levels, key, null)
    }

    private inline fun assertKey(key: Long) {
        if (key < 0)
            throw IllegalArgumentException("negative key")
        //TODO assert max size based on levels
    }


    override fun contains(value: Long): Boolean {
        return this.containsValue(value)
    }


    override fun containsKey(key: Long): Boolean {
        if(key<0L)
            return false
        return treeGetNullable(dirShift, rootRecid, store, levels, key) != null
    }

    override fun containsValue(value: Long): Boolean {
        //TODO perf
        return treeFold(rootRecid, store, levels, false, TreeTraverseCallback { k, v, b: Boolean ->
            b || v == value
        })
    }


    override fun clear() {
        treeClear(rootRecid, store, levels)
    }

    override fun <V : Any?> collect(function: LongToObjectFunction<out V>): MutableBag<V>? {
        val ret = HashBag<V>()
        forEachKeyValue { k, v ->
            val v = function.valueOf(v);
            ret.add(v)
        }
        return ret
    }

    private class Iterator(
            val m:IndexTreeLongLongMap,
            val index:Int
        ):MutableLongIterator{

        var nextKey: Long? = -1L
        var nextRet: Long? = null
        var lastKey: Long? = null;

        override fun hasNext(): Boolean {
            if(nextRet!=null)
                return true
            val prev = nextKey ?: return false;
            val ret = treeIter(m.dirShift, m.rootRecid, m.store, m.levels, prev+1)
            if(ret==null) {
                nextRet = null
                nextKey = null
            }else{
                nextRet = ret[index]
                nextKey = ret[0]
            }
            return nextRet!=null
        }

        override fun next(): Long {
            val ret = nextRet
            nextRet = null;
            if (ret == null) {
                if(nextKey!=null){
                    //fetch next item
                    if(hasNext())
                        return next()
                }
                lastKey = null
                throw NoSuchElementException()
            }
            lastKey = nextKey
            return ret;
        }

        override fun remove() {
            m.removeKey(lastKey ?: throw IllegalStateException())
            lastKey = null
        }
    }

    override fun longIterator(): MutableLongIterator {
        return Iterator(this@IndexTreeLongLongMap, 1)
    }

    override fun reject(predicate: LongPredicate): MutableLongBag? {
        val ret = LongHashBag()
        forEachKeyValue { k, v ->
            if (!predicate.accept(v))
                ret.add(v)
        }
        return ret;
    }

    override fun select(predicate: LongPredicate): MutableLongBag? {
        val ret = LongHashBag()
        forEachKeyValue { k, v ->
            if (predicate.accept(v))
                ret.add(v)
        }
        return ret;
    }

    override fun appendString(appendable: Appendable, start: String, separator: String, end: String) {
        try {
            appendable.append(start)

            var first = true;
            forEachKeyValue { k, l ->
                if (!first) {
                    appendable.append(separator)
                }
                first = false;
                //                appendable.append(k.toString())
                //                appendable.append('=')
                appendable.append(l.toString())
            }

            appendable.append(end)
        } catch (e: IOException) {
            throw RuntimeException(e)
        }

    }

    override fun size(): Int {
        return Utils.roundDownToIntMAXVAL(
                treeFold(rootRecid, store, levels, 0L) { k, v, b: Long ->
                    b + 1
                })
    }


    override fun allSatisfy(predicate: LongPredicate): Boolean {
        return treeFold(rootRecid, store, levels, true) { k, v, b: Boolean ->
            b && predicate.accept(v)
        }
    }

    override fun anySatisfy(predicate: LongPredicate): Boolean {
        //TODO PERF this traverses entire collection, terminate iteration when firt found
        return treeFold(rootRecid, store, levels, false) { k, v, b: Boolean ->
            b || predicate.accept(v)
        }
    }

    override fun count(predicate: LongPredicate): Int {
        return Utils.roundDownToIntMAXVAL(
                treeFold(rootRecid, store, levels, 0L) { k, v, b: Long ->
                    if (predicate.accept(v))
                        b + 1
                    else
                        b
                })
    }

    override fun detectIfNone(predicate: LongPredicate, ifNone: Long): Long {
        var ret = ifNone
        forEachValue { v ->
            if (predicate.accept(v))
                ret = v
        }
        return ret
    }

    override fun each(procedure: LongProcedure) {
        forEach(procedure)
    }

    override fun forEach(procedure: LongProcedure) {
        forEachValue(procedure)
    }

    override fun <T : Any?> injectInto(injectedValue: T, function: ObjectLongToObjectFunction<in T, out T>?): T {
        throw UnsupportedOperationException()
    }

    override fun max(): Long {
        val ret = treeFold<Long?>(rootRecid, store, levels, null) { k, v, b: Long? ->
            if (b == null)
                v
            else
                Math.max(b, v)

        }
        return ret ?: throw NoSuchElementException()
    }

    override fun min(): Long {
        val ret = treeFold<Long?>(rootRecid, store, levels, null) { k, v, b: Long? ->
            if (b == null)
                v
            else
                Math.min(b, v)

        }
        return ret ?: throw NoSuchElementException()
    }

    override fun noneSatisfy(predicate: LongPredicate): Boolean {
        return treeFold(rootRecid, store, levels, true) { k, v, b: Boolean ->
            if (!b)
                false
            else {
                //TODO PERF cancel iteration if first one satisfy
                !predicate.accept(v)
            }

        }
    }

    override fun sum(): Long {
        return treeFold(rootRecid, store, levels, 0) { k, v, b: Long ->
            b + v
        }
    }

    override fun toArray(): LongArray {
        return values().toArray()
    }

    override fun addToValue(key: Long, toBeAdded: Long): Long {
        val old = get(key);
        val newVal = old + toBeAdded
        put(key, newVal)
        return newVal;
    }

    override fun asSynchronized(): MutableLongLongMap? {

        throw UnsupportedOperationException()
    }

    override fun asUnmodifiable(): MutableLongLongMap? {
        throw UnsupportedOperationException()
    }

    override fun getIfAbsentPut(key: Long, function: LongFunction0): Long {
        val oldval = treeGetNullable(dirShift, rootRecid, store, levels, key)
        if (oldval == null) {
            val value = function.value()
            put(key, value)
            return value;
        } else {
            return oldval
        }
    }

    override fun getIfAbsentPut(key: Long, value: Long): Long {
        val oldval = treeGetNullable(dirShift, rootRecid, store, levels, key)
        if (oldval == null) {
            put(key, value)
            return value;
        } else {
            return oldval
        }
    }

    override fun <P : Any?> getIfAbsentPutWith(key: Long,
                                               function: LongFunction<in P>, parameter: P): Long {

        val oldval = treeGetNullable(dirShift, rootRecid, store, levels, key)
        if (oldval != null)
            return oldval

        val value = function.longValueOf(parameter)
        put(key, value)
        return value;
    }

    override fun getIfAbsentPutWithKey(key: Long, function: LongToLongFunction): Long {
        val oldval = treeGetNullable(dirShift, rootRecid, store, levels, key)
        if (oldval != null)
            return oldval

        val value = function.valueOf(key)
        put(key, value)
        return value;
    }

    override fun putAll(map: LongLongMap) {
        map.forEachKeyValue { k, v ->
            put(k, v)
        }
    }

    override fun reject(predicate: LongLongPredicate): MutableLongLongMap {
        val ret = LongLongHashMap()
        forEachKeyValue { k, v ->
            if (!predicate.accept(k, v))
                ret.put(k, v)
        }
        return ret
    }


    override fun removeKey(key: Long) {
        remove(key)
    }

    override fun removeKeyIfAbsent(key: Long, value: Long): Long {
        val oldval = treeGetNullable(dirShift, rootRecid, store, levels, key)
                ?: return value

        if (oldval.toLong() != value) {
            removeKey(key)
            return oldval
        }
        return value
    }

    override fun select(predicate: LongLongPredicate): MutableLongLongMap? {
        val ret = LongLongHashMap()
        forEachKeyValue { k, v ->
            if (predicate.accept(k, v))
                ret.put(k, v)
        }
        return ret
    }

    override fun updateValue(key: Long, initialValueIfAbsent: Long, function: LongToLongFunction): Long {
        //TODO PERF optimize
        val oldval = treeGetNullable(dirShift, rootRecid, store, levels, key) ?: initialValueIfAbsent

        val newVal = function.valueOf(oldval)
        put(key, newVal);
        return newVal

    }

    override fun withKeyValue(key: Long, value: Long): MutableLongLongMap? {
        put(key, value)
        return this;
    }

    override fun withoutAllKeys(keys: LongIterable): MutableLongLongMap? {
        keys.forEach { key ->
            removeKey(key)
        }
        return this;
    }

    override fun withoutKey(key: Long): MutableLongLongMap? {
        remove(key)
        return this;
    }

    override fun forEachKey(procedure: LongProcedure) {
        treeFold(rootRecid, store, levels, Unit, { k, v, Unit ->
            procedure.value(k)
        })
    }

    override fun forEachKeyValue(procedure: LongLongProcedure) {
        treeFold(rootRecid, store, levels, Unit, { k, v, Unit ->
            procedure.value(k, v)
        })
    }

    override fun getIfAbsent(key: Long, ifAbsent: Long): Long {
        return treeGetNullable(dirShift, rootRecid, store, levels, key)
                ?: ifAbsent;
    }

    override fun getOrThrow(key: Long): Long {
        val ret = treeGetNullable(dirShift, rootRecid, store, levels, key);
        return ret ?: throw IllegalStateException("Key $key not present.")
    }


    override fun forEachValue(procedure: LongProcedure) {
        treeFold(rootRecid, store, levels, Unit, { k, v, Unit ->
            procedure.value(v)
        })
    }


    override fun equals(other: Any?): Boolean {
        if (other == null || other !is LongLongMap)
            return false

        var c = 0;
        var ret = true;
        forEachKeyValue { k, v ->
            c++
            if (!other.containsKey(k) || other.get(k) != v)
                ret = false;
        }

        return ret && c == other.size();
    }

    override fun hashCode(): Int {
        var result = 0;
        forEachKeyValue { k, v ->
            result += DataIO.longHash(k + v + 10)
        }
        return result
    }


    override fun toString(): String {
        val s = StringBuilder()
        s.append('{')
        var first = true
        forEachKeyValue { k, v ->
            if (!first) {
                s.append(',')
                s.append(' ')
            }
            first = false;

            s.append(k)
            s.append('=')
            s.append(v)
        }
        s.append('}')
        return s.toString()
    }


    private val keySet: MutableLongSet =
            object : AbstractMutableLongCollection(), MutableLongSet {


                override fun contains(key: Long): Boolean {
                    return this@IndexTreeLongLongMap.containsKey(key)
                }

                override fun max(): Long {
                    val ret = treeLast(rootRecid, store, levels)
                            ?: throw NoSuchElementException()
                    return ret[0]
                }

                override fun min(): Long {
                    //find first key
                    val ret = treeIter(dirShift, rootRecid, store, levels, 0)
                            ?: throw NoSuchElementException()
                    return ret[0]
                }

                override fun clear() {
                    this@IndexTreeLongLongMap.clear()
                }

                override fun freeze(): LongSet {
                    return LongHashSet.newSet(this);
                }


                override fun forEach(procedure: LongProcedure) {
                    this@IndexTreeLongLongMap.forEachKey(procedure)
                }

                override fun longIterator(): MutableLongIterator {
                    return Iterator(this@IndexTreeLongLongMap, 0)
                }

                override fun remove(value: Long): Boolean {
                    val ret = this@IndexTreeLongLongMap.containsKey(value)
                    if(ret)
                        this@IndexTreeLongLongMap.removeKey(value)
                    return ret;
                }

                override fun removeAll(source: LongIterable): Boolean {
                    var changed = false
                    source.forEach { k ->
                        if(remove(k))
                            changed = true
                    }
                    return changed
                }

                override fun removeAll(vararg source: Long): Boolean {
                    var changed = false
                    source.forEach { k ->
                        if(remove(k))
                            changed = true
                    }
                    return changed
                }

                override fun retainAll(elements: LongIterable): Boolean {
                    var changed = false
                    forEach { k ->
                        if(!elements.contains(k)) {
                            remove(k)
                            changed = true
                        }
                    }
                    return changed;
                }

                override fun retainAll(vararg source: Long): Boolean {
                    return this.retainAll(LongHashSet.newSetWith(*source))
                }

                override fun toImmutable(): ImmutableLongSet {
                    return LongSets.immutable.withAll(this)
                }


                override fun asUnmodifiable(): MutableLongSet {
                    return UnmodifiableLongSet.of(this)
                }

                override fun asSynchronized(): MutableLongSet {
                    return SynchronizedLongSet.of(this)
                }

                override fun size(): Int {
                    return this@IndexTreeLongLongMap.size()
                }

            }

    override fun keySet(): MutableLongSet {
        return keySet;
    }

    private val keysView = LazyLongIterableAdapter(keySet);

    override fun keysView(): LazyLongIterable {
        return keysView;
    }

    private val keysValuesView:RichIterable<LongLongPair> = object : AbstractLazyIterable<LongLongPair>(){

        override fun each(procedure: Procedure<in LongLongPair>) {
            this@IndexTreeLongLongMap.forEachKeyValue { k, v ->
                procedure.value(PrimitiveTuples.pair(k, v))
            }
        }

        override fun iterator(): MutableIterator<LongLongPair> {
            return object : MutableIterator<LongLongPair> {
                ;
                    var nextKey: Long? = -1L
                    var nextRet: LongLongPair? = null
                    var lastKey: Long? = null;

                    override fun hasNext(): Boolean {
                        if(nextRet!=null)
                            return true
                        val prev = nextKey ?: return false;
                        val ret = treeIter(dirShift, rootRecid, store, levels, prev+1)
                        if(ret==null) {
                            nextRet = null
                            nextKey = null
                        }else{
                            nextRet = PrimitiveTuples.pair(ret[0], ret[1])
                            nextKey = ret[0]
                        }
                        return nextRet!=null
                    }

                    override fun next(): LongLongPair {
                        val ret = nextRet
                        nextRet = null;
                        if (ret == null) {
                            if(nextKey!=null){
                                //fetch next item
                                if(hasNext())
                                    return next()
                            }

                            lastKey = null
                            throw NoSuchElementException()
                        }
                        lastKey = ret.one
                        return ret;
                    }

                    override fun remove() {
                        removeKey(lastKey ?: throw UnsupportedOperationException())
                        lastKey = null
                    }
            }
        }

    }

    override fun keyValuesView(): RichIterable<LongLongPair> {
        return keysValuesView
    }

    override fun toImmutable(): ImmutableLongLongMap {
        throw UnsupportedOperationException()
    }

    private val values: MutableLongCollection =
            object : AbstractMutableLongCollection(){

                override fun contains(value: Long): Boolean {
                    return this@IndexTreeLongLongMap.containsValue(value)
                }

                override fun size(): Int {
                    return this@IndexTreeLongLongMap.size()
                }

                override fun forEach(procedure: LongProcedure) {
                    this@IndexTreeLongLongMap.forEach(procedure)
                }

                override fun max(): Long {
                    return this@IndexTreeLongLongMap.max()
                }

                override fun min(): Long {
                    return this@IndexTreeLongLongMap.min()
                }

                override fun asSynchronized(): MutableLongCollection? {
                    //TODO synchronized
                    throw UnsupportedOperationException()
                }

                override fun asUnmodifiable(): MutableLongCollection? {
                    //TODO synchronized
                    throw UnsupportedOperationException()
                }

                override fun clear() {
                    this@IndexTreeLongLongMap.clear()
                }

                override fun longIterator(): MutableLongIterator? {
                    return Iterator(this@IndexTreeLongLongMap, 1)
                }

                override fun remove(value: Long): Boolean {
                    var removed = false;
                    forEachKeyValue { k, v ->
                        if(value===v){
                            removeKey(k)
                            removed = true
                        }
                    }
                    return removed
                }

                override fun removeAll(source: LongIterable): Boolean {
                    val values = source.toSet()
                    var removed = false;
                    forEachKeyValue { k, v ->
                        if(values.contains(v)){
                            removeKey(k)
                            removed = true
                        }
                    }
                    return removed
                }

                override fun removeAll(vararg source: Long): Boolean {
                    return removeAll(LongHashSet.newSetWith(*source))
                }

                override fun retainAll(elements: LongIterable): Boolean {
                    val values = elements.toSet()
                    var removed = false;
                    forEachKeyValue { k, v ->
                        if(values.contains(v).not()){
                            removeKey(k)
                            removed = true
                        }
                    }
                    return removed
                }

                override fun retainAll(vararg source: Long): Boolean {
                    return retainAll(LongHashSet.newSetWith(*source))
                }

                override fun toImmutable(): ImmutableLongCollection? {
                    throw UnsupportedOperationException()
                }


            }

    override fun values(): MutableLongCollection {
        return values;
    }

}


internal abstract open class AbstractMutableLongCollection :
        AbstractLongIterable(),
        MutableLongCollection {

    override fun allSatisfy(predicate: LongPredicate): Boolean {
        val iter = longIterator()
        while(iter.hasNext()){
            if(!predicate.accept(iter.next()))
                return false;
        }
        return true
    }

    override fun appendString(appendable: Appendable?, start: String?, separator: String?, end: String?) {
        LongIterableIterate.appendString(this, appendable, start, separator, end)
    }


    override fun toArray(): LongArray? {
        var ret = LongArray(32)
        var pos = 0;
        forEach{k->
            if(ret.size==pos)
                ret = Arrays.copyOf(ret,ret.size*2)
            ret[pos++] = k;
        }
        if(pos !=ret.size)
            ret = Arrays.copyOf(ret,pos)
        return ret
    }

    override fun sum(): Long {
        var ret = 0L
        forEach{ret+=it}
        return ret
    }

    override fun noneSatisfy(predicate: LongPredicate?): Boolean {
        return LongIterableIterate.noneSatisfy(this, predicate)
    }


    override fun <T : Any?> injectInto(injectedValue: T, function: ObjectLongToObjectFunction<in T, out T>?): T {
        return LongIterableIterate.injectInto(this, injectedValue, function)
    }


    override fun each(procedure: LongProcedure) {
        forEach(procedure)
    }

    override fun detectIfNone(predicate: LongPredicate?, ifNone: Long): Long {
        return LongIterableIterate.detectIfNone(this, predicate, ifNone)
    }

    override fun count(predicate: LongPredicate?): Int {
        return LongIterableIterate.count(this, predicate)
    }

    override fun anySatisfy(predicate: LongPredicate?): Boolean {
        return LongIterableIterate.anySatisfy(this, predicate)
    }


    override fun <V : Any?> collect(function: LongToObjectFunction<out V>): MutableSet<V> {
        val result = Sets.mutable.with<V>()
        forEach { e->
            result.add(function.valueOf(e))
        }
        return result
    }


    override fun reject(predicate: LongPredicate): MutableLongSet {
        val ret = LongHashSet()
        forEach { r->
            if(predicate.accept(r).not())
                ret.add(r)
        }
        return ret
    }

    override fun select(predicate: LongPredicate): MutableLongSet {
        val ret = LongHashSet()
        forEach { r->
            if(predicate.accept(r))
                ret.add(r)
        }
        return ret
    }



    override fun add(element: Long): Boolean {
        throw UnsupportedOperationException("Cannot call add() on " + this.javaClass.simpleName)
    }

    override fun addAll(vararg source: Long): Boolean {
        throw UnsupportedOperationException("Cannot call addAll() on " + this.javaClass.simpleName)
    }

    override fun addAll(source: LongIterable): Boolean {
        throw UnsupportedOperationException("Cannot call addAll() on " + this.javaClass.simpleName)
    }

    override fun with(element: Long): MutableLongSet {
        throw UnsupportedOperationException("Cannot call with() on " + this.javaClass.simpleName)
    }

    override fun without(element: Long): MutableLongSet {
        throw UnsupportedOperationException("Cannot call without() on " + this.javaClass.simpleName)
    }

    override fun withAll(elements: LongIterable): MutableLongSet {
        throw UnsupportedOperationException("Cannot call withAll() on " + this.javaClass.simpleName)
    }

    override fun withoutAll(elements: LongIterable): MutableLongSet {
        throw UnsupportedOperationException("Cannot call withoutAll() on " + this.javaClass.simpleName)
    }


    override fun equals(obj: Any?): Boolean {
        if (this === obj) {
            return true
        }
        if (obj !is LongSet) {
            return false
        }
        return this.size() == obj.size() && this.containsAll(obj)
    }


    override fun hashCode(): Int {
        var ret = 0;
        forEach{k->
            ret += DataIO.longHash(k)
        }
        return ret;
    }
}
