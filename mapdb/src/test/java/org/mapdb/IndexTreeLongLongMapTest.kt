package org.mapdb

import com.gs.collections.api.LazyLongIterable
import com.gs.collections.api.collection.primitive.MutableLongCollection
import com.gs.collections.api.map.primitive.MutableLongLongMap
import com.gs.collections.api.set.primitive.MutableLongSet
import com.gs.collections.impl.map.mutable.primitive.LongLongHashMap
import org.mapdb.indexTreeLongLongMapTests_GS_GENERATED.*
import org.junit.Assert.*
import org.junit.Test
import java.util.*
import kotlin.test.assertFailsWith

class IndexTreeLongLongMapTest{

    @Test fun get_Set(){
        val map = IndexTreeLongLongMap.make()

        map.put(0L, 111L)
        map.put(3423L, 4234L)

        assertEquals(111L, map.get(0L))
        assertEquals(4234L, map.get(3423L))
    }

    @Test fun key_iter(){
        val map = IndexTreeLongLongMap.make()

        map.put(0L, 111L)
        map.put(3423L, 4234L)

        val iter = map.keyIterator()
        assertTrue(iter.hasNext())
        assertEquals(0L, iter.nextLong())
        assertTrue(iter.hasNext())
        assertEquals(3423L, iter.nextLong())
        assertFalse(iter.hasNext())
        assertFailsWith(NoSuchElementException::class.java, {
            iter.nextLong()
        })

    }

    @Test fun zero_val(){
        val map = IndexTreeLongLongMap.make()
        map.put(0L,0L);
        assertTrue(map.containsKey(0L))
        map.put(33L,0L);
        assertTrue(map.containsKey(33L))
    }

    @Test fun forEachKeyVal(){
        val map = IndexTreeLongLongMap.make()
        val ref = LongLongHashMap()
        for(i in 0L until 1000){
            map.put(i, i*10)
        }
        assertEquals(1000, map.size())

        map.forEachKeyValue { key, value ->
            ref.put(key,value)
        }

        for(i in 0L until 1000){
            assertEquals(i*10, ref.get(i))
        }

    }


    class GSHashMapTest(): AbstractMutableLongLongMapTestCase(){
        override fun classUnderTest(): MutableLongLongMap? {
            return newWithKeysValues(0L, 0L, 31L, 31L, 32L, 32L)
        }

        override fun getEmptyMap(): MutableLongLongMap? {
            return IndexTreeLongLongMap.make()
        }

        override fun newWithKeysValues(key1: Long, value1: Long): MutableLongLongMap? {
            val ret = IndexTreeLongLongMap.make()
            ret.put(key1, value1);
            return ret
        }

        override fun newWithKeysValues(key1: Long, value1: Long, key2: Long, value2: Long): MutableLongLongMap? {
            val ret = IndexTreeLongLongMap.make()
            ret.put(key1, value1);
            ret.put(key2, value2);
            return ret
        }

        override fun newWithKeysValues(key1: Long, value1: Long, key2: Long, value2: Long, key3: Long, value3: Long): MutableLongLongMap? {
            val ret = IndexTreeLongLongMap.make()
            ret.put(key1, value1);
            ret.put(key2, value2);
            ret.put(key3, value3);
            return ret
        }

        override fun newWithKeysValues(key1: Long, value1: Long, key2: Long, value2: Long, key3: Long, value3: Long, key4: Long, value4: Long): MutableLongLongMap? {
            val ret = IndexTreeLongLongMap.make()
            ret.put(key1, value1);
            ret.put(key2, value2);
            ret.put(key3, value3);
            ret.put(key4, value4);
            return ret
        }

        override fun asSynchronized() {
            //TODO Ask to expose wrapper constructor (now is package private)
        }

        override fun asUnmodifiable() {
            //TODO Ask to expose wrapper constructor (now is package private)
        }

        override fun toImmutable() {
            //TODO Ask to expose wrapper constructor (now is package private)
        }
    }

    class GSLongLongHashMapKeySetTest: LongLongHashMapKeySetTest(){

        override fun classUnderTest(): MutableLongSet {
            val v =  IndexTreeLongLongMap.make()
            v.put(1L,1L)
            v.put(2L,2L)
            v.put(3L,3L)
            return v.keySet()
        }

        override fun newWith(vararg elements: Long): MutableLongSet {
            val map = IndexTreeLongLongMap.make()
            for (i in elements.indices) {
                map.put(elements[i], i.toLong())
            }
            return map.keySet()
        }

    }

    class GSLongLongHashMapKeysViewTest : AbstractLazyLongIterableTestCase(){

        override fun classUnderTest(): LazyLongIterable? {
            val v =  IndexTreeLongLongMap.make()
            v.put(1L,1L)
            v.put(2L,2L)
            v.put(3L,3L)
            return v.keysView()
        }

        override fun getEmptyIterable(): LazyLongIterable? {
            return IndexTreeLongLongMap.make().keysView()
        }

        override fun newWith(element1: Long, element2: Long): LazyLongIterable? {
            val v =  IndexTreeLongLongMap.make()
            v.put(element1, 1L)
            v.put(element2, 2L)
            return v.keysView()
        }

    }

    class GSLongLongHashMapKeyValuesViewTest: AbstractLongLongMapKeyValuesViewTestCase(){
        override fun newWithKeysValues(key1: Long, value1: Long, key2: Long, value2: Long, key3: Long, value3: Long): MutableLongLongMap {
            val v =  IndexTreeLongLongMap.make()
            v.put(key1,value1)
            v.put(key2,value2)
            v.put(key3,value3)
            return v
        }

        override fun newWithKeysValues(key1: Long, value1: Long, key2: Long, value2: Long): MutableLongLongMap {
            val v =  IndexTreeLongLongMap.make()
            v.put(key1,value1)
            v.put(key2,value2)
            return v
        }

        override fun newWithKeysValues(key1: Long, value1: Long): MutableLongLongMap {
            val v =  IndexTreeLongLongMap.make()
            v.put(key1,value1)
            return v
        }

        override fun newEmpty(): LongLongHashMap {
            return LongLongHashMap()
        }
    }

    class GSLongLongHashMapValuesTest: LongLongHashMapValuesTest(){

        override fun classUnderTest(): MutableLongCollection? {
            val v =  IndexTreeLongLongMap.make()
            v.put(1L,1L)
            v.put(2L,2L)
            v.put(3L,3L)
            return v.values()
        }

        override fun newWith(vararg elements: Long): MutableLongCollection? {
            val v =  IndexTreeLongLongMap.make()
            for(i in 0 until elements.size)
                v.put(i.toLong(), elements[i])
            return v.values()
        }

        override fun newWithKeysValues(vararg args: Long): MutableLongLongMap? {
            val v =  IndexTreeLongLongMap.make()
            var i=0;
            while(i<args.size)
                v.put(args[i++], args[i++])
            return v
        }

    }

}