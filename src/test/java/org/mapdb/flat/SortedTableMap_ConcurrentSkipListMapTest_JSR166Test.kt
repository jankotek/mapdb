package org.mapdb.flat

import org.mapdb.CC
import org.mapdb.Serializer
import org.mapdb.tree.jsr166Tests.ConcurrentSkipListMapTest
import org.mapdb.tree.jsr166Tests.JSR166TestCase
import java.util.*
import java.util.concurrent.ConcurrentNavigableMap

class SortedTableMap_ConcurrentSkipListMapTest_JSR166Test() : ConcurrentSkipListMapTest()
{

    override fun isReadOnly(): Boolean {
        return true
    }

    override fun map5(): ConcurrentNavigableMap<*, *>? {
        val consumer = SortedTableMap.createFromSink(
                keySerializer = Serializer.INTEGER,
                valueSerializer = Serializer.STRING_INTERN,
                volume = CC.DEFAULT_MEMORY_VOLUME_FACTORY.makeVolume(null, false))
        consumer.put(Pair(JSR166TestCase.one, "A"))
        consumer.put(Pair(JSR166TestCase.two, "B"))
        consumer.put(Pair(JSR166TestCase.three, "C"))
        consumer.put(Pair(JSR166TestCase.four, "D"))
        consumer.put(Pair(JSR166TestCase.five, "E"))
        return consumer.create()
    }

    override fun emptyMap(): ConcurrentNavigableMap<Int, String>? {
        return SortedTableMap.createFromSink(
                keySerializer = Serializer.INTEGER,
                valueSerializer = Serializer.STRING_INTERN,
                volume = CC.DEFAULT_MEMORY_VOLUME_FACTORY.makeVolume(null, false))
        .create()
    }

    override fun emptyIntMap(): ConcurrentNavigableMap<Int, Int>? {
        throw AssertionError()
    }

    override fun testEquals()
    {
        val map1 = map5()
        val map2 = map5()
        assertEquals(map1, map2)
        assertEquals(map2, map1)
    }

    override fun testPutIfAbsent() {}
    override fun testPutIfAbsent2() {}
    override fun testClear() {}
    override fun testPollLastEntry() {}
    override fun testPollFirstEntry() {}
    override fun testRemove3() {throw NullPointerException()}
    override fun testPutAll() {}
    override fun testPut1_NullPointerException() {}
    override fun testRemove() {}
    override fun testRemove2() {}
    override fun testRemove1_NullPointerException() {}
    override fun testRemove2_NullPointerException() {}
    override fun testReplace() {}
    override fun testReplace2() {}
    override fun testReplaceValue() {}
    override fun testReplaceValue2() {}
    override fun testReplaceValue_NullPointerException() {}
    override fun testReplace_NullPointerException() {}
    override fun testPutIfAbsent1_NullPointerException() {}

    override fun populatedIntMap(limit: Int): NavigableMap<Int, Int>? {
        val consumer = SortedTableMap.createFromSink(
                keySerializer = Serializer.INTEGER,
                valueSerializer = Serializer.INTEGER,
                volume = CC.DEFAULT_MEMORY_VOLUME_FACTORY.makeVolume(null, false))

        var i = 0
        val n = 2 * limit / 3
        val map = java.util.TreeMap<Int,Int>()
        while (i < n) {
            val key = rnd.nextInt(limit)
            map.put(key,key*2)
            bs.set(key)
            i++
        }
        map.forEach { k, v ->
            consumer.put(Pair(k, v))
        }

        return consumer.create()
    }
}
