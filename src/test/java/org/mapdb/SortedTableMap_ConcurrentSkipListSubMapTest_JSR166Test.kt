package org.mapdb

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.mapdb.jsr166Tests.ConcurrentSkipListSubMapTest
import org.mapdb.jsr166Tests.JSR166Test
import org.mapdb.jsr166Tests.JSR166TestCase
import java.util.concurrent.ConcurrentNavigableMap

class SortedTableMap_ConcurrentSkipListSubMapTest_JSR166Test()
    : ConcurrentSkipListSubMapTest()
{


    protected override fun map5(): ConcurrentNavigableMap<*, *>? {
        val consumer = SortedTableMap.import(
                keySerializer = Serializer.INTEGER,
                valueSerializer = Serializer.STRING_INTERN,
                volume = CC.DEFAULT_MEMORY_VOLUME_FACTORY.makeVolume(null, false))
        consumer.take(Pair(JSR166Test.zero, "Z"))
        consumer.take(Pair(JSR166Test.one, "A"))
        consumer.take(Pair(JSR166Test.two, "B"))
        consumer.take(Pair(JSR166Test.three, "C"))
        consumer.take(Pair(JSR166Test.four, "D"))
        consumer.take(Pair(JSR166Test.five, "E"))
        consumer.take(Pair(JSR166Test.seven, "F"))

        val map =  consumer.finish()
        assertFalse(map.isEmpty())
        assertEquals(7, map.size.toLong())
        return map.subMap(JSR166Test.one, true, JSR166Test.seven, false)
    }

    protected override fun dmap5(): ConcurrentNavigableMap<*, *>? {
        val consumer = SortedTableMap.import(
                keySerializer = Serializer.INTEGER,
                valueSerializer = Serializer.STRING_INTERN,
                volume = CC.DEFAULT_MEMORY_VOLUME_FACTORY.makeVolume(null, false))
        consumer.take(Pair(JSR166Test.m5, "E"))
        consumer.take(Pair(JSR166Test.m4, "D"))
        consumer.take(Pair(JSR166Test.m3, "C"))
        consumer.take(Pair(JSR166Test.m2, "B"))
        consumer.take(Pair(JSR166Test.m1, "A"))

        val map = consumer.finish().descendingMap()
        assertFalse(map.isEmpty())
        assertEquals(5, map.size.toLong())
        return map
    }


    override fun emptyMap(): ConcurrentNavigableMap<Int, String>? {
        return SortedTableMap.import(
                keySerializer = Serializer.INTEGER,
                valueSerializer = Serializer.STRING_INTERN,
                volume = CC.DEFAULT_MEMORY_VOLUME_FACTORY.makeVolume(null, false))
                .finish()
    }


    override protected fun isReadOnly(): Boolean {
        return true
    }


}
