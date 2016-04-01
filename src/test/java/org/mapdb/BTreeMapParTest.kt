package org.mapdb

import org.junit.Test

import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.atomic.AtomicLong

import org.junit.Assert.assertEquals

class BTreeMapParTest {


    internal var scale = TT.testScale()
    internal val threadNum = 6 * scale
    internal val max = 1e6.toInt() * scale

    @Test
    @Throws(InterruptedException::class)
    fun parInsert() {
        if (scale == 0)
            return


        val m = DBMaker.memoryDB().make().treeMap("test").valueSerializer(Serializer.LONG).keySerializer(Serializer.LONG).make()

        val t = System.currentTimeMillis()
        val counter = AtomicLong()

        TT.fork(threadNum, {core->
                var n: Long = core.toLong()
                while (n < max) {
                    m.put(n, n)
                    n += threadNum.toLong()
                }
       })

        //        System.out.printf("  Threads %d, time %,d\n",threadNum,System.currentTimeMillis()-t);


        assertEquals(max.toLong(), m.size.toLong())
    }
}
