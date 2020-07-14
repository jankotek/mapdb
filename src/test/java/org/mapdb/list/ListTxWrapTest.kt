package org.mapdb.list

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Assert.assertArrayEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.mapdb.TT
import org.mapdb.ser.Serializers
import org.mapdb.store.HeapBufStore
import org.mapdb.store.StoreTxWrap

@RunWith(VertxUnitRunner::class)
class ListTxWrapTest {

    @Test
    fun testTx(c: TestContext) {

        val store = HeapBufStore()
        val tx = StoreTxWrap(store)
        val l = ListTxWrap<Int>(tx, Serializers.INTEGER)

        TT.async {
            Thread.sleep(1000)
            tx.txStart();
            l += 1
            tx.commit();
        }

        tx.txStart()
        l+=2
        tx.commit()
        assertArrayEquals(intArrayOf(2), l.toIntArray())
        Thread.sleep(2000)
        assertArrayEquals(intArrayOf(2,1), l.toIntArray())

    }
}