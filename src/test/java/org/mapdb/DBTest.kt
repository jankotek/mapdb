package org.mapdb

import io.kotlintest.should
import io.kotlintest.shouldBe
import org.junit.Test
import org.mapdb.serializer.Serializers
import org.mapdb.store.StoreOnHeapSer
import org.mapdb.store.StoreOnHeap

class DBTest{

    @Test
    fun store(){
        DB.newOnHeapDB().make().store should {it is StoreOnHeap}
        DB.newOnHeapSerDB().make().store should {it is StoreOnHeapSer }
    }

    @Test
    fun threadSafe(){
        fun DB.Maker.thSafe() = (this.make().store as StoreOnHeap).isThreadSafe
        DB.newOnHeapDB().thSafe() shouldBe  true
        DB.newOnHeapDB().threadSafeDisable().thSafe() shouldBe  false
        DB.newOnHeapDB().threadSafe(true).thSafe() shouldBe  true
        DB.newOnHeapDB().threadSafe(false).thSafe() shouldBe  false
    }

    @Test fun linkedList(){
        val s = DB.newOnHeapDB().make().linkedList("list", Serializers.LONG).make()
        s.put(1L)
        s.size shouldBe 1
    }
}