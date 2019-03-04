package org.mapdb.list

import io.kotlintest.shouldBe
import org.junit.Test
import org.mapdb.ser.Serializers
import org.mapdb.store.StoreOnHeap


class LinkedListTest{

    @Test
    fun test(){
        val list = LinkedList<Long>(store = StoreOnHeap(), serializer = Serializers.LONG)
        list.put(1L)
        list.put(2L)
        list.put(3L)

        list.size shouldBe 3

        var counter = 0L;
        list.forEach { it shouldBe ++counter }
        counter shouldBe 3L

        list.toList() shouldBe arrayListOf(1L, 2L, 3L)
    }

}