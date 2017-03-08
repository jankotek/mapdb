package org.mapdb

import org.junit.Assert.*
import org.junit.Test
import java.util.*
import java.util.concurrent.LinkedBlockingQueue

class QueueLongTest {
    val q = QueueLong.make()
    fun node(recid: Long) = q.store.get(recid, QueueLong.Node.SERIALIZER)!!

    @Test fun insert_take() {
        assertEquals(4, q.store.getAllRecids().asSequence().count())
        val oldHead = q.head
        assertEquals(q.head, q.tail)
        assertNotEquals(0L, q.head)
        assertEquals(0L, q.headPrev)

        assertNull(q.store.get(q.head, QueueLong.Node.SERIALIZER))

        // insert first element
        val recid = q.put(111L, 222L)
        assertEquals(oldHead, recid)
        assertEquals(recid, q.tail)
        assertEquals(recid, q.headPrev)


        val node = q.store.get(recid, QueueLong.Node.SERIALIZER)!!
        assertEquals(QueueLong.Node(prevRecid = 0, nextRecid = q.head, timestamp = 111L, value = 222L), node)

        assertEquals(node.nextRecid, q.head)
        assertNull(q.store.get(node.nextRecid, QueueLong.Node.SERIALIZER))

        //take first element
        assertEquals(node, q.take())
        assertEquals(node.nextRecid, q.tail)
        assertEquals(node.nextRecid, q.head)
        assertEquals(0L, q.headPrev)
        TT.assertFailsWith(DBException.GetVoid::class.java) {
            q.store.get(recid, QueueLong.Node.SERIALIZER)!!
        }
        assertEquals(4, q.store.getAllRecids().asSequence().count())
        assertNull(q.take())
    }

    @Test fun put_take_many() {
        assertEquals(4, q.store.getAllRecids().asSequence().count())
        val ref = LinkedBlockingQueue<Pair<Long, Long>>()

        q.verify()
        val r = Random()
        for (i in 0 until 10000) {
            val t = r.nextLong()
            val v = r.nextLong()
            q.put(t, v)
            ref.add(Pair(t, v))
        }
        assertEquals(10000, q.size())
        assertEquals(4 + 10000, q.store.getAllRecids().asSequence().count())
        q.verify()
        var node = q.take()
        while (node != null) {
            assertEquals(ref.take(), Pair(node.timestamp, node.value))
            node = q.take()
        }
        q.verify()
        assertTrue(ref.isEmpty())
        assertEquals(q.head, q.tail)
        assertEquals(0L, q.headPrev)
        assertTrue(q.tail != 0L)
        assertEquals(4, q.store.getAllRecids().asSequence().count())
    }


    @Test fun remove_start() {
        assertEquals(4, q.store.getAllRecids().asSequence().count())
        q.verify()
        val recid1 = q.put(1L, 11L)
        q.verify()
        val recid2 = q.put(2L, 22L)
        val recid3 = q.put(3L, 33L)
        q.verify()
        assertEquals(4 + 3, q.store.getAllRecids().asSequence().count())

        assertTrue(recid1 != recid2 && recid2 != recid3 && recid3 != q.head)

        assertEquals(recid1, q.tail)
        assertEquals(recid3, q.headPrev)

        assertEquals(0L, node(recid1).prevRecid)
        assertEquals(recid2, node(recid1).nextRecid)

        assertEquals(recid1, node(recid2).prevRecid)
        assertEquals(recid3, node(recid2).nextRecid)

        assertEquals(recid2, node(recid3).prevRecid)
        assertEquals(q.head, node(recid3).nextRecid)
        q.verify()
        q.remove(recid1, removeNode = true)
        q.verify()
        assertEquals(4 + 2, q.store.getAllRecids().asSequence().count())

        assertEquals(recid2, q.tail)
        assertEquals(recid3, q.headPrev)

        TT.assertFailsWith(DBException.GetVoid::class.java) {
            node(recid1)
        }

        assertEquals(0L, node(recid2).prevRecid)
        assertEquals(recid3, node(recid2).nextRecid)

        assertEquals(recid2, node(recid3).prevRecid)
        assertEquals(q.head, node(recid3).nextRecid)

        assertEquals(4 + 2, q.store.getAllRecids().asSequence().count())
        q.verify()
        assertEquals(22L, q.take()!!.value)
        q.verify()
        assertEquals(4 + 1, q.store.getAllRecids().asSequence().count())
        q.verify()
        assertEquals(33L, q.take()!!.value)
        q.verify()

        assertEquals(4, q.store.getAllRecids().asSequence().count())
        assertNull(q.take())
        assertEquals(4, q.store.getAllRecids().asSequence().count())
    }

    @Test fun remove_middle() {
        assertEquals(4, q.store.getAllRecids().asSequence().count())
        val recid1 = q.put(1L, 11L)
        val recid2 = q.put(2L, 22L)
        val recid3 = q.put(3L, 33L)
        assertEquals(4 + 3, q.store.getAllRecids().asSequence().count())

        assertTrue(recid1 != recid2 && recid2 != recid3 && recid3 != q.head)

        assertEquals(recid1, q.tail)
        assertEquals(recid3, q.headPrev)

        assertEquals(0L, node(recid1).prevRecid)
        assertEquals(recid2, node(recid1).nextRecid)

        assertEquals(recid1, node(recid2).prevRecid)
        assertEquals(recid3, node(recid2).nextRecid)

        assertEquals(recid2, node(recid3).prevRecid)
        assertEquals(q.head, node(recid3).nextRecid)

        q.verify()
        q.remove(recid2, removeNode = true)
        q.verify()
        assertEquals(4 + 2, q.store.getAllRecids().asSequence().count())

        TT.assertFailsWith(DBException.GetVoid::class.java) {
            node(recid2)
        }

        assertEquals(recid1, q.tail)
        assertEquals(recid3, q.headPrev)

        assertEquals(0L, node(recid1).prevRecid)
        assertEquals(recid3, node(recid1).nextRecid)

        assertEquals(recid1, node(recid3).prevRecid)
        assertEquals(q.head, node(recid3).nextRecid)

        q.verify()
        assertEquals(11L, q.take()!!.value)
        q.verify()
        assertEquals(4 + 1, q.store.getAllRecids().asSequence().count())
        assertEquals(33L, q.take()!!.value)
        q.verify()
        assertEquals(4, q.store.getAllRecids().asSequence().count())
        assertNull(q.take())
        q.verify()
        assertEquals(4, q.store.getAllRecids().asSequence().count())
    }

    @Test fun remove_end() {
        assertEquals(4, q.store.getAllRecids().asSequence().count())
        val recid1 = q.put(1L, 11L)
        val recid2 = q.put(2L, 22L)
        val recid3 = q.put(3L, 33L)
        assertEquals(4 + 3, q.store.getAllRecids().asSequence().count())

        assertTrue(recid1 != recid2 && recid2 != recid3 && recid3 != q.head)

        assertEquals(recid1, q.tail)
        assertEquals(recid3, q.headPrev)

        assertEquals(0L, node(recid1).prevRecid)
        assertEquals(recid2, node(recid1).nextRecid)

        assertEquals(recid1, node(recid2).prevRecid)
        assertEquals(recid3, node(recid2).nextRecid)

        assertEquals(recid2, node(recid3).prevRecid)
        assertEquals(q.head, node(recid3).nextRecid)

        assertEquals(4 + 3, q.store.getAllRecids().asSequence().count())
        q.verify()
        q.remove(recid3, removeNode = true)
        q.verify()
        assertEquals(4 + 2, q.store.getAllRecids().asSequence().count())

        TT.assertFailsWith(DBException.GetVoid::class.java) {
            node(recid3)
        }

        assertEquals(recid1, q.tail)
        assertEquals(recid2, q.headPrev)

        assertEquals(0L, node(recid1).prevRecid)
        assertEquals(recid2, node(recid1).nextRecid)

        assertEquals(recid1, node(recid2).prevRecid)
        assertEquals(q.head, node(recid2).nextRecid)

        assertEquals(4 + 2, q.store.getAllRecids().asSequence().count())
        assertEquals(11L, q.take()!!.value)
        q.verify()
        assertEquals(4 + 1, q.store.getAllRecids().asSequence().count())
        assertEquals(22L, q.take()!!.value)
        q.verify()
        assertEquals(4, q.store.getAllRecids().asSequence().count())
        assertNull(q.take())
        q.verify()
        assertEquals(4, q.store.getAllRecids().asSequence().count())
    }


    @Test fun bump_start() {
        val recid1 = q.put(1L, 11L)
        val recid2 = q.put(2L, 22L)
        val recid3 = q.put(3L, 33L)

        assertTrue(recid1 != recid2 && recid2 != recid3 && recid3 != q.head)

        assertEquals(recid1, q.tail)
        assertEquals(recid3, q.headPrev)

        assertEquals(0L, node(recid1).prevRecid)
        assertEquals(recid2, node(recid1).nextRecid)

        assertEquals(recid1, node(recid2).prevRecid)
        assertEquals(recid3, node(recid2).nextRecid)

        assertEquals(recid2, node(recid3).prevRecid)
        assertEquals(q.head, node(recid3).nextRecid)

        q.verify()
        q.bump(recid1, 111L)
        q.verify()

        assertEquals(recid2, q.tail)
        assertEquals(recid1, q.headPrev)

        assertEquals(0L, node(recid2).prevRecid)
        assertEquals(recid3, node(recid2).nextRecid)

        assertEquals(recid2, node(recid3).prevRecid)
        assertEquals(recid1, node(recid3).nextRecid)

        assertEquals(recid3, node(recid1).prevRecid)
        assertEquals(q.head, node(recid1).nextRecid)

        q.verify()
        assertEquals(2L, q.take()!!.timestamp)
        q.verify()
        assertEquals(3L, q.take()!!.timestamp)
        q.verify()
        assertEquals(111L, q.take()!!.timestamp)
        q.verify()
        assertNull(q.take())
    }

    @Test fun bump_middle() {
        assertEquals(4, q.store.getAllRecids().asSequence().count())
        val recid1 = q.put(1L, 11L)
        val recid2 = q.put(2L, 22L)
        val recid3 = q.put(3L, 33L)
        assertEquals(4 + 3, q.store.getAllRecids().asSequence().count())

        assertTrue(recid1 != recid2 && recid2 != recid3 && recid3 != q.head)

        assertEquals(recid1, q.tail)
        assertEquals(recid3, q.headPrev)

        assertEquals(0L, node(recid1).prevRecid)
        assertEquals(recid2, node(recid1).nextRecid)

        assertEquals(recid1, node(recid2).prevRecid)
        assertEquals(recid3, node(recid2).nextRecid)

        assertEquals(recid2, node(recid3).prevRecid)
        assertEquals(q.head, node(recid3).nextRecid)

        q.verify()
        assertEquals(4 + 3, q.store.getAllRecids().asSequence().count())
        q.bump(recid2, 222L)
        assertEquals(4 + 3, q.store.getAllRecids().asSequence().count())
        q.verify()

        assertEquals(recid1, q.tail)
        assertEquals(recid2, q.headPrev)

        assertEquals(0L, node(recid1).prevRecid)
        assertEquals(recid3, node(recid1).nextRecid)

        assertEquals(recid1, node(recid3).prevRecid)
        assertEquals(recid2, node(recid3).nextRecid)

        assertEquals(recid3, node(recid2).prevRecid)
        assertEquals(q.head, node(recid2).nextRecid)

        assertEquals(1L, q.take()!!.timestamp)
        q.verify()
        assertEquals(4 + 2, q.store.getAllRecids().asSequence().count())
        assertEquals(3L, q.take()!!.timestamp)
        q.verify()
        assertEquals(222L, q.take()!!.timestamp)
        q.verify()
        assertNull(q.take())
        q.verify()
        assertEquals(4, q.store.getAllRecids().asSequence().count())
    }

    @Test fun bump_end() {

        val recid1 = q.put(1L, 11L)
        val recid2 = q.put(2L, 22L)
        val recid3 = q.put(3L, 33L)

        assertTrue(recid1 != recid2 && recid2 != recid3 && recid3 != q.head)

        assertEquals(recid1, q.tail)
        assertEquals(recid3, q.headPrev)

        assertEquals(0L, node(recid1).prevRecid)
        assertEquals(recid2, node(recid1).nextRecid)

        assertEquals(recid1, node(recid2).prevRecid)
        assertEquals(recid3, node(recid2).nextRecid)

        assertEquals(recid2, node(recid3).prevRecid)
        assertEquals(q.head, node(recid3).nextRecid)

        assertEquals(4 + 3, q.store.getAllRecids().asSequence().count())
        q.verify()
        q.bump(recid3, 333L)
        q.verify()
        assertEquals(4 + 3, q.store.getAllRecids().asSequence().count())

        assertEquals(0L, node(recid1).prevRecid)
        assertEquals(recid2, node(recid1).nextRecid)

        assertEquals(recid1, node(recid2).prevRecid)
        assertEquals(recid3, node(recid2).nextRecid)

        assertEquals(recid2, node(recid3).prevRecid)
        assertEquals(q.head, node(recid3).nextRecid)

        q.verify()
        assertEquals(1L, q.take()!!.timestamp)
        q.verify()
        assertEquals(4 + 2, q.store.getAllRecids().asSequence().count())
        assertEquals(2L, q.take()!!.timestamp)
        q.verify()
        assertEquals(333L, q.take()!!.timestamp)
        q.verify()
        assertNull(q.take())
        q.verify()
        assertEquals(4, q.store.getAllRecids().asSequence().count())
    }

    @Test fun takeUntil() {
        val recid1 = q.put(1L, 11L)
        q.put(2L, 22L)
        q.put(3L, 33L)

        assertEquals(4 + 3, q.store.getAllRecids().asSequence().count())
        q.takeUntil(QueueLongTakeUntil { nodeRecid, _ ->
            q.verify()
            nodeRecid == recid1
        })
        assertEquals(4 + 2, q.store.getAllRecids().asSequence().count())
        q.verify()
        assertEquals(2L, q.take()!!.timestamp)
        q.verify()
        assertEquals(4 + 1, q.store.getAllRecids().asSequence().count())
        assertEquals(3L, q.take()!!.timestamp)
        assertEquals(4, q.store.getAllRecids().asSequence().count())
        q.verify()
        assertNull(q.take())
        q.verify()
        assertEquals(4, q.store.getAllRecids().asSequence().count())

    }


    @Test fun takeUntil2() {
        val recid1 = q.put(1L, 11L)
        q.put(2L, 22L)

        assertEquals(4 + 2, q.store.getAllRecids().asSequence().count())
        q.takeUntil(QueueLongTakeUntil { nodeRecid, _ ->
            q.verify()
            nodeRecid == recid1
        })
        assertEquals(4 + 1, q.store.getAllRecids().asSequence().count())
        q.verify()
        assertEquals(2L, q.take()!!.timestamp)
        q.verify()
        assertEquals(4 + 0, q.store.getAllRecids().asSequence().count())
        assertNull(q.take())
        q.verify()
        assertEquals(4, q.store.getAllRecids().asSequence().count())

    }


    @Test fun takeUntilAll() {
        val recid1 = q.put(1L, 11L)
        val recid2 = q.put(2L, 22L)
        val recid3 = q.put(3L, 33L)
        assertEquals(4 + 3, q.store.getAllRecids().asSequence().count())
        q.takeUntil(QueueLongTakeUntil { nodeRecid, _ ->
            assertTrue(nodeRecid in setOf(recid1, recid2, recid3))
            q.verify()
            true
        })
        q.verify()
        assertEquals(4, q.store.getAllRecids().asSequence().count())
        assertNull(q.take())
        assertEquals(4, q.store.getAllRecids().asSequence().count())
    }

    @Test fun clear() {
        q.put(1L, 11L)
        q.put(2L, 22L)
        q.put(3L, 33L)
        assertEquals(3, q.size())
        assertEquals(4 + 3, q.store.getAllRecids().asSequence().count())
        q.verify()
        q.clear()
        q.verify()
        assertEquals(0, q.size())
        assertEquals(4, q.store.getAllRecids().asSequence().count())
    }

    @Test fun size() {
        assertEquals(0, q.size())
        q.put(1L, 11L)
        q.put(2L, 22L)
        q.put(3L, 33L)
        assertEquals(3, q.size())
        q.verify()
    }

    @Test fun valuesArray() {
        assertEquals(0, q.size())
        q.put(1L, 11L)
        q.put(2L, 22L)
        q.put(3L, 33L)
        assertArrayEquals(longArrayOf(11L, 22L, 33L), q.valuesArray())
    }
}