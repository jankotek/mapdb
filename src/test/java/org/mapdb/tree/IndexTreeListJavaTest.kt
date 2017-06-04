package org.mapdb.tree

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.store.StoreTrivial
import java.nio.ByteBuffer
import java.util.*

class IndexTreeListJavaTest{

    internal fun swap(d: DataOutput2): DataInput2.ByteBuffer {
        val b = d.copyBytes()
        return DataInput2.ByteBuffer(ByteBuffer.wrap(b), 0)
    }


    @Test
    fun testDirSerializer() {
        var dir = IndexTreeListJava.dirEmpty()

        var slot = 1
        while (slot < 127) {
            dir = IndexTreeListJava.dirPut(dir, slot, slot * 1111L, slot*2222L)
            slot += 1 + slot / 5
        }

        val out = DataOutput2()
        IndexTreeListJava.dirSer.serialize(out, dir)

        val input = swap(out)
        val dir2 = IndexTreeListJava.dirSer.deserialize(input, -1);
        assertTrue(Arrays.equals(dir, dir2))

        slot = 1
        while (slot < 127) {
            val offset = IndexTreeListJava.dirOffsetFromSlot(dir, slot)
            assertEquals(slot * 1111L, dir[offset])
            assertEquals(slot * 2222L, dir[offset+1])
            slot += 1 + slot / 5
        }
    }

    @Test fun delete_notcollapsesNode(){
        val dir = IndexTreeListJava.dirEmpty();
        val store =StoreTrivial()
        val root = store.put(dir, IndexTreeListJava.dirSer)

        assertEquals(1, store.getAllRecids().asSequence().count())

        //single element without expansion
        IndexTreeListJava.treePut(4,root, store,3, 1L, 111L)
        assertEquals(1, store.getAllRecids().asSequence().count())

        //extra element near will expand all four levels
        IndexTreeListJava.treePut(4,root, store,3, 2L, 222L)
        assertEquals(4, store.getAllRecids().asSequence().count())

        //remove element, that should collapse nodes
        IndexTreeListJava.treeRemove(4,root,store, 3, 2L, null)
        assertEquals(4, store.getAllRecids().asSequence().count())
    }


    @Test fun delete_collapsesNode(){
        val dir = IndexTreeListJava.dirEmpty();
        val store =StoreTrivial()
        val root = store.put(dir, IndexTreeListJava.dirSer)

        assertEquals(1, store.getAllRecids().asSequence().count())

        //single element without expansion
        IndexTreeListJava.treePut(4,root, store,3, 1L, 111L)
        assertEquals(1, store.getAllRecids().asSequence().count())

        //extra element near will expand all four levels
        IndexTreeListJava.treePut(4,root, store,3, 2L, 222L)
        assertEquals(4, store.getAllRecids().asSequence().count())

        //remove element, that should collapse nodes
        IndexTreeListJava.treeRemoveCollapsing(4,root,store, 3, true, 2L, null)
        assertEquals(1, store.getAllRecids().asSequence().count())

    }


}