package org.mapdb.tree

import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap
import org.junit.Assert.*
import org.junit.Ignore
import org.junit.Test
import org.mapdb.TT
import org.mapdb.store.StoreDirect
import org.mapdb.store.StoreOnHeap
import org.mapdb.store.StoreTrivial
import org.mapdb.tree.IndexTreeListJava.*
import java.util.*

class IndexTreeListTest{
//
//    @Test fun putGet(){
//        val l = IndexTreeList<Long>(maxSize = 1000)
//
//        assertNull(l[1])
//        l[1] = 2L
//        assertEquals(2L, l[1])
//    }
//
//    @Test fun hugeSize(){
//        val l = IndexTreeList<Long>(maxSize = Long.MAX_VALUE)
//
//        assertNull(l[1])
//        l.set(Long.MAX_VALUE-1, 2L)
//        assertEquals(2L, l.get(Long.MAX_VALUE-1))
//
//    }

    val dirShift = 6;

    @Test fun dirTest(){
        val max = 127

        var dir = IndexTreeListJava.dirEmpty()
        assertArrayEquals(longArrayOf(0L,0L), dir)
        for(slot in 0 .. max){
            assertEquals(-2, IndexTreeListJava.dirOffsetFromSlot(dir, slot))
        }

        for(i in 0 ..max step 10)
            dir = IndexTreeListJava.dirPut(dir,i, i*10L, i*100L)

        for(i in 0 ..max step 10){
            assertEquals(i*10L, dir[2+2*i/10])
            assertEquals(i*100L, dir[2+2*i/10+1])
        }
        assertEquals(2+13*2, dir.size)

        for(i in 0 .. max){
            val pos = IndexTreeListJava.dirOffsetFromSlot(dir,i)
            if(i%10!=0) {
                assertTrue(pos < 0)
                continue
            }
            assertEquals(10L*i, dir[pos])
            assertEquals(100L*i, dir[pos+1])
        }

        //start deleting stuff
        for(i in 0 ..max step 10) {
            val size = dir.size
            dir = IndexTreeListJava.dirRemove(dir, i)
            assertEquals(size-2, dir.size)
            assertEquals(-2, IndexTreeListJava.dirOffsetFromSlot(dir,i))
        }
        assertArrayEquals(longArrayOf(0L,0L), dir)

    }

    @Test fun dirSer(){
        val max = 127
        var dir = IndexTreeListJava.dirEmpty()
        for(i in 0 ..max step 10) {
            assertArrayEquals(dir, TT.clone(dir, IndexTreeListJava.dirSer))
            dir = IndexTreeListJava.dirPut(dir, i, i * 10L, i * 100L)
        }
        assertArrayEquals(dir, TT.clone(dir, IndexTreeListJava.dirSer))

    }


//TODO zero value is allowed, check in iterations etc
    @Test fun treeGet(){

        for(binary in TT.bools) {
            //and point store to it
            val valRecid = 1111L
            val index = 5L;
            val level = 1;
            val dir = dirPut(dirEmpty(), treePos(dirShift, level, index), valRecid, index + 1)

            val store = if(binary) StoreDirect.make() else StoreTrivial();
            val recid = store.put(dir, dirSer)

            assertEquals(valRecid, treeGet(dirShift, recid, store, level, index))
            assertEquals(0, treeGet(dirShift, recid, store, 2, 1.shl(dirShift) + index))
        }
    }

    @Test fun treeGetNullable(){

        for(binary in TT.bools) {
            //and point store to it
            val valRecid = 1111L
            val index = 5L;
            val level = 1;
            val dir = dirPut(dirEmpty(), treePos(dirShift, level, index), valRecid, index + 1)

            val store = if(binary) StoreDirect.make() else StoreTrivial();
            val recid = store.put(dir, dirSer)

            assertEquals(valRecid, treeGetNullable(dirShift, recid, store, level, index))
            assertEquals(null, treeGetNullable(dirShift, recid, store, 2, 1.shl(dirShift) + index))
        }
    }

    @Test fun treePut(){
        val dir = dirEmpty()
        val s = StoreTrivial()

        val rootRecid = s.put(dir, dirSer)
        treePut(dirShift, rootRecid, s, 2, 0L, 11L)
        assertEquals(11L, treeGet(dirShift, rootRecid, s, 2, 0L))

        treePut(dirShift, rootRecid, s, 2, 2L, 1111L)
        assertEquals(1111L, treeGet(dirShift, rootRecid, s, 2, 2L))
        treePut(dirShift, rootRecid, s, 2, 3L, 2222L)
        assertEquals(1111L, treeGet(dirShift, rootRecid, s, 2, 2L))
        assertEquals(2222L, treeGet(dirShift, rootRecid, s, 2, 3L))
    }


    @Test
    fun treeRandom(){
        if(TT.shortTest())
            return

        val levels = 3;
        val maxIndex = 1.shl(dirShift*levels)
        val ref = LongLongHashMap()
        val s = StoreOnHeap(isThreadSafe = false) //minimize deserialization
        val root = s.put(IndexTreeListJava.dirEmpty(), dirSer);

        fun compareContents(){
            ref.forEachKeyValue { index, value ->
                val value2 = treeGet(dirShift, root, s, levels, index)
                if(value!=value2)
                    throw AssertionError(index);
            }

            //do iteration
            var i = treeIter(dirShift, root, s, levels, 0L);
            while(i!=null){
                val index = i[0];
                val value = i[1];
                if(value!=ref.get(index))
                    throw AssertionError()
                i = treeIter(dirShift, root, s, levels, index+1)
            }

            //do traverse
            val ref2 = LongLongHashMap();
            val count = treeFold(root, s, levels, 0) { k:Long, v:Long, c:Int->
                ref2.put(k,v)
                c+1
            }

            assertEquals(ref.size(), count)
            assertEquals(ref, ref2)
        }

        val r = Random(1)

        //do inserts
        for(i in 0..maxIndex*3){
            val index =r.nextInt(maxIndex).toLong()
            val value = r.nextLong();

            ref.put(index,value)
            treePut(dirShift, root, s, levels, index, value)

            if(i%10000==0){
                compareContents()
            }
        }
        compareContents()

        //do some deletes
        for(i in 0..maxIndex*3){

            val index =r.nextInt(maxIndex).toLong()
            if(index==0L)
                continue;

            treeRemove(dirShift, root, s, levels, index, null);
            if(treeGet(dirShift, root, s, levels, index)!=0L)
                throw AssertionError()
            ref.remove(index);
            if(i%10000==0){
                compareContents()
            }
        }
        compareContents()
    }

    @Test fun iter(){
        val dir = IndexTreeListJava.dirEmpty()
        val s = StoreTrivial()

        val rootRecid = s.put(dir, dirSer)
        treePut(dirShift, rootRecid, s, 2, 2L, 1111L)
        treePut(dirShift, rootRecid, s, 2, 3L, 2222L)

        treePut(dirShift, rootRecid, s, 2, 400L, 5555L)
        treePut(dirShift, rootRecid, s, 2, 1000L, 777L)


        assertArrayEquals(longArrayOf(2L,1111L), treeIter(dirShift, rootRecid, s,2, 0L))
        assertArrayEquals(longArrayOf(3L,2222L), treeIter(dirShift, rootRecid, s,2, 2L+1))
        assertArrayEquals(longArrayOf(400L,5555L), treeIter(dirShift, rootRecid, s,2, 3L+1))
        assertArrayEquals(longArrayOf(1000L,777L), treeIter(dirShift, rootRecid, s,2, 400L+1))
        assertNull(treeIter(dirShift, rootRecid, s,2, 1000L+1))
    }

    @Test fun constants(){
        assertEquals(64, java.lang.Long.bitCount(full))
        assertEquals(7, maxDirShift)

    }

    @Test fun dirOffsetFromLong(){
        //first bitmap
        for(pos in 0 until 64)
        for(first in 0L..1){
            if(pos==0 && first==1L)
                continue
            val bitmap1 = first.or(1L.shl(pos))
            val v = 2+first.toInt()*2;
            assertEquals(v, IndexTreeListJava.dirOffsetFromLong(bitmap1, 0L, pos))
            if(pos<63){
                assertEquals(-v-2, IndexTreeListJava.dirOffsetFromLong(bitmap1, 0, pos+1))
            }
        }

        //second bitmap
        for(pos in 0 until 64)
        for(first in 0L..1)
        for(last in 0L..1)
        for(first2 in 0L..1){
            if(pos==0 && first2==1L)
                continue
            val bitmap1 = first + last.shl(63)
            val bitmap2 = first2.or(1L.shl(pos))
            val v = 2+first.toInt()*2+last.toInt()*2+first2.toInt()*2
            assertEquals(v,
                    IndexTreeListJava.dirOffsetFromLong(bitmap1, bitmap2, 64+pos))

            if(pos<63){
                assertEquals(-v-2, IndexTreeListJava.dirOffsetFromLong(bitmap1, bitmap2, 64+pos+1))
            }
        }
    }

    @Test fun treeClear(){
        val store = StoreTrivial()
        assertFalse(store.getAllRecids().hasNext())

        val dirShift = 4
        val levels = 4

        //create tree
        val rootRecid = store.put(IndexTreeListJava.dirEmpty(), IndexTreeListJava.dirSer)

        for(index in 1L .. 10000 step 10){
            IndexTreeListJava.treePut(dirShift, rootRecid, store, levels, index, index*10)
        }

        val recidCountBefore = store.getAllRecids().asSequence().count()
        assertTrue(recidCountBefore>1)

        IndexTreeListJava.treeClear(rootRecid, store, levels)

        //make sure only root is left
        assertEquals(1, store.getAllRecids().asSequence().count())
        val root = store.get(rootRecid, IndexTreeListJava.dirSer)
        assertArrayEquals(root, IndexTreeListJava.dirEmpty())
    }



}