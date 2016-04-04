package org.mapdb

import org.junit.Assert.*
import org.junit.Test


class DBMakerTest{

    @Test fun sharded_htreemap_close(){
        val executor = TT.executor()

        val map = DBMaker.heapShardedHashMap(8).expireExecutor(executor).expireAfterCreate(100).create()
        assertTrue(executor.isShutdown.not())
        map.close()
        assertTrue(executor.isShutdown)
        assertTrue(executor.isTerminated)
    }

    @Test fun conc_scale(){
        val db =DBMaker.memoryDB().concurrencyScale(32).make()
        assertEquals(DataIO.shift(32), (db.store as StoreDirect).concShift)
    }


    @Test fun conc_disable(){
        var db =DBMaker.memoryDB().make()
        assertTrue(db.isThreadSafe)
        assertTrue(db.store.isThreadSafe)
        assertTrue(db.hashMap("aa1").create().threadSafe)
        assertTrue(db.treeMap("aa2").create().threadSafe)

        db =DBMaker.memoryDB().concurrencyDisable().make()
        assertFalse(db.isThreadSafe)
        assertFalse(db.store.isThreadSafe)
        assertFalse(db.hashMap("aa1").create().threadSafe)
        assertFalse(db.treeMap("aa2").create().threadSafe)
    }
}