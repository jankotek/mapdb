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
}