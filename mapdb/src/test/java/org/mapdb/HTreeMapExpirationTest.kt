package org.mapdb

import org.junit.Test
import org.junit.Assert.*
import java.util.*

class HTreeMapExpirationTest {


    @Test(timeout = 10000)
    fun expire_create() {
        val map: HTreeMap<Int, String> = HTreeMap.make(expireCreateTTL = 1000, concShift = 0)

        assertEquals(0, map.expireCreateQueues!![0].size())
        map.put(1, "aa")
        assertEquals(1, map.expireCreateQueues!![0].size())
        Thread.sleep(map.expireCreateTTL / 2)
        map.put(2, "bb")
        assertEquals(2, map.expireCreateQueues!![0].size())

        while (map[1] != null) {
            map.expireEvict()
            Thread.sleep(1)
        }
        assertEquals("bb", map[2])
        assertEquals(1, map.expireCreateQueues!![0].size())

        while (map[2] != null) {
            map.expireEvict()
            Thread.sleep(1)
        }
        assertEquals(0, map.expireCreateQueues!![0].size())
    }

    @Test(timeout = 10000)
    fun expire_update() {
        val map: HTreeMap<Int, String> = HTreeMap.make(expireUpdateTTL = 1000, concShift = 0)

        assertEquals(0, map.expireUpdateQueues!![0].size())
        map.put(1, "aa")
        map.put(1, "zz")
        assertEquals(1, map.expireUpdateQueues!![0].size())
        Thread.sleep(map.expireCreateTTL / 2)
        map.put(2, "bb")
        assertEquals(1, map.expireUpdateQueues!![0].size())

        while (map[1] != null) {
            map.expireEvict()
            Thread.sleep(1)
        }
        assertEquals("bb", map[2])
        assertEquals(0, map.expireUpdateQueues!![0].size())
    }

    @Test(timeout = 10000)
    fun expire_get() {
        val map: HTreeMap<Int, String> = HTreeMap.make(expireGetTTL = 1000, concShift = 0)

        map.put(1, "aa")
        map.put(2, "bb")
        assertEquals(0, map.expireGetQueues!![0].size())
        map[1]
        assertEquals(1, map.expireGetQueues!![0].size())
        Thread.sleep(3000)
        map.get(3) //run eviction stuff
        assertEquals(0, map.expireGetQueues!![0].size())

        assertEquals(null, map[1])
        assertEquals("bb", map[2])
    }

    @Test (timeout = 10000)
    fun instant_create() {
        val map: HTreeMap<Int, String> = HTreeMap.make(expireCreateTTL = 1, concShift = 0)
        map.put(1, "aa")
        Thread.sleep(100)
        map.expireEvict()
        assertNull(map[1]);
    }


    @Test(timeout = 10000)
    fun instant_update() {
        val map: HTreeMap<Int, String> = HTreeMap.make(expireUpdateTTL = 1, concShift = 0)

        assertEquals(0, map.expireUpdateQueues!![0].size())
        map.put(1, "aa")
        assertEquals("aa", map[1])
        map.put(1, "zz")
        map.put(2, "bb")
        Thread.sleep(100)
        map.expireEvict()

        assertEquals(null, map[1])
        assertEquals("bb", map[2])
    }

    @Test (timeout = 10000)
    fun instant_get() {
        val map: HTreeMap<Int, String> = HTreeMap.make(expireGetTTL = 1, concShift = 0)
        map.put(1, "aa")
        assertEquals("aa", map[1])
        Thread.sleep(100)
        map.expireEvict()

        assertNull(map[1]);
    }

    @Test (timeout = 100000)
    fun concurrentExpire() {
        val map: HTreeMap<Int, Int> = HTreeMap.make(expireCreateTTL = 300, concShift = 4,
                valueSerializer = Serializer.INTEGER, keySerializer = Serializer.INTEGER)

        val size = 10000
        TT.fork(16){
            val r = Random()
            for(i in 0 until size){
                map.put(i, r.nextInt())
            }
        }
        //everything will eventually expire
        while(!map.isEmpty()) {
            Thread.sleep(10)
            map.expireEvict()
        }
    }

    @Test (timeout = 100000)
    fun concurrentExpire_update() {
        val map: HTreeMap<Int, Int> = HTreeMap.make(expireUpdateTTL = 300, concShift = 4,
                valueSerializer = Serializer.INTEGER, keySerializer = Serializer.INTEGER)

        val size = 10000
        TT.fork(16){
            val r = Random()
            for(i in 0 until size){
                map.put(i, r.nextInt())
            }
        }
        //everything will eventually expire
        while(!map.isEmpty()) {
            Thread.sleep(10)
            map.expireEvict()
        }
    }

    @Test (timeout = 100000)
    fun concurrentExpire_get() {
        val map: HTreeMap<Int, Int> = HTreeMap.make(expireGetTTL = 300, concShift = 4,
                valueSerializer = Serializer.INTEGER, keySerializer = Serializer.INTEGER)

        val size = 10000
        TT.fork(16){
            val r = Random()
            for(i in 0 until size){
                map.put(i, r.nextInt())
                map[i]
            }
        }
        //everything will eventually expire
        while(!map.isEmpty()) {
            Thread.sleep(10)
            map.expireEvict()
        }
    }

    @Test (timeout = 10000)
    fun background_expiration(){
        val map = HTreeMap.make(expireCreateTTL = 300, concShift = 4,
                valueSerializer = Serializer.INTEGER, keySerializer = Serializer.INTEGER,
                expireExecutor = TT.executor(), expireExecutorPeriod = 100)

        for(i in 0 until 1000)
            map.put(i, i)
        //entries are still there
        assertFalse(map.isEmpty())

        //no expiration in user thread
        assertFalse(map.expireEvict)

        //wait a bit, they should be removed
        while(map.isEmpty().not())
            Thread.sleep(100)

        map.expireExecutor!!.shutdown()
    }

}