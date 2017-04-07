package org.mapdb

import org.fest.reflect.core.Reflection
import org.junit.Assert.*
import org.junit.Test
import org.mapdb.volume.SingleByteArrayVol
import java.util.*

class HTreeMapExpirationTest {


    val HTreeMap<*,*>.isForegroundEviction: Boolean
        get() = Reflection.field("isForegroundEviction").ofType(Boolean::class.java).`in`(this).get()


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
        assertFalse(map.isForegroundEviction)

        //wait a bit, they should be removed
        while(map.isEmpty().not())
            Thread.sleep(100)

        map.expireExecutor!!.shutdown()
    }

    @Test(timeout = 100000)
    fun maxSize(){
        val map = DBMaker.memoryDB().make()
                .hashMap("aa", Serializer.INTEGER, Serializer.INTEGER)
                .expireAfterCreate()
                .expireMaxSize(1000)
                .create()

        maxSizeTest(map)
    }

    @Test(timeout = 100000)
    fun maxSizeSingleSeg(){
        val map = DBMaker.memoryDB().make()
                .hashMap("aa", Serializer.INTEGER, Serializer.INTEGER)
                .expireAfterCreate()
                .expireMaxSize(1000)
                .layout(0, 1.shl(4),4)
                .create()

        maxSizeTest(map)
    }


    fun maxSizeTest(map:HTreeMap<Int,Int>) {
        assertTrue(map.expireCreateQueues != null)

        for (i in 0 until 10000) {
            map.put(i, i)
            val size = map.size

            assertTrue(size < 1100)
            if (i > 10000)
                assertTrue(size > 900)
            map.forEachKey { assertTrue(it > i - 1100) }
        }
    }

    @Test fun expireStoreSize(){
        if(TT.shortTest())
            return

        val volume = SingleByteArrayVol(1024 * 1024 * 500)

        val db = DBMaker
                .volumeDB(volume,false)
                .make()

        val map = db
                .hashMap("map", Serializer.LONG, Serializer.BYTE_ARRAY)
                .counterEnable()
                .layout(0, 8,4)
                .expireAfterCreate()
                .expireStoreSize(1024*1024*400)
                .create()

        val max = 1000000
        for(i in 0L .. max){
            map.put(i, ByteArray(1024))
        }
        assertTrue(map.size < max)
        assertTrue(map.size > 1000)
    }


    /** data should not be expireable until updated */
    @Test fun storeSize_updateTTL(){
        if(TT.shortTest())
            return

        val db = DBMaker.memoryDB().make()
        val map = db
                .hashMap("map", Serializer.INTEGER, Serializer.BYTE_ARRAY)
                .counterEnable()
                .layout(0, 8,4)
                .expireAfterUpdate(5000)
                .expireStoreSize(1024*1024*20)
                .create()

        //fill over rim
        val keyCount = 30*1024
        for(key in 0 until keyCount)
            map.put(key, ByteArray(1024))

        //wait and verify no entries were removed
        Thread.sleep(15000)
        map.expireEvict()
        assertEquals(keyCount,map.size)

        //update 2/3 entries
        for(key in 0 until keyCount*2/3)
            map.put(key, ByteArray(1023))

        //some entries should expire immediately, to free space
        map.expireEvict()
        assertTrue(map.size>keyCount/3 && map.size<keyCount*4/5)

        //now wait for time based eviction, it should remove all updated entries
        Thread.sleep(15000)
        map.expireEvict()
        assertEquals(keyCount - keyCount*2/3, map.size)
    }


    /** data should not be expireable until updated */
    @Test fun mapSize_updateTTL(){
        if(TT.shortTest())
            return

        val db = DBMaker.memoryDB().make()
        val map = db
                .hashMap("map", Serializer.INTEGER, Serializer.BYTE_ARRAY)
                .counterEnable()
                .layout(0, 8,4)
                .expireAfterUpdate(5000)
                .expireMaxSize(1000)
                .create()

        //fill over rim
        for(key in 0 until 2000)
            map.put(key, ByteArray(102))

        //wait and verify no entries were removed
        Thread.sleep(15000)
        map.expireEvict()
        assertEquals(2000,map.size)

        //update 2/3 entries
        for(key in 0 until 2000*2/3)
            map.put(key, ByteArray(103))

        //some entries should expire immediately, to free space
        map.expireEvict()
        assertEquals(1000, map.size)

        //now wait for time based eviction, it should remove all updated entries
        Thread.sleep(15000)
        map.expireEvict()
        assertEquals(2000 - 2000*2/3, map.size)
    }

    @Test fun storeSize_external_change_displace(){
        if(TT.shortTest())
            return

        val db = DBMaker.memoryDB().make()
        val map = db
                .hashMap("map", Serializer.INTEGER, Serializer.BYTE_ARRAY)
                .counterEnable()
                .layout(0, 8,4)
                .expireAfterCreate()
                .expireStoreSize(1024*1024*20)
                .create()

        //fill 10MB
        for(key in 0 until 1024*10)
            map.put(key, ByteArray(1024))

        // no entries should be evicted, there is enough space
        map.expireEvict()
        assertEquals(1024*10, map.size)

        //insert 15MB into store, that should displace some entries
        db.store.put(ByteArray(1024*1024*15), Serializer.BYTE_ARRAY)
        map.expireEvict()
        assertTrue(map.size>0)
        assertTrue(map.size<1024*10)

        //insert another 15MB, map will become empty
        db.store.put(ByteArray(1024*1024*15), Serializer.BYTE_ARRAY)
        map.expireEvict()
        assertEquals(0, map.size)
    }


    @Test(timeout = 20000L)
    @Throws(InterruptedException::class)
    fun expiration_overflow() {
        if (TT.shortTest())
            return
        val db = DBMaker.memoryDB().make()

        val ondisk = db.hashMap("onDisk",Serializer.INTEGER,Serializer.STRING).create()

        val inmemory = db.hashMap("inmemory",Serializer.INTEGER,Serializer.STRING)
                .expireAfterCreate(1000)
                .expireExecutor(TT.executor())
                .expireExecutorPeriod(300)
                .expireOverflow(ondisk)
                .create()

        //fill on disk, inmemory should stay empty
        for (i in 0..999) {
            ondisk.put(i, "aa" + i)
        }

        assertEquals(1000, ondisk.size.toLong())
        assertEquals(0, inmemory.size.toLong())

        //add stuff inmemory, ondisk should stay unchanged, until executor kicks in
        for (i in 1000..1099) {
            inmemory.put(i, "aa" + i)
        }
        assertEquals(1000, ondisk.size.toLong())
        assertEquals(100, inmemory.size.toLong())

        //wait until executor kicks in
        while (!inmemory.isEmpty()) {
            Thread.sleep(100)
        }

        //stuff should be moved to indisk
        assertEquals(1100, ondisk.size.toLong())
        assertEquals(0, inmemory.size.toLong())

        //if value is not found in-memory it should get value from on-disk
        assertEquals("aa111", inmemory.get(111))
        assertEquals(1, inmemory.size.toLong())
    }

    @Test fun issue538_overflow_NPE1() {
        val db = DBMaker.memoryDB().make()
        val m2 = db.hashMap("m2", Serializer.STRING,Serializer.LONG).create()
        val m = db.hashMap("m", Serializer.STRING,Serializer.LONG)
                .expireOverflow(m2).create()

        assertNull(m["nonExistent"])
    }


    @Test fun issue538_overflow_NPE2() {
        val db = DBMaker.memoryDB().make()
        val m2 = db.hashMap("m2", Serializer.STRING,Serializer.LONG).create()
        val m = db.hashMap("m", Serializer.STRING,Serializer.LONG)
                .expireOverflow(m2).create()

        assertNull(m["nonExistent"])
    }


    @Test fun clear_moves_to_overflow(){
        val db = DBMaker.heapDB().make()

        val map2 = HashMap<Int,Int?>()
        val map1 = db
                .hashMap("map", Serializer.INTEGER, Serializer.INTEGER)
                .expireAfterCreate(1000000)
                .expireOverflow(map2)
                .createOrOpen()

        for(i in 0 until 1000)
            map1.put(i,i)

        //clear first map should move all stuff into secondary
        map1.clearWithExpire()
        assertEquals(0, map1.size)
        assertEquals(1000, map2.size)
    }

}