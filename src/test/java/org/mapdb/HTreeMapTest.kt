@file:Suppress("CAST_NEVER_SUCCEEDS")

package org.mapdb

import org.fest.reflect.core.Reflection
import org.junit.Assert.*
import org.junit.Test
import org.mapdb.volume.SingleByteArrayVol
import java.io.Closeable
import java.io.Serializable
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class HTreeMapTest{

    val HTreeMap<*,*>.leafSerializer: Serializer<Array<Any>>
        get() = Reflection.method("getLeafSerializer").`in`(this).invoke() as Serializer<Array<Any>>

    val HTreeMap<*,*>.locks:  Utils.SingleEntryReadWriteSegmentedLock?
        get() = Reflection.method("getLocks").`in`(this).invoke() as  Utils.SingleEntryReadWriteSegmentedLock?


    fun HTreeMap<*,*>.hashToSegment(h: Int): Int =
            Reflection.method("hashToSegment")
                    .withParameterTypes(h.javaClass)
                    .`in`(this)
                    .invoke(h) as Int


    fun HTreeMap<*,*>.hash(o: Any): Int =
            Reflection.method("hash")
                    .withParameterTypes(Any::class.java)
                    .`in`(this)
                    .invoke(o) as Int


    @Test fun hashAssertion(){
        @Suppress("UNCHECKED_CAST")
        val map = HTreeMap.make<ByteArray,Int>(keySerializer = Serializer.ELSA as Serializer<ByteArray>)

        try {
            for (i in 1..100)
                map.put(TT.randomByteArray(10), 11)
            fail("hash exception expected")
        }catch(e:IllegalArgumentException){
            assertTrue(e.message!!.contains("hash"))
        }

        val map2 = HTreeMap.make<Any,Int>(keySerializer = Serializer.ELSA,
                stores = arrayOf(StoreOnHeap()), concShift = 0)

        class NotSerializable{

        }
        map2.put(NotSerializable(), 11)
    }


    @Test fun valueCreator(){
        val map = HTreeMap.make<Int,Int>(valueLoader ={it+10})
        assertEquals(11, map[1])
        assertEquals(1, map.size)
    }

    @Test fun close(){
        var closed = false
        val closeable = object: Closeable {
            override fun close() {
                closed = true
            }
        }
        val map = HTreeMap.make<Long,Long>(closeable =closeable)
        assertFalse(closed)
        map.close()
        assertTrue(closed)

    }

    @Test fun test_hash_collision() {
        val m = HTreeMap.make(keySerializer = HTreeMap_GuavaTest.singleHashSerializer, valueSerializer = Serializer.INTEGER, concShift = 0)

        for (i in 0..19) {
            m.put(i, i + 100)
        }

        for (i in 0..19) {
            assertTrue(m.containsKey(i))
            assertEquals(i + 100, m[i])
        }

        m.put(11, 1111)
        assertEquals(1111, m[11])

        //everything in single linked leaf
        val leafRecid = m.indexTrees[0].values().longIterator().next()

        val leaf = m.stores[0].get(leafRecid, m.leafSerializer)
        assertEquals(3*20, leaf!!.size)
    }

    @Test fun delete_removes_recids(){
        val m = HTreeMap.make(keySerializer = HTreeMap_GuavaTest.singleHashSerializer, valueSerializer = Serializer.INTEGER, concShift = 0)

        fun countRecids() = m.stores[0].getAllRecids().asSequence().count()

        assertEquals(1, countRecids())
        m.put(1,1)
        assertEquals(1+2, countRecids())

        m.put(2,2)
        assertEquals(1+2+1, countRecids())
        m.put(2,3)
        assertEquals(1+2+1, countRecids())
        m.remove(2)
        assertEquals(1+2, countRecids())
        m.remove(1)
        assertEquals(1, countRecids())
    }

    @Test fun delete_removes_recids_dir_collapse(){
        val sequentialHashSerializer = object :Serializer<Int>{
            override fun deserialize(input: DataInput2, available: Int): Int? {
                return input.readInt()
            }

            override fun serialize(out: DataOutput2, value: Int) {
                out.writeInt(value)
            }

            override fun hashCode(a: Int, seed: Int): Int {
                return a
            }
        }

        val m = HTreeMap.make(keySerializer = sequentialHashSerializer, valueSerializer = Serializer.INTEGER, concShift = 0)

        fun countRecids() = m.stores[0].getAllRecids().asSequence().count()

        assertEquals(1, countRecids())
        m.put(1,1)

        assertEquals(1+2, countRecids())

        m.put(2,2)
        assertEquals(9, countRecids())
        m.put(2,3)
        assertEquals(9, countRecids())
        m.remove(2)
        assertEquals(1+2, countRecids())
        m.remove(1)
        assertEquals(1, countRecids())
    }

    @Test fun clear(){
        val m = HTreeMap.make(keySerializer = Serializer.INTEGER, valueSerializer = Serializer.INTEGER)
        val recidCount = m.stores[0].getAllRecids().asSequence().count()
        for(i in 1 .. 10000)
            m.put(i, i);
        m.clear()
        assertEquals(recidCount, m.stores[0].getAllRecids().asSequence().count())
    }


    @Test(timeout = 20000)
    fun cache_load_time_expire() {
        if (TT.shortTest())
            return

        val db = DBMaker.memoryDB().make()

        val m = db.hashMap("test", Serializer.LONG, Serializer.LONG)
                .expireAfterUpdate(100).expireAfterCreate(100).create()
        val time = System.currentTimeMillis()
        var counter: Long = 0
        while (time + 5000 > System.currentTimeMillis()) {
            m.put(counter++, counter++)
        }
        m.clear()
    }

    @Test(timeout = 20000)
    fun cache_load_size_expire() {
        if (TT.shortTest())
            return

        val db = DBMaker.memoryDB().make()

        val m = db.hashMap("test", Serializer.LONG, Serializer.LONG).expireMaxSize(10000).create()
        val time = System.currentTimeMillis()
        var counter: Long = 0
        while (time + 5000 > System.currentTimeMillis()) {
            m.put(counter++, counter++)
            //            if(counter%1000<2) System.out.println(m.size());
        }
        m.clear()
    }


    @Test fun hasher() {
        val m = DBMaker.memoryDB().make()
                .hashMap("test", Serializer.INT_ARRAY, Serializer.INTEGER).create()


        var i = 0
        while (i < 1e5){
            m.put(intArrayOf(i, i, i), i)
            i++
        }

        i = 0
        while (i < 1e5){
            assertEquals(i, m.get(intArrayOf(i, i, i)))
            i++
        }

    }


    @Test fun mod_listener_lock2() {
        val db = DBMaker.memoryDB().make()
        val counter = AtomicInteger()
        var m:HTreeMap<String,String>? = null
        var seg:Int? = null
        m = db.hashMap("name", Serializer.STRING, Serializer.STRING)
            .modificationListener(MapModificationListener { _, _, _, _ ->
                for (i in 0..m!!.locks!!.segmentCount - 1) {
                    assertEquals(seg == i,
                            m!!.locks!!.isWriteLockedByCurrentThread(i))
                }
                counter.incrementAndGet()
            })
            .create()

        seg = m.hashToSegment(m.hash("aa"))

        m.put("aa", "aa")
        m.put("aa", "bb")
        m.remove("aa")

        m.put("aa", "aa")
        m.remove("aa", "aa")
        m.putIfAbsent("aa", "bb")
        m.replace("aa", "bb", "cc")
        m.replace("aa", "cc")

        assertEquals(8, counter.get().toLong())
    }

    @Test
    fun test_iterate_and_remove() {
        val max = 1e5.toInt()

        val m = DBMaker.memoryDB().make().hashSet("test", Serializer.INTEGER).create()

        for (i in 0..max - 1) {
            m.add(i)
        }


        val control = HashSet<Int?>()
        val iter = m.iterator()

        for (i in 0..max / 2 - 1) {
            assertTrue(iter.hasNext())
            control.add(iter.next())
        }

        m.clear()

        while (iter.hasNext()) {
            control.add(iter.next())
        }

    }

    /*
        Hi jan,

        Today i found another problem.

        my code is

        HTreeMap<Object, Object>  map = db.createHashMap("cache").expireMaxSize(MAX_ITEM_SIZE).counterEnable()
                .expireAfterWrite(EXPIRE_TIME, TimeUnit.SECONDS).expireStoreSize(MAX_GB_SIZE).make();

        i set EXPIRE_TIME = 216000

        but the data was expired right now,the expire time is not 216000s, it seems there is a bug for expireAfterWrite.

        if i call expireAfterAccess ,everything seems ok.

    */
    @Test(timeout = 100000)
    @Throws(InterruptedException::class)
    fun expireAfterWrite() {
        if (TT.shortTest())
            return
        //NOTE this test has race condition and may fail under heavy load.
        //TODO increase timeout and move into integration tests.

        val db = DBMaker.memoryDB().make()

        val MAX_ITEM_SIZE = 1e7.toLong()
        val EXPIRE_TIME = 3L
        val MAX_GB_SIZE = 1e7.toLong()

        val m = db.hashMap("cache", Serializer.INTEGER, Serializer.INTEGER)
                .expireMaxSize(MAX_ITEM_SIZE).counterEnable()
                .expireAfterCreate(EXPIRE_TIME, TimeUnit.SECONDS)
                .expireAfterUpdate(EXPIRE_TIME, TimeUnit.SECONDS)
                .expireStoreSize(MAX_GB_SIZE).create()

        for (i in 0..999) {
            m.put(i, i)
        }
        Thread.sleep(2000)

        for (i in 0..499) {
            m.put(i, i + 1)
        }
        //wait until size is 1000
        while (m.size != 1000) {
            m[2348294] //so internal tasks have change to run
            Thread.sleep(10)
        }

        Thread.sleep(2000)

        //wait until size is 1000
        while (m.size != 500) {
            m.expireEvict()
            Thread.sleep(10)
        }
    }


    class AA(internal val vv: Int) : Serializable {

        override fun equals(other: Any?): Boolean {
            return other is AA && other.vv == vv
        }
    }


    @Test(expected = IllegalArgumentException::class)
    fun inconsistentHash() {
        val db = DBMaker.memoryDB().make()

        val m = db.hashMap("test", Serializer.ELSA, Serializer.INTEGER).create()

        var i = 0
        while (i < 1e50){
            m.put(AA(i), i)
            i++
        }
    }

    @Test fun continous_expiration(){
        val size = 128 * 1024*1024
        val volume = SingleByteArrayVol(size)
        val db = DBMaker.volumeDB(volume, false).make()
        val map = db
            .hashMap("map", Serializer.LONG, Serializer.BYTE_ARRAY)
            .expireAfterCreate()
            .expireStoreSize((size*0.7).toLong())
            .expireExecutor(TT.executor())
            .expireExecutorPeriod(100)
            .expireCompactThreshold(0.5)
            .create()

        val t = TT.nowPlusMinutes(10.0)
        var key = 0L
        val random = Random()
        while(t>System.currentTimeMillis()){
            map.put(key, ByteArray(random.nextInt(32000)))
        }

        db.close()
    }


    @Test fun mod_listener_lock() {
        val db = DBMaker.memoryDB().make()
        val counter = AtomicInteger()
        var m:HTreeMap<String,String>? = null;
        m = db.hashMap("name", Serializer.STRING, Serializer.STRING)
                .modificationListener(object : MapModificationListener<String,String> {
                    override fun modify(key: String, oldValue: String?, newValue: String?, triggered: Boolean) {
                        val segment = m!!.hashToSegment(m!!.hash(key))
                        m!!.locks!!.checkWriteLocked(segment)
                        counter.incrementAndGet()
                    }
                })
                .create()
        m.put("aa", "aa")
        m.put("aa", "bb")
        m.remove("aa")

        m.put("aa", "aa")
        m.remove("aa", "aa")
        m.putIfAbsent("aa", "bb")
        m.replace("aa", "bb", "cc")
        m.replace("aa", "cc")

        assertEquals(8, counter.get())
    }

    @Test fun calculateCollisions(){
        val map = DBMaker.heapDB().make().hashMap("name", Serializer.LONG, Serializer.LONG).createOrOpen()
        for(i in 0L until 1000)
            map[i] = i
        val (collision, size) = map.calculateCollisionSize()
        assertEquals(0, collision)
        assertEquals(1000, size)
    }

    @Test fun calculateCollisions2(){
        val ser2 = object: Serializer<Long> by Serializer.LONG{
            override fun hashCode(a: Long, seed: Int): Int {
                return 0
            }
        }

        val map = DBMaker.heapDB().make().hashMap("name", ser2, Serializer.LONG).createOrOpen()
        for(i in 0L until 1000)
            map[i] = i
        val (collision, size) = map.calculateCollisionSize()
        assertEquals(999, collision)
        assertEquals(1000, size)
    }

    @Test fun key_iterator_does_not_deserialize_external_values(){
        var keyDeserCount = 0
        var stopValDeser = false

        val keyser = object: Serializer<Int> by Serializer.INTEGER{
            override fun deserialize(input: DataInput2, available: Int): Int {
                keyDeserCount++
                return input.readInt()
            }
        }


        val valser = object: Serializer<Int> by Serializer.INTEGER{
            override fun deserialize(input: DataInput2, available: Int): Int {
                assert(!stopValDeser)
                return input.readInt()
            }
        }

        val map = DBMaker
                .memoryDB()
                .make()
                .hashMap("test",keyser, valser)
                .create()
        for(i in 0 until 10000) {
            map.put(i, i)
        }

        keyDeserCount=0
        stopValDeser = true
        for(k in map.keys){
        }
        assertEquals(10000, keyDeserCount )
    }




}