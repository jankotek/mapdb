package org.mapdb

import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet
import org.junit.Assert.*
import org.junit.Test
import org.mapdb.serializer.GroupSerializerObjectArray
import java.math.BigDecimal
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class DBTest{

    @Test fun store_consistent(){
        val store = StoreTrivial()
        val db = DB(store, storeOpened = false);
        val htreemap = db.hashMap("map", keySerializer = Serializer.LONG, valueSerializer = Serializer.LONG).create()
        assertTrue(store===db.store)
        htreemap.stores.forEach{
            assertTrue(store===it)
        }

        for(indexTree in htreemap.indexTrees)
            assertTrue(store===(indexTree as IndexTreeLongLongMap).store)
    }


    @Test fun name_catalog_with(){
        val db = DB(store=StoreTrivial(), storeOpened = false)

        db.lock.writeLock().lock()
        var nameCatalog = db.nameCatalogLoad()
        nameCatalog.put("aaa", "bbbb")
        db.nameCatalogSave(nameCatalog)

        nameCatalog = db.nameCatalogLoad()
        assertEquals(1, nameCatalog.size)
        assertEquals("bbbb",nameCatalog.get("aaa"))
    }

    @Test fun name_catalog_singleton(){
        val db = DB(store=StoreTrivial(), storeOpened = false)

        db.lock.writeLock().lock()
        var nameCatalog = db.nameCatalogLoad()
        db.nameCatalogPutClass(nameCatalog, "aaa", Serializer.BIG_DECIMAL)
        assertEquals(1, nameCatalog.size)
        assertEquals("org.mapdb.Serializer#BIG_DECIMAL", nameCatalog.get("aaa"))
        db.nameCatalogSave(nameCatalog)

        nameCatalog = db.nameCatalogLoad()

        val ser:Serializer<BigDecimal>? = db.nameCatalogGetClass(nameCatalog, "aaa")
        assertTrue(Serializer.BIG_DECIMAL===ser)
    }

    @Test fun hashMap_create_unresolvable_serializer(){
        val db = DB(store=StoreTrivial(), storeOpened = false)
        val unresolvable = object:Serializer<String>{
            override fun deserialize(input: DataInput2, available: Int): String? {
                throw UnsupportedOperationException()
            }

            override fun serialize(out: DataOutput2, value: String) {
                throw UnsupportedOperationException()
            }
        }
        val hashmap = db.hashMap("aa", Serializer.BIG_DECIMAL, unresolvable).create()

        assertEquals(Serializer.BIG_DECIMAL, hashmap.keySerializer)
        assertEquals(unresolvable, hashmap.valueSerializer)

        db.lock.writeLock().lock()
        val nameCatalog = db.nameCatalogLoad()
        assertTrue(2<nameCatalog.size)
        assertEquals("HashMap",nameCatalog["aa#type"])
        assertEquals("org.mapdb.Serializer#BIG_DECIMAL", nameCatalog["aa#keySerializer"])
    }

    @Test fun hashMap_Create(){
        val db = DB(store=StoreTrivial(), storeOpened = false)

        val hmap = db.hashMap("aa", Serializer.BIG_DECIMAL, Serializer.BOOLEAN)
                .valueInline()
                .layout(0, 8, 2)
                .hashSeed(1000)
                .expireAfterCreate(11L)
                .expireAfterUpdate(22L)
                .expireAfterGet(33L)
                .counterEnable()
                .removeCollapsesIndexTreeDisable()
                .create()

        db.lock.writeLock().lock()
        val p = db.nameCatalogParamsFor("aa")

        assertEquals(17, p.size)
        assertEquals(1,hmap.indexTrees.size)
        assertEquals((hmap.indexTrees[0] as IndexTreeLongLongMap).rootRecid.toString(), p["aa"+DB.Keys.rootRecids])
        assertEquals("HashMap", p["aa"+DB.Keys.type])
        assertEquals("org.mapdb.Serializer#BIG_DECIMAL", p["aa"+DB.Keys.keySerializer])
        assertEquals("org.mapdb.Serializer#BOOLEAN", p["aa"+DB.Keys.valueSerializer])
        assertEquals("true", p["aa"+DB.Keys.valueInline])
        assertTrue((hmap.indexTrees[0] as IndexTreeLongLongMap).collapseOnRemove.not())
        assertEquals("false", p["aa"+DB.Keys.removeCollapsesIndexTree])

        assertEquals("0", p["aa"+DB.Keys.concShift])
        assertEquals("2", p["aa"+DB.Keys.levels])
        assertEquals("3", p["aa"+DB.Keys.dirShift])
        assertEquals("1000", p["aa"+DB.Keys.hashSeed])
        assertEquals("11", p["aa"+DB.Keys.expireCreateTTL])
        assertEquals("22", p["aa"+DB.Keys.expireUpdateTTL])
        assertEquals("33", p["aa"+DB.Keys.expireGetTTL])

        fun qToString(q:QueueLong)=""+q.tailRecid+","+q.headRecid+","+q.headPrevRecid
        assertEquals(qToString(hmap.expireCreateQueues!![0]), p["aa"+DB.Keys.expireCreateQueues])
        assertEquals(qToString(hmap.expireUpdateQueues!![0]), p["aa"+DB.Keys.expireUpdateQueues])
        assertEquals(qToString(hmap.expireGetQueues!![0]), p["aa"+DB.Keys.expireGetQueues])

        assertEquals(1, hmap.counterRecids!!.size)
        assertTrue(p["aa"+DB.Keys.counterRecids]!!.toLong()>0)

    }


    @Test fun hashMap_Create_Default(){
        val db = DB(store=StoreTrivial(), storeOpened = false)

        val hmap = db.hashMap("aa")
                .create()
        db.lock.writeLock().lock()
        val p = db.nameCatalogParamsFor("aa")

        assertEquals(17, p.size)
        val rootRecids = hmap.indexTrees
                .map { (it as IndexTreeLongLongMap).rootRecid.toString()}
                .fold("",{str, it-> str+",$it"})

        assertEquals(8, Utils.identityCount(hmap.indexTrees))
        assertEquals(1, hmap.stores.toSet().size)
        assertEquals(rootRecids, ","+p["aa"+DB.Keys.rootRecids])
        assertEquals("HashMap", p["aa"+DB.Keys.type])
        assertEquals("org.mapdb.Serializer#JAVA", p["aa"+DB.Keys.keySerializer])
        assertEquals("org.mapdb.Serializer#JAVA", p["aa"+DB.Keys.valueSerializer])
        assertEquals("false", p["aa"+DB.Keys.valueInline])
        assertTrue((hmap.indexTrees[0] as IndexTreeLongLongMap).collapseOnRemove)
        assertEquals("true", p["aa"+DB.Keys.removeCollapsesIndexTree])


        assertEquals("3", p["aa"+DB.Keys.concShift])
        assertEquals("4", p["aa"+DB.Keys.levels])
        assertEquals("4", p["aa"+DB.Keys.dirShift])
        assertTrue(p["aa"+DB.Keys.hashSeed]!!.toInt() != 0)
        assertEquals("0", p["aa"+DB.Keys.expireCreateTTL])
        assertEquals("0", p["aa"+DB.Keys.expireUpdateTTL])
        assertEquals("0", p["aa"+DB.Keys.expireGetTTL])

        assertEquals("", p["aa"+DB.Keys.expireCreateQueues])
        assertEquals("", p["aa"+DB.Keys.expireUpdateQueues])
        assertEquals("", p["aa"+DB.Keys.expireGetQueues])

        assertEquals(null, hmap.counterRecids)
        assertEquals("", p["aa"+DB.Keys.counterRecids])


        hmap.stores.forEach{assertTrue(db.store===it)}
        hmap.indexTrees.forEach{assertTrue(db.store===(it as IndexTreeLongLongMap).store)}
    }

    @Test fun hashMap_Create_conc_expire(){
        val db = DB(store=StoreTrivial(), storeOpened = false)

        val hmap = db.hashMap("aa")
                .expireAfterCreate(10)
                .expireAfterUpdate(20)
                .expireAfterGet(30)
                .create()
        db.lock.writeLock().lock()
        val p = db.nameCatalogParamsFor("aa")

        assertEquals(17, p.size)
        assertEquals(8, hmap.indexTrees.size)
        assertEquals(8, Utils.identityCount(hmap.indexTrees))
        assertEquals(1, hmap.stores.toSet().size)

        val rootRecids = hmap.indexTrees
                .map { (it as IndexTreeLongLongMap).rootRecid.toString()}
                .fold("",{str, it-> str+",$it"})
        assertEquals(rootRecids, ","+p["aa"+DB.Keys.rootRecids])
        assertEquals("HashMap", p["aa"+DB.Keys.type])
        assertEquals("org.mapdb.Serializer#JAVA", p["aa"+DB.Keys.keySerializer])
        assertEquals("org.mapdb.Serializer#JAVA", p["aa"+DB.Keys.valueSerializer])
        assertEquals("3", p["aa"+DB.Keys.concShift])
        assertEquals("4", p["aa"+DB.Keys.levels])
        assertEquals("4", p["aa"+DB.Keys.dirShift])
        assertTrue(p["aa"+DB.Keys.hashSeed]!!.toInt() != 0)
        assertEquals("10", p["aa"+DB.Keys.expireCreateTTL])
        assertEquals("20", p["aa"+DB.Keys.expireUpdateTTL])
        assertEquals("30", p["aa"+DB.Keys.expireGetTTL])

        assertEquals(null, hmap.counterRecids)
        assertEquals("", p["aa"+DB.Keys.counterRecids])

        hmap.stores.forEach{assertTrue(db.store===it)}
        hmap.indexTrees.forEach{assertTrue(db.store===(it as IndexTreeLongLongMap).store)}
        hmap.expireCreateQueues!!.forEach{assertTrue(db.store===it.store)}
        hmap.expireUpdateQueues!!.forEach{assertTrue(db.store===it.store)}
        hmap.expireGetQueues!!.forEach{assertTrue(db.store===it.store)}


        fun qToString(qq:Array<QueueLong>):String{
            val r = LongArrayList()
            for(q in qq){
                r.add(q.tailRecid)
                r.add(q.headRecid)
                r.add(q.headPrevRecid)
            }
            return r.makeString("",",","")
        }
        assertEquals(qToString(hmap.expireCreateQueues!!), p["aa"+DB.Keys.expireCreateQueues])
        assertEquals(qToString(hmap.expireUpdateQueues!!), p["aa"+DB.Keys.expireUpdateQueues])
        assertEquals(qToString(hmap.expireGetQueues!!), p["aa"+DB.Keys.expireGetQueues])


        //ensure there are no duplicates in recids
        val expireRecids = LongHashSet();
        arrayOf(hmap.expireCreateQueues!!, hmap.expireUpdateQueues!!, hmap.expireGetQueues!!).forEach{
            it.forEach{
                expireRecids.add(it.headRecid)
                expireRecids.add(it.tailRecid)
                expireRecids.add(it.headPrevRecid)
            }
        }
        assertEquals(8*3*3, expireRecids.size())

    }

    @Test fun hashMap_Create_Multi_Store(){
        val hmap = DBMaker
                .memoryShardedHashMap(8)
                .expireAfterCreate(10)
                .expireAfterUpdate(10)
                .expireAfterGet(10)
                .create()
        assertEquals(3, hmap.concShift)
        assertEquals(8, hmap.stores.size)
        assertEquals(8, Utils.identityCount(hmap.stores))
        assertEquals(8, Utils.identityCount(hmap.indexTrees))
        assertEquals(8, Utils.identityCount(hmap.expireCreateQueues!!))
        assertEquals(8, Utils.identityCount(hmap.expireUpdateQueues!!))
        assertEquals(8, Utils.identityCount(hmap.expireGetQueues!!))

        for(segment in 0 until 8){
            val store = hmap.stores[segment]
            assertTrue(store===(hmap.indexTrees[segment] as IndexTreeLongLongMap).store)
            assertTrue(store===hmap.expireCreateQueues!![segment].store)
            assertTrue(store===hmap.expireUpdateQueues!![segment].store)
            assertTrue(store===hmap.expireGetQueues!![segment].store)
        }
    }

    @Test fun hashMap_expireUnit(){
        val hmap = DBMaker.heapDB().make().hashMap("aa")
                .expireAfterCreate(1, TimeUnit.SECONDS)
                .expireAfterUpdate(2, TimeUnit.DAYS)
                .expireAfterGet(3, TimeUnit.HOURS)
                .create()

        assertEquals(TimeUnit.SECONDS.toMillis(1), hmap.expireCreateTTL)
        assertEquals(TimeUnit.DAYS.toMillis(2), hmap.expireUpdateTTL)
        assertEquals(TimeUnit.HOURS.toMillis(3), hmap.expireGetTTL)
    }


    @Test fun hashmap_layout_number_to_shift(){
        fun tt(v:Int, expected:Int){
            val map = DBMaker.heapDB().make().hashMap("aa").layout(v,v,1).create();
            assertEquals(expected, map.concShift)
            assertEquals(expected, map.dirShift)
        }

        tt(-1, 0)
        tt(0, 0)
        tt(1, 0)
        tt(2, 1)
        tt(3, 2)
        tt(4, 2)
        tt(5, 3)
        tt(6, 3)
        tt(7, 3)
        tt(8, 3)
        tt(9, 4)
    }


    @Test fun executors_hashMap(){
        val db = DBMaker.heapDB().make()
        assertEquals(0, db.executors.size)
        val exec = Executors.newSingleThreadScheduledExecutor()
        val htreemap = db.hashMap("map")
                .expireAfterCreate(1)
                .expireExecutor(exec)
                .expireExecutorPeriod(10000)
                .create()

        assertEquals(setOf(exec), db.executors)
        assertEquals(exec, htreemap.expireExecutor)
        assertTrue(exec.isTerminated.not() && exec.isShutdown.not())

        //keep it busy a bit during termination
        exec.submit { Thread.sleep(300) }
        db.close()
        //close should terminate this dam thing
        assertTrue(exec.isTerminated && exec.isShutdown)
        assertTrue(db.executors.isEmpty())

    }



    @Test fun treeMap_create_unresolvable_serializer(){
        val db = DB(store=StoreTrivial(), storeOpened = false)
        val unresolvable = object:GroupSerializerObjectArray<String>(){
            override fun deserialize(input: DataInput2, available: Int): String? {
                throw UnsupportedOperationException()
            }

            override fun serialize(out: DataOutput2, value: String) {
                throw UnsupportedOperationException()
            }
        }
        val map = db.treeMap("aa", Serializer.BIG_DECIMAL, unresolvable).create()

        assertEquals(Serializer.BIG_DECIMAL, map.keySerializer)
        assertEquals(unresolvable, map.valueSerializer)

        db.lock.writeLock().lock()
        val nameCatalog = db.nameCatalogLoad()
        assertTrue(2<nameCatalog.size)
        assertEquals("TreeMap",nameCatalog["aa#type"])
        assertEquals("org.mapdb.Serializer#BIG_DECIMAL", nameCatalog["aa#keySerializer"])
    }

    @Test fun treeMap_Create(){
        val db = DB(store=StoreTrivial(), storeOpened = false)

        val map = db.treeMap("aa", Serializer.BIG_DECIMAL, Serializer.BOOLEAN)
                .counterEnable()
                .maxNodeSize(16)
                .create()
        db.lock.writeLock().lock()
        val p = db.nameCatalogParamsFor("aa")

        assertEquals(6, p.size)
        assertEquals("TreeMap", p["aa"+DB.Keys.type])
        assertEquals("org.mapdb.Serializer#BIG_DECIMAL", p["aa"+DB.Keys.keySerializer])
        assertEquals("org.mapdb.Serializer#BOOLEAN", p["aa"+DB.Keys.valueSerializer])
        assertEquals("16", p["aa"+DB.Keys.maxNodeSize])
        assertEquals(map.rootRecidRecid.toString(), p["aa"+DB.Keys.rootRecidRecid])
//TODO reenable once counter is done
//        assertTrue(p["aa"+DB.Keys.counterRecids]!!.toLong()>0)

    }


    @Test fun treeMap_Create_Default(){
        val db = DB(store=StoreTrivial(), storeOpened = false)

        val map = db.treeMap("aa")
                .create()

        db.lock.writeLock().lock()
        val p = db.nameCatalogParamsFor("aa")

        assertEquals(6, p.size)
        assertEquals(map.store, db.store)
        assertEquals("0", p["aa"+DB.Keys.counterRecid])
        assertEquals(CC.BTREEMAP_MAX_NODE_SIZE.toString(), p["aa"+DB.Keys.maxNodeSize])
        assertEquals(map.rootRecidRecid.toString(), p["aa"+DB.Keys.rootRecidRecid])
        assertEquals("TreeMap", p["aa"+DB.Keys.type])
        assertEquals("org.mapdb.Serializer#JAVA", p["aa"+DB.Keys.keySerializer])
        assertEquals("org.mapdb.Serializer#JAVA", p["aa"+DB.Keys.valueSerializer])
    }

    @Test fun treeMap_import(){
        val db = DB(store=StoreTrivial(), storeOpened = false)
        val maker = db.treeMap("aa", Serializer.INTEGER, Serializer.INTEGER)
                .import()
        maker.takeAll((0..6).map{Pair(it, it*2)})
        val map = maker.finish()
        assertEquals(7, map.size)
        for(i in 0..6){
            assertEquals(i*2, map[i])
        }
    }


    @Test fun treeMap_import_size(){
        val db = DB(store=StoreTrivial(), storeOpened = false)
        val maker = db.treeMap("aa", Serializer.INTEGER, Serializer.INTEGER)
                .counterEnable()
                .import()
        maker.takeAll((0..6).map{Pair(it, it*2)})
        val map = maker.finish()
        assertEquals(7, map.size)
    }

    @Test fun treeMap_reopen(){
        val f = TT.tempFile()

        var db = DB(store=StoreDirect.make(file=f.path), storeOpened = false)
        var map = db.treeMap("map", Serializer.INTEGER, Serializer.INTEGER).create()
        map.put(11,22)
        db.commit()
        db.close()

        db = DB(store=StoreDirect.make(file=f.path), storeOpened = true)
        map = db.treeMap("map", Serializer.INTEGER, Serializer.INTEGER).open()
        assertEquals(22, map[11])

        f.delete()
    }

    @Test fun hashMap_reopen(){
        val f = TT.tempFile()

        var db = DB(store=StoreDirect.make(file=f.path), storeOpened = false)
        var map = db.hashMap("map", Serializer.INTEGER, Serializer.INTEGER).create()
        map.put(11,22)
        db.commit()
        db.close()

        db = DB(store=StoreDirect.make(file=f.path), storeOpened = true)
        map = db.hashMap("map", Serializer.INTEGER, Serializer.INTEGER).open()
        assertEquals(22, map[11])

        f.delete()
    }


    @Test fun treeSet_base(){
        val db = DB(store=StoreTrivial(), storeOpened = false)

        val set = db.treeSet("set").serializer(Serializer.INTEGER).make();
        set.add(1)
        assertEquals(1, set.size)

        db.lock.writeLock().lock()
        val catalog = db.nameCatalogParamsFor("set")
        assertNull(catalog["set"+ DB.Keys.keySerializer])
        assertNull(catalog["set"+ DB.Keys.valueSerializer])

        assertEquals("org.mapdb.Serializer#INTEGER", catalog["set"+ DB.Keys.serializer])
    }

    @Test fun hashSet_base(){
        val db = DB(store=StoreTrivial(), storeOpened = false)

        val set = db.hashSet("set").serializer(Serializer.INTEGER).make();
        set.add(1)
        assertEquals(1, set.size)

        db.lock.writeLock().lock()
        val catalog = db.nameCatalogParamsFor("set")
        assertNull(catalog["set"+ DB.Keys.keySerializer])
        assertNull(catalog["set"+ DB.Keys.valueSerializer])

        assertEquals("org.mapdb.Serializer#INTEGER", catalog["set"+ DB.Keys.serializer])
    }


    @Test fun hashSet_create_unresolvable_serializer(){
        val db = DB(store=StoreTrivial(), storeOpened = false)
        val unresolvable = object:Serializer<String>{
            override fun deserialize(input: DataInput2, available: Int): String? {
                throw UnsupportedOperationException()
            }

            override fun serialize(out: DataOutput2, value: String) {
                throw UnsupportedOperationException()
            }
        }
        val hashmap = db.hashSet("aa", unresolvable).create()

        assertEquals(unresolvable, hashmap.map.keySerializer)

        db.lock.writeLock().lock()
        val nameCatalog = db.nameCatalogLoad()
        assertTrue(2<nameCatalog.size)
        assertEquals("HashSet",nameCatalog["aa#type"])
        assertEquals(null, nameCatalog["aa#serializer"])
    }

    @Test fun hashSet_Create(){
        val db = DB(store=StoreTrivial(), storeOpened = false)

        val hmap = db.hashSet("aa", Serializer.BIG_DECIMAL)
                .layout(0, 8, 2)
                .hashSeed(1000)
                .expireAfterCreate(11L)
                .expireAfterGet(33L)
                .counterEnable()
                .removeCollapsesIndexTreeDisable()
                .create()

        db.lock.writeLock().lock()
        val p = db.nameCatalogParamsFor("aa")

        assertEquals(13, p.size)
        assertEquals(1,hmap.map.indexTrees.size)
        assertEquals((hmap.map.indexTrees[0] as IndexTreeLongLongMap).rootRecid.toString(), p["aa"+DB.Keys.rootRecids])
        assertEquals("HashSet", p["aa"+DB.Keys.type])
        assertEquals("org.mapdb.Serializer#BIG_DECIMAL", p["aa"+DB.Keys.serializer])
        assertTrue((hmap.map.indexTrees[0] as IndexTreeLongLongMap).collapseOnRemove.not())
        assertEquals("false", p["aa"+DB.Keys.removeCollapsesIndexTree])

        assertEquals("0", p["aa"+DB.Keys.concShift])
        assertEquals("2", p["aa"+DB.Keys.levels])
        assertEquals("3", p["aa"+DB.Keys.dirShift])
        assertEquals("1000", p["aa"+DB.Keys.hashSeed])
        assertEquals("11", p["aa"+DB.Keys.expireCreateTTL])
        assertEquals(null, p["aa"+DB.Keys.expireUpdateTTL])
        assertEquals("33", p["aa"+DB.Keys.expireGetTTL])

        fun qToString(q:QueueLong)=""+q.tailRecid+","+q.headRecid+","+q.headPrevRecid
        assertEquals(qToString(hmap.map.expireCreateQueues!![0]), p["aa"+DB.Keys.expireCreateQueues])
        assertEquals(null, p["aa"+DB.Keys.expireUpdateQueues])
        assertEquals(qToString(hmap.map.expireGetQueues!![0]), p["aa"+DB.Keys.expireGetQueues])

        assertEquals(1, hmap.map.counterRecids!!.size)
        assertTrue(p["aa"+DB.Keys.counterRecids]!!.toLong()>0)

    }


    @Test fun hashSet_Create_Default(){
        val db = DB(store=StoreTrivial(), storeOpened = false)

        val hmap = db.hashSet("aa")
                .create()
        db.lock.writeLock().lock()
        val p = db.nameCatalogParamsFor("aa")

        assertEquals(13, p.size)
        val rootRecids = hmap.map.indexTrees
                .map { (it as IndexTreeLongLongMap).rootRecid.toString()}
                .fold("",{str, it-> str+",$it"})

        assertEquals(8, Utils.identityCount(hmap.map.indexTrees))
        assertEquals(1, hmap.map.stores.toSet().size)
        assertEquals(rootRecids, ","+p["aa"+DB.Keys.rootRecids])
        assertEquals("HashSet", p["aa"+DB.Keys.type])
        assertEquals("org.mapdb.Serializer#JAVA", p["aa"+DB.Keys.serializer])
        assertEquals(null, p["aa"+DB.Keys.valueInline])
        assertTrue((hmap.map.indexTrees[0] as IndexTreeLongLongMap).collapseOnRemove)
        assertEquals("true", p["aa"+DB.Keys.removeCollapsesIndexTree])


        assertEquals("3", p["aa"+DB.Keys.concShift])
        assertEquals("4", p["aa"+DB.Keys.levels])
        assertEquals("4", p["aa"+DB.Keys.dirShift])
        assertTrue(p["aa"+DB.Keys.hashSeed]!!.toInt() != 0)
        assertEquals("0", p["aa"+DB.Keys.expireCreateTTL])
        assertEquals(null, p["aa"+DB.Keys.expireUpdateTTL])
        assertEquals("0", p["aa"+DB.Keys.expireGetTTL])

        assertEquals("", p["aa"+DB.Keys.expireCreateQueues])
        assertEquals(null, p["aa"+DB.Keys.expireUpdateQueues])
        assertEquals("", p["aa"+DB.Keys.expireGetQueues])

        assertEquals(null, hmap.map.counterRecids)
        assertEquals("", p["aa"+DB.Keys.counterRecids])


        hmap.map.stores.forEach{assertTrue(db.store===it)}
        hmap.map.indexTrees.forEach{assertTrue(db.store===(it as IndexTreeLongLongMap).store)}
    }

    @Test fun hashSet_Create_conc_expire(){
        val db = DB(store=StoreTrivial(), storeOpened = false)

        val hmap = db.hashSet("aa")
                .expireAfterCreate(10)
                .expireAfterGet(30)
                .create()
        db.lock.writeLock().lock()
        val p = db.nameCatalogParamsFor("aa")

        assertEquals(13, p.size)
        assertEquals(8, hmap.map.indexTrees.size)
        assertEquals(8, Utils.identityCount(hmap.map.indexTrees))
        assertEquals(1, hmap.map.stores.toSet().size)

        val rootRecids = hmap.map.indexTrees
                .map { (it as IndexTreeLongLongMap).rootRecid.toString()}
                .fold("",{str, it-> str+",$it"})
        assertEquals(rootRecids, ","+p["aa"+DB.Keys.rootRecids])
        assertEquals("HashSet", p["aa"+DB.Keys.type])
        assertEquals("org.mapdb.Serializer#JAVA", p["aa"+DB.Keys.serializer])
        assertEquals(null, p["aa"+DB.Keys.keySerializer])
        assertEquals(null, p["aa"+DB.Keys.valueSerializer])
        assertEquals("3", p["aa"+DB.Keys.concShift])
        assertEquals("4", p["aa"+DB.Keys.levels])
        assertEquals("4", p["aa"+DB.Keys.dirShift])
        assertTrue(p["aa"+DB.Keys.hashSeed]!!.toInt() != 0)
        assertEquals("10", p["aa"+DB.Keys.expireCreateTTL])
        assertEquals(null, p["aa"+DB.Keys.expireUpdateTTL])
        assertEquals("30", p["aa"+DB.Keys.expireGetTTL])

        assertEquals(null, hmap.map.counterRecids)
        assertEquals("", p["aa"+DB.Keys.counterRecids])

        hmap.map.stores.forEach{assertTrue(db.store===it)}
        hmap.map.indexTrees.forEach{assertTrue(db.store===(it as IndexTreeLongLongMap).store)}
        hmap.map.expireCreateQueues!!.forEach{assertTrue(db.store===it.store)}
        assertNull(hmap.map.expireUpdateQueues)
        hmap.map.expireGetQueues!!.forEach{assertTrue(db.store===it.store)}


        fun qToString(qq:Array<QueueLong>):String{
            val r = LongArrayList()
            for(q in qq){
                r.add(q.tailRecid)
                r.add(q.headRecid)
                r.add(q.headPrevRecid)
            }
            return r.makeString("",",","")
        }
        assertEquals(qToString(hmap.map.expireCreateQueues!!), p["aa"+DB.Keys.expireCreateQueues])
        assertEquals(null, p["aa"+DB.Keys.expireUpdateQueues])
        assertEquals(qToString(hmap.map.expireGetQueues!!), p["aa"+DB.Keys.expireGetQueues])


        //ensure there are no duplicates in recids
        val expireRecids = LongHashSet();
        arrayOf(hmap.map.expireCreateQueues!!, hmap.map.expireGetQueues!!).forEach{
            it.forEach{
                expireRecids.add(it.headRecid)
                expireRecids.add(it.tailRecid)
                expireRecids.add(it.headPrevRecid)
            }
        }
        assertEquals(8*3*2, expireRecids.size())

    }

    fun btreemap(set: NavigableSet<*>):BTreeMap<*,*>{
        return (set as BTreeMapJava.KeySet).m as BTreeMap<*,*>
    }

    @Test fun hashSet_Create_Multi_Store(){
        val hmap = DBMaker
                .memoryShardedHashSet(8)
                .expireAfterCreate(10)
                .expireAfterGet(10)
                .create()
        assertEquals(3, hmap.map.concShift)
        assertEquals(8, hmap.map.stores.size)
        assertEquals(8, Utils.identityCount(hmap.map.stores))
        assertEquals(8, Utils.identityCount(hmap.map.indexTrees))
        assertEquals(8, Utils.identityCount(hmap.map.expireCreateQueues!!))
        assertNull(hmap.map.expireUpdateQueues)
        assertEquals(8, Utils.identityCount(hmap.map.expireGetQueues!!))

        for(segment in 0 until 8){
            val store = hmap.map.stores[segment]
            assertTrue(store===(hmap.map.indexTrees[segment] as IndexTreeLongLongMap).store)
            assertTrue(store===hmap.map.expireCreateQueues!![segment].store)
            assertTrue(store===hmap.map.expireGetQueues!![segment].store)
        }
    }

    @Test fun hashSet_expireUnit(){
        val hmap = DBMaker.heapDB().make().hashSet("aa")
                .expireAfterCreate(1, TimeUnit.SECONDS)
                .expireAfterGet(3, TimeUnit.HOURS)
                .create()

        assertEquals(TimeUnit.SECONDS.toMillis(1), hmap.map.expireCreateTTL)
        assertEquals(0, hmap.map.expireUpdateTTL)
        assertEquals(TimeUnit.HOURS.toMillis(3), hmap.map.expireGetTTL)
    }


    @Test fun hashSet_layout_number_to_shift(){
        fun tt(v:Int, expected:Int){
            val map = DBMaker.heapDB().make().hashSet("aa").layout(v,v,1).create();
            assertEquals(expected, map.map.concShift)
            assertEquals(expected, map.map.dirShift)
        }

        tt(-1, 0)
        tt(0, 0)
        tt(1, 0)
        tt(2, 1)
        tt(3, 2)
        tt(4, 2)
        tt(5, 3)
        tt(6, 3)
        tt(7, 3)
        tt(8, 3)
        tt(9, 4)
    }


    @Test fun executors_hashSet(){
        val db = DBMaker.heapDB().make()
        assertEquals(0, db.executors.size)
        val exec = Executors.newSingleThreadScheduledExecutor()
        val htreemap = db.hashSet("map")
                .expireAfterCreate(1)
                .expireExecutor(exec)
                .expireExecutorPeriod(10000)
                .create()

        assertEquals(setOf(exec), db.executors)
        assertEquals(exec, htreemap.map.expireExecutor)
        assertTrue(exec.isTerminated.not() && exec.isShutdown.not())

        //keep it busy a bit during termination
        exec.submit { Thread.sleep(300) }
        db.close()
        //close should terminate this dam thing
        assertTrue(exec.isTerminated && exec.isShutdown)
        assertTrue(db.executors.isEmpty())

    }



    @Test fun treeSet_create_unresolvable_serializer(){
        val db = DB(store=StoreTrivial(), storeOpened = false)
        val unresolvable = object:GroupSerializerObjectArray<String>(){
            override fun deserialize(input: DataInput2, available: Int): String? {
                throw UnsupportedOperationException()
            }

            override fun serialize(out: DataOutput2, value: String) {
                throw UnsupportedOperationException()
            }
        }
        val map = db.treeSet("aa", unresolvable).create()

        assertEquals(unresolvable, btreemap(map).keySerializer)

        db.lock.writeLock().lock()
        val nameCatalog = db.nameCatalogLoad()
        assertTrue(2<nameCatalog.size)
        assertEquals("TreeSet",nameCatalog["aa#type"])
        assertEquals(null, nameCatalog["aa#keySerializer"])
        assertEquals(null, nameCatalog["aa#serializer"])
    }

    @Test fun treeSet_Create(){
        val db = DB(store=StoreTrivial(), storeOpened = false)

        val map = db.treeSet("aa", Serializer.BIG_DECIMAL)
                .counterEnable()
                .maxNodeSize(16)
                .create()
        db.lock.writeLock().lock()
        val p = db.nameCatalogParamsFor("aa")

        assertEquals(5, p.size)
        assertEquals("TreeSet", p["aa"+DB.Keys.type])
        assertEquals("org.mapdb.Serializer#BIG_DECIMAL", p["aa"+DB.Keys.serializer])
        assertEquals("16", p["aa"+DB.Keys.maxNodeSize])
        assertEquals(btreemap(map).rootRecidRecid.toString(), p["aa"+DB.Keys.rootRecidRecid])
        //TODO reenable once counter is done
        //        assertTrue(p["aa"+DB.Keys.counterRecids]!!.toLong()>0)

    }


    @Test fun treeSet_Create_Default(){
        val db = DB(store=StoreTrivial(), storeOpened = false)

        val map = db.treeSet("aa")
                .create()

        db.lock.writeLock().lock()
        val p = db.nameCatalogParamsFor("aa")

        assertEquals(5, p.size)
        assertEquals(btreemap(map).store, db.store)
        assertEquals("0", p["aa"+DB.Keys.counterRecid])
        assertEquals(CC.BTREEMAP_MAX_NODE_SIZE.toString(), p["aa"+DB.Keys.maxNodeSize])
        assertEquals(btreemap(map).rootRecidRecid.toString(), p["aa"+DB.Keys.rootRecidRecid])
        assertEquals("TreeSet", p["aa"+DB.Keys.type])
        assertEquals("org.mapdb.Serializer#JAVA", p["aa"+DB.Keys.serializer])
        assertEquals(null, p["aa"+DB.Keys.keySerializer])
        assertEquals(null, p["aa"+DB.Keys.valueSerializer])
    }
// TODO treeSet import
//    @Test fun treeSet_import(){
//        val db = DB(store=StoreTrivial(), storeOpened = false)
//        val maker = db.treeSet("aa", Serializer.INTEGER)
//                .import()
//        maker.takeAll((0..6).map{it})
//        val map = maker.finish()
//        assertEquals(7, map.size)
//        for(i in 0..6){
//            assertTrue(map.contains(i))
//        }
//    }
//
//
//    @Test fun treeSet_import_size(){
//        val db = DB(store=StoreTrivial(), storeOpened = false)
//        val maker = db.treeSet("aa", Serializer.INTEGER)
//                .counterEnable()
//                .import()
//        maker.takeAll((0..6).map{it})
//        val map = maker.finish()
//        assertEquals(7, map.size)
//    }
//
    @Test fun treeSet_reopen(){
        val f = TT.tempFile()

        var db = DB(store=StoreDirect.make(file=f.path), storeOpened = false)
        var map = db.treeSet("map", Serializer.INTEGER).create()
        map.add(11)
        db.commit()
        db.close()

        db = DB(store=StoreDirect.make(file=f.path), storeOpened = true)
        map = db.treeSet("map", Serializer.INTEGER).open()
        assertTrue(map.contains(11))

        f.delete()
    }

    @Test fun hashSet_reopen(){
        val f = TT.tempFile()

        var db = DB(store=StoreDirect.make(file=f.path), storeOpened = false)
        var map = db.hashSet("map", Serializer.INTEGER).create()
        map.add(11)
        db.commit()
        db.close()

        db = DB(store=StoreDirect.make(file=f.path), storeOpened = true)
        map = db.hashSet("map", Serializer.INTEGER).open()
        assertTrue(map.contains(11))

        f.delete()
    }

    @Test fun indexTreeLongLongMap_create(){
        val db = DBMaker.memoryDB().make()
        val map = db.indexTreeLongLongMap("map").make();
        map.put(1L, 2L);
        assertEquals(1, map.size())
    }


    @Test fun indexTreeLongLongMap_reopen(){
        val f = TT.tempFile()

        var db = DB(store=StoreDirect.make(file=f.path), storeOpened = false)
        var map = db.indexTreeLongLongMap("aa").layout(3,5).removeCollapsesIndexTreeDisable().make()
        for(i in 1L .. 1000L)
            map.put(i,i*2)
        db.commit()
        db.close()

        db = DB(store=StoreDirect.make(file=f.path), storeOpened = true)
        map = db.indexTreeLongLongMap("aa").open()

        for(i in 1L .. 1000L)
            assertEquals(i*2, map.get(i))
        assertEquals(1000, map.size())

        db.lock.writeLock().lock()
        val catalog = db.nameCatalogLoad()
        assertEquals(5, catalog.size)
        assertEquals("false", catalog["aa"+DB.Keys.removeCollapsesIndexTree])
        assertEquals("2",catalog["aa"+DB.Keys.dirShift])
        assertEquals("5",catalog["aa"+DB.Keys.levels])
        assertEquals("IndexTreeLongLongMap", catalog["aa"+DB.Keys.type])
        assertEquals(map.rootRecid.toString(), catalog["aa"+DB.Keys.rootRecid])
        f.delete()
    }


    @Test fun indexTreeList_create(){
        val db = DBMaker.memoryDB().make()
        val list:IndexTreeList<Int> = db.indexTreeList("map", Serializer.INTEGER).make();
        list.add(11)
        assertEquals(1, list.size)
    }


    @Test fun indexTreeList_reopen(){
        val f = TT.tempFile()

        var db = DB(store=StoreDirect.make(file=f.path), storeOpened = false)
        var list = db.indexTreeList("aa",Serializer.INTEGER).layout(3,5).removeCollapsesIndexTreeDisable().make()
        for(i in 1 .. 1000)
            list.add(i)
        db.commit()
        db.close()

        db = DB(store=StoreDirect.make(file=f.path), storeOpened = true)
        list = db.indexTreeList("aa").open() as IndexTreeList<Int>

        for(i in 1 .. 1000)
            assertEquals(i, list[i-1])
        assertEquals(1000, list.size)

        db.lock.writeLock().lock()
        val catalog = db.nameCatalogLoad()
        assertEquals(7, catalog.size)
        assertEquals("false", catalog["aa"+DB.Keys.removeCollapsesIndexTree])
        assertEquals("2",catalog["aa"+DB.Keys.dirShift])
        assertEquals("5",catalog["aa"+DB.Keys.levels])
        assertEquals("IndexTreeLongLongMap", catalog["aa"+DB.Keys.type])
        assertEquals("org.mapdb.Serializer#INTEGER",catalog["aa"+DB.Keys.serializer])
        assertEquals((list.map as IndexTreeLongLongMap).rootRecid.toString(), catalog["aa"+DB.Keys.rootRecid])
        f.delete()
    }


    @Test fun weakref_test(){
        fun test(f:(db:DB)->DB.Maker<*>){
            var db = DBMaker.memoryDB().make()
            var c = f(db).make()
            assertTrue(c===f(db).make())

            db = DBMaker.memoryDB().make()
            c = f(db).make()
            assertTrue(c===f(db).open())

            db = DBMaker.memoryDB().make()
            c = f(db).create()
            assertTrue(c===f(db).open())

            db = DBMaker.memoryDB().make()
            c = f(db).create()
            assertTrue(c===f(db).make())
        }

        test{it.hashMap("aa")}
        test{it.hashSet("aa")}
        test{it.treeMap("aa")}
        test{it.treeSet("aa")}

        test{it.atomicBoolean("aa")}
        test{it.atomicInteger("aa")}
        test{it.atomicVar("aa")}
        test{it.atomicString("aa")}
        test{it.atomicLong("aa")}

        test{it.indexTreeList("aa")}
        test{it.indexTreeLongLongMap("aa")}
    }

    @Test fun get() {
        val db = DBMaker.memoryDB().make()

        assertNull(db.get<Any?>("aa"))
        assertTrue(db.treeMap("aa").make() === db.get<Any?>("aa"))
        assertTrue(db.treeSet("ab").make() === db.get<Any?>("ab"))
        assertTrue(db.hashMap("ac").make() === db.get<Any?>("ac"))
        assertTrue(db.hashSet("ad").make() === db.get<Any?>("ad"))

        assertTrue(db.atomicBoolean("ae").make() === db.get<Any?>("ae"))
        assertTrue(db.atomicInteger("af").make() === db.get<Any?>("af"))
        assertTrue(db.atomicVar("ag").make() === db.get<Any?>("ag"))
        assertTrue(db.atomicString("ah").make() === db.get<Any?>("ah"))
        assertTrue(db.atomicLong("ai").make() === db.get<Any?>("ai"))

        assertTrue(db.indexTreeList("aj").make() === db.get<Any?>("aj"))
        assertTrue(db.indexTreeLongLongMap("ak").make() === db.get<Any?>("ak"))
    }



}