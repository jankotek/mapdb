package org.mapdb

import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet
import org.junit.Assert.*
import org.junit.Test
import java.math.BigDecimal
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

        var nameCatalog = db.nameCatalogLoad()
        nameCatalog.put("aaa", "bbbb")
        db.nameCatalogSave(nameCatalog)

        nameCatalog = db.nameCatalogLoad()
        assertEquals(1, nameCatalog.size)
        assertEquals("bbbb",nameCatalog.get("aaa"))
    }

    @Test fun name_catalog_singleton(){
        val db = DB(store=StoreTrivial(), storeOpened = false)

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
        val unresolvable = object:Serializer<String>(){
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

        val nameCatalog = db.nameCatalogLoad()
        assertTrue(2<nameCatalog.size)
        assertEquals("HashMap",nameCatalog["aa.type"])
        assertEquals("org.mapdb.Serializer#BIG_DECIMAL", nameCatalog["aa.keySerializer"])
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

        val p = db.nameCatalogParamsFor("aa")

        assertEquals(14, p.size)
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
        assertEquals(1, hmap.counterRecids!!.size)
        assertTrue(p["aa"+DB.Keys.counterRecids]!!.toLong()>0)

    }


    @Test fun hashMap_Create_Default(){
        val db = DB(store=StoreTrivial(), storeOpened = false)

        val hmap = db.hashMap("aa")
                .create()

        val p = db.nameCatalogParamsFor("aa")

        assertEquals(14, p.size)
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

        val p = db.nameCatalogParamsFor("aa")

        assertEquals(14, p.size)
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


    @Test fun executors(){
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
        val unresolvable = object:Serializer<String>(){
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

        val nameCatalog = db.nameCatalogLoad()
        assertTrue(2<nameCatalog.size)
        assertEquals("TreeMap",nameCatalog["aa.type"])
        assertEquals("org.mapdb.Serializer#BIG_DECIMAL", nameCatalog["aa.keySerializer"])
    }

    @Test fun treeMap_Create(){
        val db = DB(store=StoreTrivial(), storeOpened = false)

        val map = db.treeMap("aa", Serializer.BIG_DECIMAL, Serializer.BOOLEAN)
                .counterEnable()
                .maxNodeSize(16)
                .create()

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
        val maker = db.treeMap("aa")
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
        val maker = db.treeMap("aa")
                .counterEnable()
                .import()
        maker.takeAll((0..6).map{Pair(it, it*2)})
        val map = maker.finish()
        assertEquals(7, map.size)
    }

}