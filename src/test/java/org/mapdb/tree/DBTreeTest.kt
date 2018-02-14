package org.mapdb.tree

import org.junit.Assert
import org.junit.Test
import org.mapdb.*
import org.mapdb.serializer.GroupSerializerObjectArray
import org.mapdb.serializer.Serializers
import org.mapdb.store.StoreTrivial
import java.util.*

class DBTreeTest{

    fun btreemap(set: NavigableSet<*>): BTreeMap<*, *> {
        return (set as BTreeMapJava.KeySet).m as BTreeMap<*, *>
    }


    @Test fun treeSet_Create(){
        val db = DB(store = StoreTrivial(), storeOpened = false, isThreadSafe = false)

        val map = db.treeSet("aa", Serializers.BIG_DECIMAL)
                .counterEnable()
                .maxNodeSize(16)
                .create()

        val p = db.nameCatalogParamsFor("aa")

        Assert.assertEquals(5, p.size)
        Assert.assertEquals("TreeSet", p["aa" + DB.Keys.type])
        Assert.assertEquals("org.mapdb.serializer.Serializers#BIG_DECIMAL", p["aa" + DB.Keys.serializer])
        Assert.assertEquals("16", p["aa" + DB.Keys.maxNodeSize])
        Assert.assertEquals(btreemap(map).rootRecidRecid.toString(), p["aa" + DB.Keys.rootRecidRecid])
        //TODO reenable once counter is done
        //        assertTrue(p["aa"+DB.Keys.counterRecids]!!.toLong()>0)

    }


    @Test fun treeSet_Create_Default(){
        val db = DB(store = StoreTrivial(), storeOpened = false, isThreadSafe = false)

        val map = db.treeSet("aa")
                .create()

        val p = db.nameCatalogParamsFor("aa")

        Assert.assertEquals(5, p.size)
        Assert.assertEquals(btreemap(map).store, db.store)
        Assert.assertEquals("0", p["aa" + DB.Keys.counterRecid])
        Assert.assertEquals(CC.BTREEMAP_MAX_NODE_SIZE.toString(), p["aa" + DB.Keys.maxNodeSize])
        Assert.assertEquals(btreemap(map).rootRecidRecid.toString(), p["aa" + DB.Keys.rootRecidRecid])
        Assert.assertEquals("TreeSet", p["aa" + DB.Keys.type])
        Assert.assertEquals("org.mapdb.DB#defaultSerializer", p["aa" + DB.Keys.serializer])
        Assert.assertEquals(null, p["aa" + DB.Keys.keySerializer])
        Assert.assertEquals(null, p["aa" + DB.Keys.valueSerializer])
    }



    @Test fun treeSet_create_unresolvable_serializer(){
        val db = DB(store =StoreTrivial(), storeOpened = false, isThreadSafe = false)
        val unresolvable = object: GroupSerializerObjectArray<String>(){
            override fun deserialize(input: DataInput2, available: Int): String? {
                throw UnsupportedOperationException()
            }

            override fun serialize(out: DataOutput2, value: String) {
                throw UnsupportedOperationException()
            }
        }
        val map = db.treeSet("aa", unresolvable).create()

        Assert.assertEquals(unresolvable, btreemap(map).keySerializer)

        val nameCatalog = db.nameCatalogLoad()
        Assert.assertTrue(2 < nameCatalog.size)
        Assert.assertEquals("TreeSet", nameCatalog["aa#type"])
        Assert.assertEquals(null, nameCatalog["aa#keySerializer"])
        Assert.assertEquals(null, nameCatalog["aa#serializer"])
    }
}