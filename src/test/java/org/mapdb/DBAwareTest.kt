package org.mapdb

import org.junit.Assert.assertEquals
import org.junit.Assert.assertSame
import org.junit.Test
import org.mapdb.serializer.GroupSerializerObjectArray


class DBAwareTest{

    class Aware: GroupSerializerObjectArray<Any>(), DB.DBAware, DB.NamedRecordAware{

        override fun deserialize(input: DataInput2, available: Int): Any {
            throw UnsupportedOperationException("not implemented")
        }

        override fun serialize(out: DataOutput2, value: Any) {
            throw UnsupportedOperationException("not implemented")
        }

        var db:DB? = null

        override fun callbackDB(db_: DB) {
            db = db_
        }

        var name:String? = null
        var record:Any? = null
        override fun callbackRecord(name: String, collection: Any) {
            this.name = name
            this.record = collection
        }

    }

    val aware = Aware()

    val db = DBMaker.memoryDB().make()

    @Test fun dbAware_hashSet(){

        val c = db.hashSet("aaa", aware).createOrOpen()
        assertSame(db, aware.db)
        assertEquals("aaa", aware.name)
        assertSame(c, aware.record)
    }

    @Test fun dbAware_treemap_key(){
        val c = db.treeMap("aaa", aware, db.defaultSerializer).createOrOpen()
        assertSame(db, aware.db)
        assertEquals("aaa", aware.name)
        assertSame(c, aware.record)
    }


    @Test fun dbAware_treemap_value(){
        val c = db.treeMap("aaa", db.defaultSerializer,aware).createOrOpen()
        assertSame(db, aware.db)
        assertEquals("aaa", aware.name)
        assertSame(c, aware.record)
    }
}