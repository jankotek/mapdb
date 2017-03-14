@file:Suppress("CAST_NEVER_SUCCEEDS")

package org.mapdb

import org.fest.reflect.core.Reflection
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.*

/**
 * Tests Serialization abstraction in DB
 */
class DBSerTest{

    fun DB.pojoSingletons() =
            Reflection.method("pojoSingletons")
                    .`in`(this)
                    .invoke() as Array<Any>


    @Test fun named(){
        val f = TT.tempFile();
        var db = DBMaker.fileDB(f).make()

        var atom = db.atomicInteger("atom").create()
        atom.set(1111)

        @Suppress("UNCHECKED_CAST")
        var map = db.hashMap("map").create() as MutableMap<Any,Any>
        map.put(11, atom)
        db.close()

        db = DBMaker.fileDB(f).make()

        @Suppress("UNCHECKED_CAST")
        map = db.hashMap("map").open() as MutableMap<Any,Any>
        val o = map[11]
        assertTrue(o is Atomic.Integer && o.get()==1111)

        atom = db.atomicInteger("atom").open()

        assertTrue(o===atom)
        db.close()
        f.delete()
    }

    fun <E> dbClone(e:E, db:DB):E {
        return TT.clone(e, db.defaultSerializer)
    }

    @Test fun dbSingleton(){
        val db = DBMaker.memoryDB().make()
        assertTrue(db===dbClone(db,db))
    }

    @Test fun serializerSingleton(){
        val db = DBMaker.memoryDB().make()
        for(f in Serializer::class.java.declaredFields){
            f.isAccessible=true
            val v = f.get(null)
            assertTrue(f.name, v===dbClone(v,db))
        }
    }

    @Test fun pojoSingletons1(){
        val db = DBMaker.memoryDB().make()
        val singletons = db.pojoSingletons()

        // Verify that format is backward compatible. Verify that singletons declared in DB object are the same as this list.
        //
        //if DB.pojoSingletons changes, this method will have to be updated as well.
        // !!! DO NOT CHANGE INDEX OF EXISTING VALUE, just add to the END!!!
        val other = arrayOf(
                db,
                db.defaultSerializer,
                Serializer.CHAR, Serializer.STRING_ORIGHASH , Serializer.STRING, Serializer.STRING_DELTA,
                Serializer.STRING_DELTA2, Serializer.STRING_INTERN, Serializer.STRING_ASCII, Serializer.STRING_NOSIZE,
                Serializer.LONG, Serializer.LONG_PACKED, Serializer.LONG_DELTA, Serializer.INTEGER,
                Serializer.INTEGER_PACKED, Serializer.INTEGER_DELTA, Serializer.BOOLEAN, Serializer.RECID,
                Serializer.RECID_ARRAY, Serializer.ILLEGAL_ACCESS, Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY_DELTA,
                Serializer.BYTE_ARRAY_DELTA2, Serializer.BYTE_ARRAY_NOSIZE, Serializer.CHAR_ARRAY, Serializer.INT_ARRAY,
                Serializer.LONG_ARRAY, Serializer.DOUBLE_ARRAY, Serializer.JAVA, Serializer.ELSA, Serializer.UUID,
                Serializer.BYTE, Serializer.FLOAT, Serializer.DOUBLE, Serializer.SHORT, Serializer.SHORT_ARRAY,
                Serializer.FLOAT_ARRAY, Serializer.BIG_INTEGER, Serializer.BIG_DECIMAL, Serializer.CLASS,
                Serializer.DATE,
                Collections.EMPTY_LIST,
                Collections.EMPTY_SET,
                Collections.EMPTY_MAP,
                Serializer.SQL_DATE,
                Serializer.SQL_TIME,
                Serializer.SQL_TIMESTAMP
        )

        singletons.forEachIndexed { i, singleton ->
            assertTrue(other[i]===singleton)
        }
    }

    @Test fun pojoSingleton_no_dup(){
        val db = DBMaker.memoryDB().make()
        val singletons = db.pojoSingletons()

        val map = IdentityHashMap<Any,Any>();
        singletons.forEach { map.put(it,"") }

        assertEquals(map.size, singletons.size)
    }

}
