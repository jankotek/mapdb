@file:Suppress("CAST_NEVER_SUCCEEDS")

package org.mapdb

import org.junit.Assert.*
import org.junit.Test
import org.mapdb.serializer.Serializer
import org.mapdb.serializer.Serializers
import java.util.*

/**
 * Tests Serialization abstraction in DB
 */
class DBSerTest{

    fun DB.pojoSingletons():Array<Any> = TT.reflectionInvokeMethod(this, "pojoSingletons")


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
                Serializers.CHAR, Serializers.STRING_ORIGHASH, Serializers.STRING, Serializers.STRING_DELTA,
                Serializers.STRING_DELTA2, Serializers.STRING_INTERN, Serializers.STRING_ASCII, Serializers.STRING_NOSIZE,
                Serializers.LONG, Serializers.LONG_PACKED, Serializers.LONG_DELTA, Serializers.INTEGER,
                Serializers.INTEGER_PACKED, Serializers.INTEGER_DELTA, Serializers.BOOLEAN, Serializers.RECID,
                Serializers.RECID_ARRAY, Serializers.SERIALIZER_UNSUPPORTED, Serializers.BYTE_ARRAY, Serializers.BYTE_ARRAY_DELTA,
                Serializers.BYTE_ARRAY_DELTA2, Serializers.BYTE_ARRAY_NOSIZE, Serializers.CHAR_ARRAY, Serializers.INT_ARRAY,
                Serializers.LONG_ARRAY, Serializers.DOUBLE_ARRAY, Serializers.JAVA, Serializers.ELSA, Serializers.UUID,
                Serializers.BYTE, Serializers.FLOAT, Serializers.DOUBLE, Serializers.SHORT, Serializers.SHORT_ARRAY,
                Serializers.FLOAT_ARRAY, Serializers.BIG_INTEGER, Serializers.BIG_DECIMAL, Serializers.CLASS,
                Serializers.DATE,
                Collections.EMPTY_LIST,
                Collections.EMPTY_SET,
                Collections.EMPTY_MAP,
                Serializers.SQL_DATE,
                Serializers.SQL_TIME,
                Serializers.SQL_TIMESTAMP
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
