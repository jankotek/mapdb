package org.mapdb

import org.junit.Assert.assertEquals
import org.junit.Test
import org.mapdb.serializer.*
import org.mapdb.tree.BTreeMap
import java.util.*

/**
 * test if map keyset, values and entrySet correctly use Serializer.hashcode and Serializer.equals
 */
//TODO this needs more work, better way to mock serializer statistics
abstract class MapSubcolsTest{

    var keyEqCount = 0
    var keyHashCount = 0

    val keyser = object: SerializerByteArray() {
        override fun equals(first: ByteArray?, second: ByteArray?): Boolean {
            keyEqCount++
            return super.equals(first, second)
        }

        override fun hashCode(o: ByteArray, seed: Int): Int {
            keyHashCount++
            return super.hashCode(o, seed)
        }
    }

    var valEqCount = 0
    var valHashCount = 0

    val valser = object: SerializerIntArray(){
        override fun equals(first: IntArray?, second: IntArray?): Boolean {
            valEqCount++
            return super.equals(first, second)
        }

        override fun hashCode(o: IntArray, seed: Int): Int {
            valHashCount++
            return super.hashCode(o, seed)
        }
    }


    fun clear(){
        keyEqCount = 0
        keyHashCount = 0
        valEqCount = 0
        valHashCount = 0
    }

    fun check(ke:Int, kh:Int, ve:Int, vh:Int){
        assertEquals(ke, keyEqCount)
        assertEquals(kh, keyHashCount)
        assertEquals(ve, valEqCount)
        assertEquals(vh, valHashCount)
    }

    fun test(map:MutableMap<ByteArray, IntArray>){
        map.put(byteArrayOf(1), intArrayOf(4))
        map.put(byteArrayOf(2), intArrayOf(5))
        map.put(byteArrayOf(3), intArrayOf(6))

        val keys = TreeSet<ByteArray>(keyser)
        keys+=byteArrayOf(1)
        keys+=byteArrayOf(2)
        keys+=byteArrayOf(3)
        val vals = ArrayList<IntArray>()
        vals+=intArrayOf(4)
        vals+=intArrayOf(5)
        vals+=intArrayOf(6)

        clear()
        assert(map.keys.equals(keys))
        check(3,0,0,0)
        assert(map.values.equals(vals))
        check(0,0,3,0)

        clear()


    }


    @Test
    fun hashMap(){
        test(DBMaker.memoryDB().make().hashMap("aa", keyser, valser).create())
    }


    @Test
    fun treeMap(){
        val m = DBMaker.memoryDB().make().treeMap("aa", keyser, valser).create() as BTreeMap
        assert(m.keySerializer == m.comparator())
        test(m)
    }
}
