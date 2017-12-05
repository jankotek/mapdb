package org.mapdb.issues

import org.junit.Test
import org.mapdb.*
import org.mapdb.serializer.Serializer
import org.mapdb.serializer.Serializers
import java.util.*


class Issue357_Map_keySet_remove : Serializer<String> {

    val ser = ArrayList<String>()
    val deser = ArrayList<String>()

    override fun serialize(out: DataOutput2, value: String) {
        ser += value
        out.writeUTF(value)
    }

    override fun deserialize(input: DataInput2, available: Int): String {
        val v = input.readUTF()
        deser += v
        return v
    }

    @Test fun hashMap(){
        val m = DBMaker.memoryDB().make()
                .hashMap("map", Serializers.INTEGER, this)
                .create()

        check(m)
    }

    @Test fun treeMap(){
        val m = DBMaker.memoryDB().make()
                .treeMap("map", Serializers.INTEGER, this)
                .valuesOutsideNodesEnable()
                .create()

        check(m)
    }

    private fun check(m: DBConcurrentMap<Int, String>) {
        m.put(1, "one")
        deser.clear()
        ser.clear()
        m.keys.remove(1)
        assert(ser.isEmpty())
        assert(deser.isEmpty())

        m.put(2, "two")
        deser.clear()
        ser.clear()
        m.removeBoolean(1)
        assert(ser.isEmpty())
        assert(deser.isEmpty())


        m.put(3, "three")
        deser.clear()
        ser.clear()
        m.putOnly(3, "tri")
        assert(ser.size==1)
        assert(deser.isEmpty())


        deser.clear()
        ser.clear()
        val iter = m.keys.iterator()
        iter.next()
        iter.remove()
        assert(ser.isEmpty())
        assert(deser.isEmpty())
    }

}