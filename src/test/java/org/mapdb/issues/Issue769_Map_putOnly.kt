package org.mapdb.issues

import org.junit.Test
import org.mapdb.*
import java.util.*

class Issue769_Map_putOnly : Serializer<String>{

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
                .hashMap("map", Serializer.INTEGER, this)
                .create()

        check(m)
    }

    @Test fun treeMap(){
        val m = DBMaker.memoryDB().make()
                .treeMap("map", Serializer.INTEGER, this)
                .valuesOutsideNodesEnable()
                .create()

        check(m)
    }

    private fun check(m: MapExtra<Int, String>) {
        m.put(1, "one")
        deser.clear()
        ser.clear()
        m.putOnly(1, "two")

        assert(ser == arrayListOf("two"))
        assert(deser.isEmpty())
    }

}