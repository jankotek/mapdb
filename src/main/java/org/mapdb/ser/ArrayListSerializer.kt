package org.mapdb.ser

import org.mapdb.io.DataInput2
import org.mapdb.io.DataOutput2

class ArrayListSerializer<K>(val ser: Serializer<K>) : Serializer<ArrayList<K>> {

    override fun serialize(k: ArrayList<K>, out: DataOutput2) {
        out.writePackedInt(k.size)
        for(e in k){
            ser.serialize(e, out)
        }
    }

    override fun deserialize(input: DataInput2): ArrayList<K> {
        val size = input.readPackedInt()
        val list = ArrayList<K>(size)
        for(i in 0 until size){
            val e = ser.deserialize(input)
            list.add(e)
        }
        return list
    }

    override fun serializedType(): Class<*> = ArrayList::class.java

}
