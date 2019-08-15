package org.mapdb.ser

import org.mapdb.io.DataInput2
import org.mapdb.io.DataOutput2

class  PairSer<A,B>(
        private val serA:Serializer<A>,
        private val serB:Serializer<B>
):Serializer<Pair<A,B>>{

    override fun serialize(out: DataOutput2, k: Pair<A, B>) {
        serA.serialize(out, k.first)
        serB.serialize(out, k.second);
    }

    override fun deserialize(input: DataInput2): Pair<A, B> {
        val a = serA.deserialize(input)
        val b = serB.deserialize(input)
        return Pair(a,b)
    }

    override fun serializedType(): Class<*> = Pair::class.java

}