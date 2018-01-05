package org.mapdb.serializer

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.elsa.ElsaSerializerPojo

/**
 * Uses Elsa serialization: http://www.github.com/jankotek/elsa
 */
class SerializerElsa :Serializer<Any?>{

    protected val ser = ElsaSerializerPojo()

    override fun deserialize(input: DataInput2, available: Int): Any? {
        return ser.deserialize(input)
    }

    override fun serialize(out: DataOutput2, value: Any) {
        ser.serialize(out, value)
    }

}