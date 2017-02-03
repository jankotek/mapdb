package org.mapdb.serializer

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

/**
 * Wraps `Serializer` into `GroupSerializer`.
 *
 * It uses object arrays to represent group of objects in memory
 */
data class GroupSerializerWrapper<A>(val ser: Serializer<A>):GroupSerializerObjectArray<A>() {
    init {
        if (ser is GroupSerializer<A>)
            throw IllegalArgumentException("wrapped serializer is already GroupSerializer")
    }

    override fun serialize(out: DataOutput2, value: A) {
        ser.serialize(out, value)
    }

    override fun deserialize(input: DataInput2, available: Int): A {
        return ser.deserialize(input, available)
    }

    companion object {

        fun <V> wrap(serializer: Serializer<out Any?>): GroupSerializer<V> {
            return if (serializer is GroupSerializer) {
                serializer as GroupSerializer<V>
            } else {
                GroupSerializerWrapper(serializer as Serializer<V>)
            }
        }
    }
}
