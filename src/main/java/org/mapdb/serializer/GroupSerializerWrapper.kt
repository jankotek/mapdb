package org.mapdb.serializer

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.hasher.Hasher

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

        @Suppress("UNCHECKED_CAST")
        fun <V> wrap(serializer: Serializer<out Any?>): GroupSerializer<V> {
            return if (serializer is GroupSerializer) {
                serializer as GroupSerializer<V>
            } else {
                GroupSerializerWrapper(serializer as Serializer<V>)
            }
        }
    }


    //TODO all those method needed?
    override fun defaultHasher(): Hasher<A> {
        return ser.defaultHasher()
    }

    override fun fixedSize(): Int {
        return ser.fixedSize()
    }

    override fun isTrusted(): Boolean {
        return ser.isTrusted()
    }

    override fun needsAvailableSizeHint(): Boolean {
        return ser.needsAvailableSizeHint()
    }

    override fun deserializeFromLong(input: Long, available: Int): A {
        return ser.deserializeFromLong(input, available)
    }

    override fun clone(value: A): A {
        return ser.clone(value)
    }

    override fun isQuick(): Boolean {
        return ser.isQuick()
    }

    override fun nextValue(value: A): A {
        return ser.nextValue(value)
    }
}
