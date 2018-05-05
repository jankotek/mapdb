package org.mapdb.serializer

import org.mapdb.io.*
import java.util.*

object Serializers {


    /** Serializer for [java.lang.Integer] */
    @JvmField
    val INTEGER = object : Serializer<Int> {

        override fun serializedType() = java.lang.Integer::class.java

        override fun serialize(k: Int, out: DataOutput2) {
            out.writeInt(k)
        }

        override fun deserialize(input: DataInput2): Int {
            return input.readInt()
        }


        override fun hashCode(k: Int): Int {
            return DataIO.intHash(k)
        }
    }

    /** Serializer for [java.lang.Long] */
    @JvmField
    val LONG = object : Serializer<Long> {

        override fun serializedType() = java.lang.Long::class.java

        override fun serialize(k: Long, out: DataOutput2) {
            out.writeLong(k)
        }

        override fun deserialize(input: DataInput2): Long {
            return input.readLong()
        }

        override fun hashCode(k: Long): Int {
            return DataIO.longHash(k)
        }
    }


    /** Serializer for recids (packed 6 bytes, extra parity bit) */
    @JvmField
    val RECID = object : Serializer<Long> {

        override fun serializedType() = java.lang.Long::class.java

        override fun serialize(k: Long, out: DataOutput2) {
            out.writePackedRecid(k)
        }

        override fun deserialize(input: DataInput2): Long {
            return input.readPackedLong()
        }


        override fun hashCode(k: Long): Int {
            return DataIO.longHash(k)
        }
    }

    /** Serializer for [java.lang.String] */
    @JvmField
    val STRING = object : Serializer<String> {

        override fun serializedType() = java.lang.String::class.java

        override fun serialize(k: String, out: DataOutput2) {
            out.writeUTF(k)
        }

        override fun deserialize(input: DataInput2): String {
            return input.readUTF()
        }

        //FIXME better hash code
    }

    /** Serializer for `byte[]`, adds extra few bytes for array size */
    @JvmField
    val BYTE_ARRAY = object : AbstractByteArraySerializer() {

        override fun deserialize(input: DataInput2): ByteArray {
            val size = input.readPackedInt()
            val b = ByteArray(size)
            input.readFully(b)
            return b
        }

        override fun serialize(k: ByteArray, out: DataOutput2) {
            out.writePackedInt(k.size)
            out.sizeHint(k.size)
            out.write(k)
        }

    }


    /**
     * Serializer for `byte[]`, but does not add extra bytes for array size.
     *
     * Uses [org.mapdb.io.DataInput2.available] to determine array size on deserialization.
     */
    @JvmField
    val BYTE_ARRAY_NOSIZE = object : AbstractByteArraySerializer() {

        override fun deserialize(input: DataInput2): ByteArray {
            val size = input.available()
            val b = ByteArray(size)
            input.readFully(b)
            return b
        }

        override fun serialize(k: ByteArray, out: DataOutput2) {
            out.sizeHint(k.size)
            out.write(k)
        }


    }

    /** serialize record into ByteArray using given serializer */
    fun <K> serializeToByteArray(record: K, serializer: Serializer<K>): ByteArray {
        val out = DataOutput2ByteArray()
        serializer.serialize(record, out)
        return out.copyBytes()
    }


    /** serialize record into ByteArray using given serializer */
    fun <K> serializeToByteArrayNullable(record: K?, serializer: Serializer<K>): ByteArray? {
        return if (record == null) null
        else serializeToByteArray(record, serializer)
    }

    fun <K> clone(record: K, serializer: Serializer<K>): K {
        val ba = serializeToByteArray(record, serializer)
        return deserializeFromByteArray(ba, serializer)
    }

    fun <K> deserializeFromByteArray(ba: ByteArray, serializer: Serializer<K>): K {
        return serializer.deserialize(DataInput2ByteArray(ba))
    }

    fun <K> binaryEqual(ser: Serializer<K>, a: K, b: K): Boolean {
        val aa = serializeToByteArray(a, ser)
        val bb = serializeToByteArray(b, ser)
        return Arrays.equals(aa, bb)

    }
}

abstract class AbstractByteArraySerializer : Serializer<ByteArray> {


    override fun serializedType() = ByteArray::class.java

    override fun equals(k1: ByteArray?, k2: ByteArray?): Boolean {
        return Arrays.equals(k1, k2)
    }

    override fun hashCode(k: ByteArray): Int {
        return Arrays.hashCode(k) //TODO better hash, 31 is weak
    }
}
