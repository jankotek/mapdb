package org.mapdb.serializer

import org.mapdb.io.DataInput2
import org.mapdb.io.DataOutput2
import org.mapdb.io.DataOutput2ByteArray

object Serializers{


    /** Serializer for [java.lang.Integer] */
    @JvmStatic val INTEGER = object:Serializer<Int>{
        override fun serialize(k:Int, out: DataOutput2) {
            out.writeInt(k)
        }

        override fun deserialize(input: DataInput2): Int {
            return input.readInt()
        }
    }

    /** Serializer for [java.lang.Long] */
    @JvmStatic val LONG = object:Serializer<Long>{
        override fun serialize(k: Long, out: DataOutput2) {
            out.writeLong(k)
        }

        override fun deserialize(input: DataInput2): Long {
            return input.readLong()
        }
    }


    /** Serializer for recids (packed 6 bytes, extra parity bit) */
    @JvmStatic val RECID = object:Serializer<Long>{
        override fun serialize(k: Long, out: DataOutput2) {
            out.writePackedRecid(k)
        }

        override fun deserialize(input: DataInput2): Long {
            return input.readPackedLong()
        }
    }

    /** Serializer for [java.lang.String] */
    @JvmStatic val STRING = object:Serializer<String>{
        override fun serialize(k: String, out: DataOutput2) {
            out.writeUTF(k)
        }

        override fun deserialize(input: DataInput2): String {
            return input.readUTF()
        }
    }

    /** Serializer for `byte[]`, adds extra few bytes for array size */
    @JvmStatic val BYTE_ARRAY = object:Serializer<ByteArray>{
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
    @JvmStatic val BYTE_ARRAY_NOSIZE = object:Serializer<ByteArray>{
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
        return if(record==null) null
            else serializeToByteArray(record, serializer)
    }
}