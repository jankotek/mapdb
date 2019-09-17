package org.mapdb.ser

import org.mapdb.io.DataInput2
import org.mapdb.io.DataInput2ByteArray
import org.mapdb.io.DataOutput2
import org.mapdb.io.DataOutput2ByteArray
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.util.*

object Serializers {


    /** Serializer for [java.lang.Integer] */
    @JvmField
    val INTEGER = IntegerSerializer()

    /** Serializer for [java.lang.Long] */
    @JvmField
    val LONG = LongSerializer();


    /** Serializer for recids (packed 6 bytes, extra parity bit) */
    @JvmField
    val RECID = RecidSerializer()

    /** Serializer for [java.lang.String] */
    @JvmField
    val STRING = StringSerializer()

    @JvmField
    val STRING_DELTA = StringDeltaSerializer()

    @JvmField
    val STRING_DELTA2 = StringDelta2Serializer()


    @JvmField
    val STRING_NOSIZE = STRING // TODO string_nosize


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

        override fun serialize(out: DataOutput2, k: ByteArray) {
            out.sizeHint(k.size)
            out.write(k)
        }


    }

    /** serialize record into ByteArray using given serializer */
    fun <K> serializeToByteArray(record: K, serializer: Serializer<K>): ByteArray {
        val out = DataOutput2ByteArray()
        serializer.serialize(out, record)
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


    @JvmField val JAVA = object:DefaultGroupSerializer<Any>(){
        override fun serialize(out: DataOutput2, k: Any) {
            val b = ByteArrayOutputStream()
            val b2 = ObjectOutputStream(b)
            b2.writeObject(k)
            b2.close()
            val ba = b.toByteArray()
            out.writePackedInt(ba.size)
            out.write(ba)
        }

        override fun deserialize(input: DataInput2): Any {
            val size = input.readPackedInt()
            val ba = ByteArray(size)
            input.readFully(ba)

            val s = ObjectInputStream(ByteArrayInputStream(ba))
            return s.readObject()
        }

        override fun serializedType(): Class<*>? = null

    }

    @JvmField val BYTE = ByteSerializer();

    /** Serializer for `byte[]`, adds extra few bytes for array size */
    @JvmField val BYTE_ARRAY = ByteArraySerializer();
    @JvmField val BYTE_ARRAY_DELTA = ByteArrayDeltaSerializer();
    @JvmField val BYTE_ARRAY_DELTA2 = ByteArrayDelta2Serializer();


    @JvmField val CHAR = CharSerializer();
    @JvmField val CHAR_ARRAY = CharArraySerializer();

    @JvmField val SHORT = ShortSerializer();
    @JvmField val SHORT_ARRAY = ShortArraySerializer();

    @JvmField val FLOAT = FloatSerializer();
    @JvmField val FLOAT_ARRAY = FloatArraySerializer();

    @JvmField val DOUBLE = DoubleSerializer();
    @JvmField val DOUBLE_ARRAY = DoubleArraySerializer();

    @JvmField val BOOLEAN = BooleanSerializer();

    @JvmField val INT_ARRAY = IntArraySerializer();
    @JvmField val LONG_ARRAY = LongArraySerializer();


    @JvmField val BIG_DECIMAL = BigDecimalSerializer();
    @JvmField val BIG_INTEGER = BigIntegerSerializer();

    @JvmField val CLASS = ClassSerializer();
    @JvmField val DATE = DateSerializer();
    @JvmField val UUID = UUIDSerializer();


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
