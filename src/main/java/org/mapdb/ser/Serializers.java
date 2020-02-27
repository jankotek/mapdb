package org.mapdb.ser;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mapdb.DBException;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataInput2ByteArray;
import org.mapdb.io.DataOutput2;
import org.mapdb.io.DataOutput2ByteArray;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

public final class Serializers {

    private Serializers(){}


    /** Serializer for [java.lang.Integer] */
    public static final Serializer<Integer> INTEGER = new IntegerSerializer();

    /** Serializer for [java.lang.Long] */
    public static final Serializer<Long>  LONG = new LongSerializer();


    /** Serializer for recids (packed 6 bytes, extra parity bit) */
    public static final Serializer<Long>  RECID = new RecidSerializer();

    /** Serializer for [java.lang.String] */
    public static final Serializer<String> STRING  = new StringSerializer();

    public static final Serializer<String> STRING_DELTA = new StringDeltaSerializer();

    public static final Serializer<String>  STRING_DELTA2 = new StringDelta2Serializer();


    public static final Serializer<String> STRING_NOSIZE = new StringNoSizeSerializer();


    /**
     * Serializer for `byte[]`, but does not add extra bytes for array size.
     *
     * Uses [org.mapdb.io.DataInput2.available] to determine array size on deserialization.
     */
    public static final Serializer<byte[]> BYTE_ARRAY_NOSIZE = new ByteArrayNoSizeSerializer();



    public static final Serializer JAVA = new JavaSerializer();

    public static final Serializer<Byte> BYTE = new ByteSerializer();

    /** Serializer for `byte[]`, adds extra few bytes for array size */
    public static final Serializer<byte[]> BYTE_ARRAY = new ByteArraySerializer();
    public static final Serializer<byte[]> BYTE_ARRAY_DELTA = new ByteArrayDeltaSerializer();
    public static final Serializer<byte[]> BYTE_ARRAY_DELTA2 = new ByteArrayDelta2Serializer();


    public static final Serializer<Character> CHAR = new CharSerializer();
    public static final Serializer<char[]> CHAR_ARRAY = new CharArraySerializer();

    public static final Serializer<Short> SHORT = new ShortSerializer();
    public static final Serializer<short[]> SHORT_ARRAY = new ShortArraySerializer();

    public static final Serializer<Float> FLOAT = new FloatSerializer();
    public static final Serializer<float[]> FLOAT_ARRAY = new FloatArraySerializer();

    public static final Serializer<Double> DOUBLE = new DoubleSerializer();
    public static final Serializer<double[]> DOUBLE_ARRAY = new DoubleArraySerializer();

    public static final Serializer<Boolean> BOOLEAN = new BooleanSerializer();

    public static final Serializer<int[]> INT_ARRAY = new IntArraySerializer();
    public static final Serializer<long[]> LONG_ARRAY = new LongArraySerializer();


    public static final Serializer<BigDecimal> BIG_DECIMAL = new BigDecimalSerializer();
    public static final Serializer<BigInteger> BIG_INTEGER = new BigIntegerSerializer();

    public static final Serializer<Class> CLASS = new ClassSerializer();
    public static final Serializer<Date> DATE = new DateSerializer();
    public static final Serializer<UUID> UUID = new UUIDSerializer();


    public static <R> byte[] serializeToByteArray(R record, Serializer<R> serializer) {
        DataOutput2ByteArray out = new DataOutput2ByteArray();
        serializer.serialize(out,record);
        return out.copyBytes();
    }

    @Nullable
    public static <R> R clone(@NotNull R r, @NotNull Serializer<R> ser) {
        byte[] b = serializeToByteArray(r, ser);
        return ser.deserialize(new DataInput2ByteArray(b));
    }

    @NotNull
    public static boolean binaryEqual(@NotNull Serializer<Object> ser, Object a, Object b) {
        if(a==b)
            return true;
        if(a==null || b==null)
            return false;
        byte[] ba = serializeToByteArray(a, ser);
        byte[] bb = serializeToByteArray(b, ser);
        return Arrays.equals(ba,bb);
    }
}
