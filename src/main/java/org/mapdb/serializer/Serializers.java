package org.mapdb.serializer;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public class Serializers {
    /**
     * A predefined {@link Serializer} that handles non-null
     * {@link String Strings} whereby Strings are serialized to a UTF-8 encoded
     * format. The Serializer also stores the String's size, allowing it to be
     * used as a GroupSerializer in BTreeMaps.
     * <p>
     * This Serializer hashes Strings using the original hash code method as
     * opposed to the {@link Serializers#STRING} Serializer.
     * <p>
     * If a {@code null} value is passed to the Serializer, a
     * {@link NullPointerException} will be thrown.
     *
     * @see Serializers#STRING
     */
    public static final GroupSerializer<String> STRING_ORIGHASH = new SerializerStringOrigHash();
    /**
     * A predefined {@link Serializer} that handles non-null
     * {@link String Strings} whereby Strings are serialized to a UTF-8 encoded
     * format. The Serializer also stores the String's size, allowing it to be
     * used as a GroupSerializer in BTreeMaps.
     * <p>
     * This Serializer hashes Strings using a specially tailored hash code
     * method as opposed to the {@link Serializers#STRING_ORIGHASH} Serializer.
     * <p>
     * If a {@code null} value is passed to the Serializer, a
     * {@link NullPointerException} will be thrown.
     *
     * @see Serializers#STRING_ORIGHASH
     */
    public static final GroupSerializer<String> STRING = new SerializerString();
    /**
     * A predefined {@link Serializer} that handles non-null
     * {@link String Strings} whereby Strings are serialized to a UTF-8 encoded
     * format. The Serializer also stores the String's size, allowing it to be
     * used as a GroupSerializer in BTreeMaps. Neighboring strings may be delta
     * encoded for increased storage efficency.
     * <p>
     * This Serializer hashes Strings using a specially tailored hash code
     * method as opposed to the {@link Serializers#STRING_ORIGHASH} Serializer.
     * <p>
     * If a {@code null} value is passed to the Serializer, a
     * {@link NullPointerException} will be thrown.
     *
     * @see Serializers#STRING
     */
    public static final GroupSerializer<String> STRING_DELTA = new SerializerStringDelta();
    /**
     * A predefined {@link Serializer} that handles non-null
     * {@link String Strings} whereby Strings are serialized to a UTF-8 encoded
     * format. The Serializer also stores the String's size, allowing it to be
     * used as a GroupSerializer in BTreeMaps. Neighboring strings may be delta
     * encoded for increased storage efficency.
     * <p>
     * This Serializer hashes Strings using a specially tailored hash code
     * method as opposed to the {@link Serializers#STRING_ORIGHASH} Serializer.
     * <p>
     * If a {@code null} value is passed to the Serializer, a
     * {@link NullPointerException} will be thrown.
     *
     * @see Serializers#STRING
     */
    public static final GroupSerializer<String> STRING_DELTA2 = new SerializerStringDelta2();
    /**
     * A predefined {@link Serializer} that handles non-null
     * {@link String Strings} whereby Strings are serialized to a ASCII encoded
     * format (8 bit character) which is faster than using a UTF-8 format. The
     * Serializer also stores the String's size, allowing it to be used as a
     * GroupSerializer in BTreeMaps.
     * <p>
     * This Serializer hashes Strings using a specially tailored hash code
     * method as opposed to the {@link Serializers#STRING_ORIGHASH} Serializer.
     * <p>
     * If a {@code null} value is passed to the Serializer, a
     * {@link NullPointerException} will be thrown.
     *
     * @see Serializers#STRING_ORIGHASH
     */
    public static final GroupSerializer<String> STRING_ASCII = new SerializerStringAscii();
    /**
     * A predefined {@link Serializer} that handles non-null
     * {@link String Strings} whereby Strings are serialized to a UTF-8 encoded
     * format. The Serializer does <em>not</em> store the String's size, thereby
     * preventing it from being used as a GroupSerializer.
     * <p>
     * This Serializer hashes Strings using the original hash code method as
     * opposed to the {@link Serializers#STRING} Serializer.
     * <p>
     * If a {@code null} value is passed to the Serializer, a
     * {@link NullPointerException} will be thrown.
     *
     * @see Serializers#STRING_ORIGHASH
     */
    public static final Serializer<String> STRING_NOSIZE = new SerializerStringNoSize();
    /**
     * A predefined {@link Serializer} that handles non-null {@link Long Longs}
     * whereby Longs are serialized to an 8 byte format. The Serializer also
     * stores the Longs's size, allowing it to be used as a GroupSerializer in
     * BTreeMaps.
     * <p>
     * This Serializer hashes Longs using the original {@link Long#hashCode()}
     * method.
     * <p>
     * If a {@code null} value is passed to the Serializer, a
     * {@link NullPointerException} will be thrown.
     *
     */
    public static final GroupSerializer<Long> LONG = new SerializerLong();
    /**
     * A predefined {@link Serializer} that handles non-null {@link Long Longs}
     * whereby Longs are serialized to a compressed byte format. The Serializer
     * also stores the Longs's size, allowing it to be used as a GroupSerializer
     * in BTreeMaps.
     * <p>
     * Smaller positive values occupy less than 8 bytes. Large and negative
     * values could occupy 8 or 9 bytes.
     * <p>
     * This Serializer hashes Longs using the original {@link Long#hashCode()}
     * method.
     * <p>
     * If a {@code null} value is passed to the Serializer, a
     * {@link NullPointerException} will be thrown.
     *
     */
    public static final GroupSerializer<Long> LONG_PACKED = new SerializerLongPacked();
    /**
     * A predefined {@link Serializer} that handles non-null {@link Long Longs}
     * whereby Longs are serialized to a compressed byte format and neighboring
     * Longs are delta encoded in BTreeMaps. Neighbors with a small delta can be
     * encoded using a single byte.
     * <p>
     * Smaller positive values occupy less than 8 bytes. Large and negative
     * values could occupy 8 or 9 bytes.
     * <p>
     * This Serializer hashes Longs using the original {@link Long#hashCode()}
     * method.
     * <p>
     * If a {@code null} value is passed to the Serializer, a
     * {@link NullPointerException} will be thrown.
     *
     */
    public static final GroupSerializer<Long> LONG_DELTA = new SerializerLongDelta();
    /**
     * A predefined {@link Serializer} that handles non-null
     * {@link Integer Integers} whereby Integers are serialized to a 4 byte
     * format.
     * <p>
     * This Serializer hashes Integers using the original
     * {@link Integer#hashCode()} method.
     * <p>
     * If a {@code null} value is passed to the Serializer, a
     * {@link NullPointerException} will be thrown.
     *
     */
    public static final GroupSerializer<Integer> INTEGER = new SerializerInteger();
    /**
     * A predefined {@link Serializer} that handles non-null
     * {@link Integer Integers} whereby Integers are serialized to a compressed
     * byte format.The Serializer also stores the Longs's size, allowing it to
     * be used as a GroupSerializer in BTreeMaps.
     * <p>
     * Smaller positive values occupy less than 4 bytes. Large and negative
     * values could occupy 4 or 5 bytes.
     * <p>
     * This Serializer hashes Integers using the original
     * {@link Integer#hashCode()} method.
     * <p>
     * If a {@code null} value is passed to the Serializer, a
     * {@link NullPointerException} will be thrown.
     *
     */
    public static final GroupSerializer<Integer> INTEGER_PACKED = new SerializerIntegerPacked();
    /**
     * A predefined {@link Serializer} that handles non-null
     * {@link Integer Integers} whereby Integers are serialized to a compressed
     * byte format and neighboring Integers are delta encoded in BTreeMaps.
     * Neighbors with a small delta can be encoded using a single byte.
     * <p>
     * Smaller positive values occupy less than 4 bytes. Large and negative
     * values could occupy 4 or 5 bytes.
     * <p>
     * This Serializer hashes Integers using the original
     * {@link Integer#hashCode()} method.
     * <p>
     * If a {@code null} value is passed to the Serializer, a
     * {@link NullPointerException} will be thrown.
     *
     */
    public static final GroupSerializer<Integer> INTEGER_DELTA = new SerializerIntegerDelta();
    /**
     * A predefined {@link Serializer} that handles non-null
     * {@link Boolean Booleans} whereby Booleans are serialized to a one byte
     * format.
     * <p>
     * If a {@code null} value is passed to the Serializer, a
     * {@link NullPointerException} will be thrown.
     *
     */
    public static final GroupSerializer<Boolean> BOOLEAN = new SerializerBoolean();
    /**
     * A predefined {@link Serializer} that handles non-null {@link Long Longs}
     * used as a recid whereby recids are serialized to an eight byte format
     * including a checksum.
     * <p>
     * If a {@code null} value is passed to the Serializer, a
     * {@link NullPointerException} will be thrown.
     *
     */
    public static final GroupSerializer<Long> RECID = new SerializerRecid();
    /**
     * A predefined {@link Serializer} that handles non-null arrays of longs
     * used as a recids whereby recids are serialized to an eight byte format
     * including a checksum.
     * <p>
     * If a {@code null} array is passed to the Serializer, a
     * {@link NullPointerException} will be thrown.
     * <p>
     * If an array that contains a {@code null} value is passed to the
     * Serializer, a {@link NullPointerException} will be thrown.
     *
     */
    public static final GroupSerializer<long[]> RECID_ARRAY = new SerializerRecidArray();
    /**
     * A predefined {@link Serializer} that always throws an
     * {@link UnsupportedOperationException} when invoked.
     * <p>
     * This serializer can be used for testing and assertions.
     */
    public static final GroupSerializer<Object> SERIALIZER_UNSUPPORTED = new SerializerUnsupported();
    /**
     * Serializes {@code byte[]} it adds header which contains size information
     */
    public static final GroupSerializer<byte[]> BYTE_ARRAY = new SerializerByteArray();
    public static final GroupSerializer<byte[]> BYTE_ARRAY_DELTA = new SerializerByteArrayDelta();
    public static final GroupSerializer<byte[]> BYTE_ARRAY_DELTA2 = new SerializerByteArrayDelta2();
    /**
     * Serializes {@code byte[]} directly into underlying store It does not
     * store size, so it can not be used in Maps and other collections.
     */
    public static final Serializer<byte[]> BYTE_ARRAY_NOSIZE = new SerializerByteArrayNoSize();
    /**
     * Serializes {@code char[]} it adds header which contains size information
     */
    public static final GroupSerializer<char[]> CHAR_ARRAY = new SerializerCharArray();
    /**
     * Serializes {@code int[]} it adds header which contains size information
     */
    public static final GroupSerializer<int[]> INT_ARRAY = new SerializerIntArray();
    /**
     * Serializes {@code long[]} it adds header which contains size information
     */
    public static final GroupSerializer<long[]> LONG_ARRAY = new SerializerLongArray();
    /**
     * Serializes {@code double[]} it adds header which contains size
     * information
     */
    public static final GroupSerializer<double[]> DOUBLE_ARRAY = new SerializerDoubleArray();
    /**
     * A predefined {@link Serializer} that handles non-null
     * {@code Serializable} Java objects whereby the standard Java serialization
     * will be applied using {@link java.io.ObjectInputStream} and
     * {@link java.io.ObjectOutputStream} methods.
     * <p>
     * This Serializer hashes Objects using a specially tailored hash code
     * method that, in turn, is using the objects own {@code hashCode()}
     * <p>
     * If a {@code null} value is passed to the Serializer, a
     * {@link NullPointerException} will be thrown.
     *
     * @see java.io.Serializable
     */
    public static final GroupSerializer JAVA = new SerializerJava();
    public static final GroupSerializer ELSA = new SerializerElsa();
    /**
     * Serializers {@link java.util.UUID} class
     */
    public static final GroupSerializer<java.util.UUID> UUID = new SerializerUUID();
    /**
     * A predefined {@link Serializer} that handles non-null {@link Byte Bytes}
     * whereby Bytes are serialized to a one byte format.
     * <p>
     * This Serializer hashes Bytes using the original {@link Byte#hashCode()}
     * method.
     * <p>
     * If a {@code null} value is passed to the Serializer, a
     * {@link NullPointerException} will be thrown.
     *
     */
    public static final GroupSerializer<Byte> BYTE = new SerializerByte();
    /**
     * A predefined {@link Serializer} that handles non-null
     * {@link Float Floats} whereby Floats are serialized to a 4 byte format.
     * The Serializer also stores the Float's size, allowing it to be used as a
     * GroupSerializer in BTreeMaps.
     * <p>
     * This Serializer hashes Floats using the original {@link Float#hashCode()}
     * method.
     * <p>
     * If a {@code null} value is passed to the Serializer, a
     * {@link NullPointerException} will be thrown.
     *
     */
    public static final GroupSerializer<Float> FLOAT = new SerializerFloat();
    /**
     * A predefined {@link Serializer} that handles non-null
     * {@link Double Doubles} whereby Doubles are serialized to an 8 byte
     * format. The Serializer also stores the Float's size, allowing it to be
     * used as a GroupSerializer in BTreeMaps.
     * <p>
     * This Serializer hashes Doubles using the original
     * {@link Double#hashCode()} method.
     * <p>
     * If a {@code null} value is passed to the Serializer, a
     * {@link NullPointerException} will be thrown.
     *
     */
    public static final GroupSerializer<Double> DOUBLE = new SerializerDouble();
    /**
     * A predefined {@link Serializer} that handles non-null
     * {@link Short Shorts} whereby Shorts are serialized to a 2 byte format.
     * The Serializer also stores the Short's size, allowing it to be used as a
     * GroupSerializer in BTreeMaps.
     * <p>
     * This Serializer hashes Shorts using the original {@link Short#hashCode()}
     * method.
     * <p>
     * If a {@code null} value is passed to the Serializer, a
     * {@link NullPointerException} will be thrown.
     *
     */
    public static final GroupSerializer<Short> SHORT = new SerializerShort();


    /**
     * A predefined {@link Serializer} that handles non-null
     * {@link Character Characters}.
     * <p>
     * If a {@code null} value is passed to the Serializer, a
     * {@link NullPointerException} will be thrown.
     */
    public static final GroupSerializer<Character> CHAR = new SerializerChar();

    /**
     * A predefined {@link Serializer} that handles non-null
     * {@link String Strings} whereby Strings are serialized to a UTF-8 encoded
     * format. The Serializer also stores the String's size, allowing it to be
     * used as a GroupSerializer in BTreeMaps. Neighboring strings may be delta
     * encoded for increased storage efficency.
     * <p>
     * Deserialized strings are automatically interned {@link String#intern()}
     * allowing a more heap space efficient storage for repeated strings.
     * <p>
     * This Serializer hashes Strings using a specially tailored hash code
     * method as opposed to the {@link Serializers#STRING_ORIGHASH} Serializer.
     * <p>
     * If a {@code null} value is passed to the Serializer, a
     * {@link NullPointerException} will be thrown.
     *
     * @see Serializers#STRING
     */
    public static final GroupSerializer<String> STRING_INTERN = new SerializerStringIntern();


    // TODO boolean array
//    GroupSerializer<boolean[]> BOOLEAN_ARRAY = new GroupSerializer<boolean[]>() {
//        @Override
//        public void serialize(DataOutput2 out, boolean[] value) throws IOException {
//            out.packInt( value.length);//write the number of booleans not the number of bytes
//            SerializerBase.writeBooleanArray(out,value);
//        }
//
//        @Override
//        public boolean[] deserialize(DataInput2 in, int available) throws IOException {
//            int size = in.unpackInt();
//            return SerializerBase.readBooleanArray(size, in);
//        }
//
//        @Override
//        public boolean isTrusted() {
//            return true;
//        }
//
//        @Override
//        public boolean equals(boolean[] a1, boolean[] a2) {
//            return Arrays.equals(a1,a2);
//        }
//
//        @Override
//        public int hashCode(boolean[] booleans, int seed) {
//            return Arrays.hashCode(booleans);
//        }
//    };
    public static final GroupSerializer<short[]> SHORT_ARRAY = new SerializerShortArray();
    public static final GroupSerializer<float[]> FLOAT_ARRAY = new SerializerFloatArray();
    public static final GroupSerializer<BigInteger> BIG_INTEGER = new SerializerBigInteger();
    public static final GroupSerializer<BigDecimal> BIG_DECIMAL = new SerializerBigDecimal();
    public static final GroupSerializer<Class<?>> CLASS = new SerializerClass();
    public static final GroupSerializer<Date> DATE = new SerializerDate();
    public static final GroupSerializer<java.sql.Date> SQL_DATE = new SerializerSqlDate();
    public static final GroupSerializer<java.sql.Time> SQL_TIME = new SerializerSqlTime();
    public static final GroupSerializer<java.sql.Timestamp> SQL_TIMESTAMP = new SerializerSqlTimestamp();
}
