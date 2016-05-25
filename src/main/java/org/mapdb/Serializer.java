/*
 *  Copyright (c) 2012 Jan Kotek
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.mapdb;

import org.jetbrains.annotations.NotNull;
import org.mapdb.serializer.*;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

/**
 * This interface specifies how Java Objects are serialized and de-serialized
 * and also how objects are compared, hashed and tested for equality for use
 * with MapDB.
 * <p>
 * Implementing classes do not have to be thread safe.
 *
 * @param <A> the type of object that the Serializer handles.
 *
 * @author Jan Kotek
 */
public interface Serializer<A /*extends Comparable<? super A>*/> extends Comparator<A> {

    /**
     * A predefined {@link Serializer} that handles non-null
     * {@link Character Characters}.
     * <p>
     * If a {@code null} value is passed to the Serializer, a
     * {@link NullPointerException} will be thrown.
     */
    GroupSerializer<Character> CHAR = new SerializerChar();

    /**
     * A predefined {@link Serializer} that handles non-null
     * {@link String Strings} whereby serialized Strings are serialized to a
     * UTF-8 encoded format. The Serializer also stores the String's size,
     * allowing it to be used as a collection serializer.
     * <p>
     * This Serializer hashes Strings using the original
     * {@link String#hashCode()} method as opposed to the
     * {@link Serializer#STRING} Serializer.
     * <p>
     * If a {@code null} value is passed to the Serializer, a
     * {@link NullPointerException} will be thrown.
     *
     * @see Serializer#STRING
     */
    GroupSerializer<String> STRING_ORIGHASH = new SerializerStringOrigHash();

    /**
     * A predefined {@link Serializer} that handles non-null
     * {@link String Strings} whereby serialized Strings are serialized to a
     * UTF-8 encoded format. The Serializer also stores the String's size,
     * allowing it to be used as a collection serializer.
     * <p>
     * This Serializer hashes Strings using a specially tailored
     * {@link String#hashCode()} method as opposed to the
     * {@link Serializer#STRING_ORIGHASH} Serializer.
     * <p>
     * If a {@code null} value is passed to the Serializer, a
     * {@link NullPointerException} will be thrown.
     *
     * @see Serializer#STRING_ORIGHASH
     */
    GroupSerializer<String> STRING = new SerializerString();

    GroupSerializer<String> STRING_DELTA = new SerializerStringDelta();
    GroupSerializer<String> STRING_DELTA2 = new SerializerStringDelta2();

    /**
     * Serializes strings using UTF8 encoding. Deserialized String is interned
     * {@link String#intern()}, so it could save some memory.
     *
     * Stores string size so can be used as collection serializer. Does not
     * handle null values
     */
    GroupSerializer<String> STRING_INTERN = new SerializerStringIntern();

    /**
     * Serializes strings using ASCII encoding (8 bit character). Is faster
     * compared to UTF8 encoding. Stores string size so can be used as
     * collection serializer. Does not handle null values
     */
    GroupSerializer<String> STRING_ASCII = new SerializerStringAscii();

    /**
     * Serializes strings using UTF8 encoding. Used mainly for testing. Does not
     * handle null values.
     */
    Serializer<String> STRING_NOSIZE = new SerializerStringNoSize();

    /**
     * Serializes Long into 8 bytes, used mainly for testing. Does not handle
     * null values.
     */
    GroupSerializer<Long> LONG = new SerializerLong();

    /**
     * Packs positive LONG, so smaller positive values occupy less than 8 bytes.
     * Large and negative values could occupy 8 or 9 bytes.
     */
    GroupSerializer<Long> LONG_PACKED = new SerializerLongPacked();

    /**
     * Applies delta packing on {@code java.lang.Long}. Difference between
     * consequential numbers is also packed itself, so for small diffs it takes
     * only single byte per number.
     */
    GroupSerializer<Long> LONG_DELTA = new SerializerLongDelta();

    /**
     * Serializes Integer into 4 bytes, used mainly for testing. Does not handle
     * null values.
     */
    GroupSerializer<Integer> INTEGER = new SerializerInteger();

    /**
     * Packs positive Integer, so smaller positive values occupy less than 4
     * bytes. Large and negative values could occupy 4 or 5 bytes.
     */
    GroupSerializer<Integer> INTEGER_PACKED = new SerializerIntegerPacked();

    /**
     * Applies delta packing on {@code java.lang.Integer}. Difference between
     * consequential numbers is also packed itself, so for small diffs it takes
     * only single byte per number.
     */
    GroupSerializer<Integer> INTEGER_DELTA = new SerializerIntegerDelta();

    GroupSerializer<Boolean> BOOLEAN = new SerializerBoolean();

    ;


    /** Packs recid + it adds 1bit checksum. */

    GroupSerializer<Long> RECID = new SerializerRecid();

    GroupSerializer<long[]> RECID_ARRAY = new SerializerRecidArray();

    /**
     * Always throws {@link IllegalAccessError} when invoked. Useful for testing
     * and assertions.
     */
    GroupSerializer<Object> ILLEGAL_ACCESS = new SerializerIllegalAccess();

    /**
     * Serializes {@code byte[]} it adds header which contains size information
     */
    GroupSerializer<byte[]> BYTE_ARRAY = new SerializerByteArray();

    GroupSerializer<byte[]> BYTE_ARRAY_DELTA = new SerializerByteArrayDelta();
    GroupSerializer<byte[]> BYTE_ARRAY_DELTA2 = new SerializerByteArrayDelta2();

    /**
     * Serializes {@code byte[]} directly into underlying store It does not
     * store size, so it can not be used in Maps and other collections.
     */
    Serializer<byte[]> BYTE_ARRAY_NOSIZE = new SerializerByteArrayNoSize();

    /**
     * Serializes {@code char[]} it adds header which contains size information
     */
    GroupSerializer<char[]> CHAR_ARRAY = new SerializerCharArray();

    /**
     * Serializes {@code int[]} it adds header which contains size information
     */
    GroupSerializer<int[]> INT_ARRAY = new SerializerIntArray();

    /**
     * Serializes {@code long[]} it adds header which contains size information
     */
    GroupSerializer<long[]> LONG_ARRAY = new SerializerLongArray();

    /**
     * Serializes {@code double[]} it adds header which contains size
     * information
     */
    GroupSerializer<double[]> DOUBLE_ARRAY = new SerializerDoubleArray();

    /**
     * Serializer which uses standard Java Serialization with
     * {@link java.io.ObjectInputStream} and {@link java.io.ObjectOutputStream}
     */
    GroupSerializer JAVA = new SerializerJava();

    GroupSerializer ELSA = new SerializerElsa();

    /**
     * Serializers {@link java.util.UUID} class
     */
    GroupSerializer<java.util.UUID> UUID = new SerializerUUID();

    GroupSerializer<Byte> BYTE = new SerializerByte();

    GroupSerializer<Float> FLOAT = new SerializerFloat();

    GroupSerializer<Double> DOUBLE = new SerializerDouble();

    GroupSerializer<Short> SHORT = new SerializerShort();

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
    GroupSerializer<short[]> SHORT_ARRAY = new SerializerShortArray();

    GroupSerializer<float[]> FLOAT_ARRAY = new SerializerFloatArray();

    GroupSerializer<BigInteger> BIG_INTEGER = new SerializerBigInteger();

    GroupSerializer<BigDecimal> BIG_DECIMAL = new SerializerBigDecimal();

    GroupSerializer<Class<?>> CLASS = new SerializerClass();

    GroupSerializer<Date> DATE = new SerializerDate();

    //    //this has to be lazily initialized due to circular dependencies
//    static final  class __BasicInstance {
//        final static GroupSerializer<Object> s = new SerializerBase();
//    }
//
//
//    /**
//     * Basic serializer for most classes in {@code java.lang} and {@code java.util} packages.
//     * It does not handle custom POJO classes. It also does not handle classes which
//     * require access to {@code DB} itself.
//     */
//    GroupSerializer<Object> BASIC = new GroupSerializer<Object>(){
//
//        @Override
//        public void serialize(DataOutput2 out, Object value) throws IOException {
//            __BasicInstance.s.serialize(out,value);
//        }
//
//        @Override
//        public Object deserialize(DataInput2 in, int available) throws IOException {
//            return __BasicInstance.s.deserialize(in,available);
//        }
//
//        @Override
//        public boolean isTrusted() {
//            return true;
//        }
//    };
//
    /**
     * Serialize the content of the given object into the given
     * {@link DataOutput2}.
     *
     * @param out DataOutput2 to save object into
     * @param value Object to serialize
     *
     * @throws IOException in case of an I/O error
     */
    void serialize(@NotNull DataOutput2 out, @NotNull A value) throws IOException;

    /**
     * Deserializes and returns the content of the given {@link DataInput2}.
     *
     * @param input DataInput2 to de-serialize data from
     * @param available how many bytes that are available in the DataInput2 for
     * reading, may be -1 (in streams) or 0 (null).
     * @return the de-serialized content of the given {@link DataInput2}
     * @throws IOException in case of an I/O error
     */
    A deserialize(@NotNull DataInput2 input, int available) throws IOException;

    /**
     * Returns the fixed size of the serialized form in bytes or -1 if the size
     * is not fixed (e.g. for Strings).
     * <p>
     * Some optimizations can be applied to serializers with a fixed size.
     *
     * @return the fixed size of the serialized form in bytes or -1 if the size
     * is not fixed
     */
    default int fixedSize() {
        return -1;
    }

    /**
     * Returns if this Serializer is trusted to always read the same number of
     * bytes as it writes for any given object being serialized/de-serialized.
     * <p>
     * MapDB has a relaxed record size boundary checking. It expects
     * deserializers to read exactly as many bytes as were written during
     * serialization. If a deserializer reads more bytes than it wrote, it might
     * start reading others record data in store.
     * <p>
     * Some serializers (Kryo) have problems with this. To prevent this, we can
     * not read data directly from a store, but we must copy them into separate
     * {@code byte[]} buffers. Thus, zero-copy optimizations are disabled by
     * default, but can be explicitly enabled here by letting this method return
     * {@code true}.
     * <p>
     * This flag indicates if this serializer was 'verified' to read as many
     * bytes as it writes. It should also be much better tested etc.
     *
     *
     * @return if this Serializer is trusted to always read the same number of
     * bytes as it writes for any given object being serialized/de-serialized
     */
    default boolean isTrusted() {
        return false;
    }

    @Override
    default int compare(A first, A second) {
        return ((Comparable) first).compareTo(second);
    }

    /**
     * Returns if the first and second arguments are equal to each other.
     * Consequently, if both arguments are {@code null}, {@code true} is
     * returned and if exactly one argument is {@code null}, {@code false} is
     * returned.
     *
     * @param first an object
     * @param second another object to be compared with the first object for
     * equality
     *
     * @return if the first and second arguments are equal to each other
     * @see Object#equals(Object)
     */
    default boolean equals(A first, A second) {
        return Objects.equals(first, second);
    }

    /**
     * Returns a hash code of a non-null argument.
     *
     * @param a an object
     * @return a hash code of a non-null argument
     * @see Object#hashCode
     */
    /**
     * Returns a hash code of a given non-null argument. The output of the
     * method is affected by the given seed, allowing protection against crafted
     * hash attacks and to provide a better distribution of hashes.
     *
     * @param o an object
     * @param seed used to "scramble" the
     * @return a hash code of a non-null argument
     * @see Object#hashCode
     * @throws NullPointerException if the
     */
    default int hashCode(@NotNull A o, int seed) {
        return DataIO.intHash(o.hashCode() + seed);
    }

    /**
     * TODO: Document this method
     *
     * @return
     */
    default boolean needsAvailableSizeHint() {
        return false;
    }

    /**
     * Deserializes and returns the content of the given long.
     *
     * @param input long to de-serialize data from
     * @param available how many bytes that are available in the long for
     * reading, or 0 (null).
     * @return the de-serialized content of the given long
     * @throws IOException in case of an I/O error
     */
    default A deserializeFromLong(long input, int available) throws IOException {
        if (CC.ASSERT && available < 0 || available > 8) {
            throw new AssertionError();
        }
        byte[] b = new byte[available];
        DataIO.putLong(b, 0, input, available);
        return deserialize(new DataInput2.ByteArray(b), available);
    }

//
// TODO code from 2.0, perhaps it will be useful, do performance benchmarks etc
//    /**
//     * Find the first children node with a key equal or greater than the given key.
//     * If all items are smaller it returns {@code keyser.length(keys)}
//     *
//     * @param node BTree Node to find position in
//     * @param key key whose position needs to be find
//     * @return position of key in node
//     */
//    public int findChildren(final BTreeMap.BNode node, final Object key) {
//        KEYS keys = (KEYS) node.keys;
//        int keylen = length(keys);
//        int left = 0;
//        int right = keylen;
//
//        int middle;
//        //$DELAY$
//        // binary search
//        for(;;) {
//            //$DELAY$
//            middle = (left + right) / 2;
//            if(middle==keylen)
//                return middle+node.leftEdgeInc(); //null is positive infinitive
//            if (compareIsSmaller(keys,middle, (KEY) key)) {
//                left = middle + 1;
//            } else {
//                right = middle;
//            }
//            if (left >= right) {
//                return  right+node.leftEdgeInc();
//            }
//        }
//    }
//
//    public int findChildren2(final BTreeMap.BNode node, final Object key) {
//        KEYS keys = (KEYS) node.keys;
//        int keylen = length(keys);
//
//        int left = 0;
//        int right = keylen;
//        int comp;
//        int middle;
//        //$DELAY$
//        // binary search
//        while (true) {
//            //$DELAY$
//            middle = (left + right) / 2;
//            if(middle==keylen)
//                return -1-(middle+node.leftEdgeInc()); //null is positive infinitive
//            comp = compare(keys, middle, (KEY) key);
//            if(comp==0){
//                //try one before last, in some cases it might be duplicate of last
//                if(!node.isRightEdge() && middle==keylen-1 && middle>0
//                        && compare(keys,middle-1,(KEY)key)==0){
//                    middle--;
//                }
//                return middle+node.leftEdgeInc();
//            } else if ( comp< 0) {
//                left = middle +1;
//            } else {
//                right = middle;
//            }
//            if (left >= right) {
//                return  -1-(right+node.leftEdgeInc());
//            }
//        }
//
//    }
}
