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
 * Provides serialization and deserialization
 *
 * @author Jan Kotek
 */
public interface Serializer<A> extends Comparator<A>{


    GroupSerializer<Character> CHAR = new SerializerChar();



    /**
     * <p>
     * Serializes strings using UTF8 encoding.
     * Stores string size so can be used as collection serializer.
     * Does not handle null values
     * </p><p>
     * Unlike {@link Serializer#STRING} this method hashes String with {@link String#hashCode()}
     * </p>
     */
    GroupSerializer<String> STRING_ORIGHASH = new SerializerStringOrigHash();

    /**
     * Serializes strings using UTF8 encoding.
     * Stores string size so can be used as collection serializer.
     * Does not handle null values
     */
    GroupSerializer<String> STRING = new SerializerString();

    GroupSerializer<String> STRING_DELTA = new SerializerStringDelta();
    GroupSerializer<String> STRING_DELTA2 = new SerializerStringDelta2();



    /**
     * Serializes strings using UTF8 encoding.
     * Deserialized String is interned {@link String#intern()},
     * so it could save some memory.
     *
     * Stores string size so can be used as collection serializer.
     * Does not handle null values
     */
    GroupSerializer<String> STRING_INTERN = new SerializerStringIntern();

    /**
     * Serializes strings using ASCII encoding (8 bit character).
     * Is faster compared to UTF8 encoding.
     * Stores string size so can be used as collection serializer.
     * Does not handle null values
     */
    GroupSerializer<String> STRING_ASCII = new SerializerStringAscii();

    /**
     * Serializes strings using UTF8 encoding.
     * Used mainly for testing.
     * Does not handle null values.
     */
    Serializer<String> STRING_NOSIZE = new SerializerStringNoSize();





    /** Serializes Long into 8 bytes, used mainly for testing.
     * Does not handle null values.*/

    GroupSerializer<Long> LONG = new SerializerLong();

    /**
     *  Packs positive LONG, so smaller positive values occupy less than 8 bytes.
     *  Large and negative values could occupy 8 or 9 bytes.
     */
    GroupSerializer<Long> LONG_PACKED = new SerializerLongPacked();

    /**
     * Applies delta packing on {@code java.lang.Long}.
     * Difference between consequential numbers is also packed itself, so for small diffs it takes only single byte per
     * number.
     */
    GroupSerializer<Long> LONG_DELTA = new SerializerLongDelta();


    /** Serializes Integer into 4 bytes, used mainly for testing.
     * Does not handle null values.*/

    GroupSerializer<Integer> INTEGER = new SerializerInteger();

    /**
     *  Packs positive Integer, so smaller positive values occupy less than 4 bytes.
     *  Large and negative values could occupy 4 or 5 bytes.
     */
    GroupSerializer<Integer> INTEGER_PACKED = new SerializerIntegerPacked();


    /**
     * Applies delta packing on {@code java.lang.Integer}.
     * Difference between consequential numbers is also packed itself, so for small diffs it takes only single byte per
     * number.
     */
    GroupSerializer<Integer> INTEGER_DELTA = new SerializerIntegerDelta();


    GroupSerializer<Boolean> BOOLEAN = new SerializerBoolean();

    ;


    /** Packs recid + it adds 3bits checksum. */

    GroupSerializer<Long> RECID = new SerializerRecid();

    GroupSerializer<long[]> RECID_ARRAY = new SerializerRecidArray();

    /**
     * Always throws {@link IllegalAccessError} when invoked. Useful for testing and assertions.
     */
    GroupSerializer<Object> ILLEGAL_ACCESS = new SerializerIllegalAccess();


    /**
     * Serializes {@code byte[]} it adds header which contains size information
     */
    GroupSerializer<byte[]> BYTE_ARRAY = new SerializerByteArray();

    GroupSerializer<byte[]> BYTE_ARRAY_DELTA = new SerializerByteArrayDelta();
    GroupSerializer<byte[]> BYTE_ARRAY_DELTA2 = new SerializerByteArrayDelta2();

    /**
     * Serializes {@code byte[]} directly into underlying store
     * It does not store size, so it can not be used in Maps and other collections.
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
     * Serializes {@code double[]} it adds header which contains size information
     */
    GroupSerializer<double[]> DOUBLE_ARRAY = new SerializerDoubleArray();


    /** Serializer which uses standard Java Serialization with {@link java.io.ObjectInputStream} and {@link java.io.ObjectOutputStream} */
    GroupSerializer JAVA = new SerializerJava();

    /** Serializers {@link java.util.UUID} class */
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
     * Serialize the content of an object into a ObjectOutput
     *
     * @param out ObjectOutput to save object into
     * @param value Object to serialize
     *
     * @throws java.io.IOException in case of IO error
     */
    void serialize(@NotNull DataOutput2 out, @NotNull A value) throws IOException;


    /**
     * Deserialize the content of an object from a DataInput.
     *
     * @param input to read serialized data from
     * @param available how many bytes are available in DataInput for reading, may be -1 (in streams) or 0 (null).
     * @return deserialized object
     * @throws java.io.IOException in case of IO error
     */
    A deserialize(@NotNull DataInput2 input, int available) throws IOException;

    /**
     * Data could be serialized into record with variable size or fixed size.
     * Some optimizations can be applied to serializers with fixed size
     *
     * @return fixed size or -1 for variable size
     */
    default int fixedSize(){
        return -1;
    }

    /**
     * <p>
     * MapDB has relax record size boundary checking.
     * It expect deserializer to read exactly as many bytes as were writen during serialization.
     * If deserializer reads more bytes it might start reading others record data in store.
     * </p><p>
     * Some serializers (Kryo) have problems with this. To prevent this we can not read
     * data directly from store, but must copy them into separate {@code byte[]}.
     * So zero copy optimalizations is disabled by default, and must be explicitly enabled here.
     * </p><p>
     * This flag indicates if this serializer was 'verified' to read as many bytes as it
     * writes. It should be also much better tested etc.
     * </p>
     *
     * @return true if this serializer is well tested and writes as many bytes as it reads.
     */
    default boolean isTrusted(){
        return false;
    }

    @Override
    default int compare(A o1, A o2) {
        return ((Comparable)o1).compareTo(o2);
    }

    default boolean equals(A a1, A a2){
        return a1==a2 || (a1!=null && a1.equals(a2));
    }

    default int hashCode(@NotNull A a, int seed){
        return DBUtil.intHash(a.hashCode()+seed);
    }

    default boolean needsAvailableSizeHint(){
        return false;
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
