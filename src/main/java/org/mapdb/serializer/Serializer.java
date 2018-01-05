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
package org.mapdb.serializer;

import org.jetbrains.annotations.NotNull;
import org.mapdb.CC;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.hasher.Hasher;
import org.mapdb.hasher.Hashers;
import org.mapdb.util.DataIO;

import java.io.*;

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
//TODO annotate static serializers as non nullable
public interface Serializer<A>{



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
     * Serializes the content of the given value into the given
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
     *
     * @return the de-serialized content of the given {@link DataInput2}
     * @throws IOException in case of an I/O error
     */
    A deserialize(@NotNull DataInput2 input, int available) throws IOException;


    default Hasher<A> defaultHasher(){
        return Hashers.JAVA;
    }

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
        if (CC.PARANOID && available < 0 || available > 8) {
            throw new AssertionError();
        }
        byte[] b = new byte[available];
        DataIO.putLong(b, 0, input, available);
        return deserialize(new DataInput2.ByteArray(b), available);
    }

    /** Creates binary copy of given object. If the datatype is immutable the same instance might be returned */
    default A clone(A value) throws IOException {
        DataOutput2 out = new DataOutput2();
        serialize(out, value);
        DataInput2 in2 = new DataInput2.ByteArray(out.copyBytes());
        return deserialize(in2, out.pos);
    }

    /**
     * Return true if serializer only reads from stream, without doing anything else.
     * If thats case, MapDB can make some optimalizations
     *
     * @return true if serializer is self contained, and does not call anything else
     */
    default boolean isQuick(){
        return false;
    }


    //TODO remove this method
    /** returns value+1, or null if there is no bigger value. */
    default A nextValue(A value){
        throw new UnsupportedOperationException("Next Value not supported");
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
