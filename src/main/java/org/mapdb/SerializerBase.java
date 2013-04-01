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

import java.io.*;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

import static org.mapdb.SerializationHeader.*;

/**
 * Serializer which uses 'header byte' to serialize/deserialize
 * most of classes from 'java.lang' and 'java.util' packages.
 *
 * @author Jan Kotek
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class SerializerBase implements Serializer{


    static final class knownSerializable{
        static final Set get = new HashSet(Arrays.asList(
            BTreeKeySerializer.STRING,
            BTreeKeySerializer.ZERO_OR_POSITIVE_LONG,
            BTreeKeySerializer.ZERO_OR_POSITIVE_INT,
            Utils.COMPARABLE_COMPARATOR, Utils.COMPARABLE_COMPARATOR_WITH_NULLS,

            Serializer.STRING_SERIALIZER, Serializer.LONG_SERIALIZER, Serializer.INTEGER_SERIALIZER,
            Serializer.EMPTY_SERIALIZER, Serializer.BASIC_SERIALIZER, Serializer.CRC32_CHECKSUM
    ));
    }

    public static void assertSerializable(Object o){
        if(o!=null && !(o instanceof Serializable)
                && !knownSerializable.get.contains(o)){
            throw new IllegalArgumentException("Not serializable: "+o.getClass());
        }
    }

    /**
     * Utility class similar to ArrayList, but with fast identity search.
     */
    protected final static class FastArrayList<K> {

        private int size = 0;
        private K[] elementData = (K[]) new Object[1];

        boolean forwardRefs = false;

        K get(int index) {
            if (index >= size)
                throw new IndexOutOfBoundsException();
            return elementData[index];
        }

        void add(K o) {
            if (elementData.length == size) {
                //grow array if necessary
                elementData = Arrays.copyOf(elementData, elementData.length * 2);
            }

            elementData[size] = o;
            size++;
        }

        int size() {
            return size;
        }


        /**
         * This method is reason why ArrayList is not used.
         * Search an item in list and returns its index.
         * It uses identity rather than 'equalsTo'
         * One could argue that TreeMap should be used instead,
         * but we do not expect large object trees.
         * This search is VERY FAST compared to Maps, it does not allocate
         * new instances or uses method calls.
         *
         * @param obj to find in list
         * @return index of object in list or -1 if not found
         */
        int identityIndexOf(Object obj) {
            for (int i = 0; i < size; i++) {
                if (obj == elementData[i]){
                    forwardRefs = true;
                    return i;
                }
            }
            return -1;
        }

    }




    @Override
    public void serialize(final DataOutput out, final Object obj) throws IOException {
        serialize(out, obj, null);
    }


    public void serialize(final DataOutput out, final Object obj, FastArrayList<Object> objectStack) throws IOException {

        /**try to find object on stack if it exists*/
        if (objectStack != null) {
            int indexInObjectStack = objectStack.identityIndexOf(obj);
            if (indexInObjectStack != -1) {
                //object was already serialized, just write reference to it and return
                out.write(OBJECT_STACK);
                Utils.packInt(out, indexInObjectStack);
                return;
            }
            //add this object to objectStack
            objectStack.add(obj);
        }

        final Class clazz = obj != null ? obj.getClass() : null;

        /** first try to serialize object without initializing object stack*/
        if (obj == null) {
            out.write(NULL);
            return;
        } else if (clazz == Boolean.class) {
            if ((Boolean) obj)
                out.write(BOOLEAN_TRUE);
            else
                out.write(BOOLEAN_FALSE);
            return;
        } else if (clazz == Integer.class) {
            final int val = (Integer) obj;
            writeInteger(out, val);
            return;
        } else if (clazz == Double.class) {
            double v = (Double) obj;
            if (v == -1d)
                out.write(DOUBLE_MINUS_1);
            else if (v == 0d)
                out.write(DOUBLE_0);
            else if (v == 1d)
                out.write(DOUBLE_1);
            else if (v >= 0 && v <= 255 && (int) v == v) {
                out.write(DOUBLE_255);
                out.write((int) v);
            } else if (v >= Short.MIN_VALUE && v <= Short.MAX_VALUE && (short) v == v) {
                out.write(DOUBLE_SHORT);
                out.writeShort((int) v);
            } else {
                out.write(DOUBLE_FULL);
                out.writeDouble(v);
            }
            return;
        } else if (clazz == Float.class) {
            float v = (Float) obj;
            if (v == -1f)
                out.write(FLOAT_MINUS_1);
            else if (v == 0f)
                out.write(FLOAT_0);
            else if (v == 1f)
                out.write(FLOAT_1);
            else if (v >= 0 && v <= 255 && (int) v == v) {
                out.write(FLOAT_255);
                out.write((int) v);
            } else if (v >= Short.MIN_VALUE && v <= Short.MAX_VALUE && (short) v == v) {
                out.write(FLOAT_SHORT);
                out.writeShort((int) v);

            } else {
                out.write(FLOAT_FULL);
                out.writeFloat(v);
            }
            return;
        } else if (clazz == BigInteger.class) {
            out.write(BIGINTEGER);
            byte[] buf = ((BigInteger) obj).toByteArray();
            Utils.packInt(out, buf.length);
            out.write(buf);
            return;
        } else if (clazz == BigDecimal.class) {
            out.write(BIGDECIMAL);
            BigDecimal d = (BigDecimal) obj;
            byte[] buf = d.unscaledValue().toByteArray();
            Utils.packInt(out, buf.length);
            out.write(buf);
            Utils.packInt(out, d.scale());
            return;
        } else if (clazz == Long.class) {
            final long val = (Long) obj;
            writeLong(out, val);
            return;
        } else if (clazz == Short.class) {
            short val = (Short) obj;
            if (val == -1)
                out.write(SHORT_MINUS_1);
            else if (val == 0)
                out.write(SHORT_0);
            else if (val == 1)
                out.write(SHORT_1);
            else if (val > 0 && val < 255) {
                out.write(SHORT_255);
                out.write(val);
            } else {
                out.write(SHORT_FULL);
                out.writeShort(val);
            }
            return;
        } else if (clazz == Byte.class) {
            byte val = (Byte) obj;
            if (val == -1)
                out.write(BYTE_MINUS_1);
            else if (val == 0)
                out.write(BYTE_0);
            else if (val == 1)
                out.write(BYTE_1);
            else {
                out.write(BYTE_FULL);
                out.writeByte(val);
            }
            return;
        } else if (clazz == Character.class) {
            out.write(CHAR);
            out.writeChar((Character) obj);
            return;
        } else if (clazz == String.class) {
            String s = (String) obj;
            if (s.length() == 0) {
                out.write(STRING_EMPTY);
            } else {
                out.write(STRING);
                serializeString(out, s);
            }
            return;
        } else if (obj instanceof Class) {
            out.write(CLASS);
            serializeClass(out, (Class)obj);
            return;
        } else if (obj instanceof int[]) {
            writeIntArray(out, (int[]) obj);
            return;
        } else if (obj instanceof long[]) {
            writeLongArray(out, (long[]) obj);
            return;
        } else if (obj instanceof short[]) {
            out.write(SHORT_ARRAY);
            short[] a = (short[]) obj;
            Utils.packInt(out, a.length);
            for(short s:a) out.writeShort(s);
            return;
        } else if (obj instanceof boolean[]) {
            out.write(BOOLEAN_ARRAY);
            boolean[] a = (boolean[]) obj;
            Utils.packInt(out, a.length);
            for(boolean s:a) out.writeBoolean(s); //TODO pack 8 booleans to single byte
            return;
        } else if (obj instanceof double[]) {
            out.write(DOUBLE_ARRAY);
            double[] a = (double[]) obj;
            Utils.packInt(out, a.length);
            for(double s:a) out.writeDouble(s);
            return;
        } else if (obj instanceof float[]) {
            out.write(FLOAT_ARRAY);
            float[] a = (float[]) obj;
            Utils.packInt(out, a.length);
            for(float s:a) out.writeFloat(s);
            return;
        } else if (obj instanceof char[]) {
            out.write(CHAR_ARRAY);
            char[] a = (char[]) obj;
            Utils.packInt(out, a.length);
            for(char s:a) out.writeChar(s);
            return;
        } else if (obj instanceof byte[]) {
            byte[] b = (byte[]) obj;
            serializeByteArray(out, b);
            return;
        } else if (clazz == Date.class) {
            out.write(DATE);
            out.writeLong(((Date) obj).getTime());
            return;
        } else if (clazz == UUID.class) {
            out.write(UUID);
            out.writeLong(((UUID) obj).getMostSignificantBits());
            out.writeLong(((UUID)obj).getLeastSignificantBits());
            return;
        } else if(clazz == BTreeKeySerializer.BasicKeySerializer.class){
            out.write(B_TREE_BASIC_KEY_SERIALIZER);
            if(((BTreeKeySerializer.BasicKeySerializer)obj).defaultSerializer!=this) throw new InternalError();
            return;
        } else if(clazz == CompressSerializerWrapper.class){
            out.write(SERIALIZER_COMPRESSION_WRAPPER);
            serialize(out, ((CompressSerializerWrapper)obj).serializer, objectStack);
            return;

        } else if(obj == BTreeKeySerializer.ZERO_OR_POSITIVE_LONG){
            out.write(B_TREE_SERIALIZER_POS_LONG);
            return;
        } else if(obj == BTreeKeySerializer.ZERO_OR_POSITIVE_INT){
            out.write(B_TREE_SERIALIZER_POS_INT);
            return;
        } else if(obj == Serializer.STRING_SERIALIZER){
            out.write(SerializationHeader.STRING_SERIALIZER);
            return;
        } else if(obj == Serializer.LONG_SERIALIZER){
            out.write(SerializationHeader.LONG_SERIALIZER);
            return;
        } else if(obj == Serializer.INTEGER_SERIALIZER){
            out.write(SerializationHeader.INTEGER_SERIALIZER);
            return;
        } else if(obj == Serializer.EMPTY_SERIALIZER){
            out.write(SerializationHeader.EMPTY_SERIALIZER);
            return;
        } else if(obj == Serializer.CRC32_CHECKSUM){
            out.write(SerializationHeader.CRC32_SERIALIZER);
            return;
        } else if(obj == BTreeKeySerializer.STRING){
            out.write(B_TREE_SERIALIZER_STRING);
            return;
        } else if(obj == Utils.COMPARABLE_COMPARATOR){
            out.write(COMPARABLE_COMPARATOR);
            return;
        } else if(obj == Utils.COMPARABLE_COMPARATOR_WITH_NULLS){
            out.write(COMPARABLE_COMPARATOR_WITH_NULLS);
            return;
        } else if(obj == BASIC_SERIALIZER){
            out.write(SerializationHeader.BASIC_SERIALIZER);
            return;
        } else if(obj == Fun.HI){
            out.write(FUN_HI);
        } else if(obj == this){
            out.write(THIS_SERIALIZER);
            return;
        }




        /** classes bellow need object stack, so initialize it if not alredy initialized*/
        if (objectStack == null) {
            objectStack = new FastArrayList();
            objectStack.add(obj);
        }


        if (obj instanceof Object[]) {
            Object[] b = (Object[]) obj;
            boolean packableLongs = b.length <= 255;
            boolean allNull = true;
            if (packableLongs) {
                //check if it contains packable longs
                for (Object o : b) {
                    if(o!=null){
                        allNull=false;
                        if (o.getClass() != Long.class || ((Long) o < 0 && (Long) o != Long.MAX_VALUE)) {
                            packableLongs = false;
                        }
                    }

                    if(!packableLongs && !allNull)
                        break;
                }
            }else{
                //check for all null
                for (Object o : b) {
                    if(o!=null){
                        allNull=false;
                        break;
                    }
                }
            }
            if(allNull){
                out.write(ARRAY_OBJECT_ALL_NULL);
                Utils.packInt(out, b.length);

                // Write classfor components
                Class<?> componentType = obj.getClass().getComponentType();
                serializeClass(out, componentType);

            }else if (packableLongs) {
                //packable Longs is special case,  it is often used in JDBM to reference fields
                out.write(ARRAY_OBJECT_PACKED_LONG);
                out.write(b.length);
                for (Object o : b) {
                    if (o == null)
                        Utils.packLong(out, 0);
                    else
                        Utils.packLong(out, (Long) o + 1);
                }

            } else {
                out.write(ARRAY_OBJECT);
                Utils.packInt(out, b.length);

                // Write classfor components
                Class<?> componentType = obj.getClass().getComponentType();
                serializeClass(out, componentType);

                for (Object o : b)
                    serialize(out, o, objectStack);

            }

        } else if (clazz == ArrayList.class) {
            ArrayList l = (ArrayList) obj;
            boolean packableLongs = l.size() < 255;
            if (packableLongs) {
                //packable Longs is special case,  it is often used in JDBM to reference fields
                for (Object o : l) {
                    if (o != null && (o.getClass() != Long.class || ((Long) o < 0 && (Long) o != Long.MAX_VALUE))) {
                        packableLongs = false;
                        break;
                    }
                }
            }
            if (packableLongs) {
                out.write(ARRAYLIST_PACKED_LONG);
                out.write(l.size());
                for (Object o : l) {
                    if (o == null)
                        Utils.packLong(out, 0);
                    else
                        Utils.packLong(out, (Long) o + 1);
                }
            } else {
                serializeCollection(ARRAYLIST, out, obj, objectStack);
            }

        } else if (clazz == java.util.LinkedList.class) {
            serializeCollection(LINKEDLIST, out, obj, objectStack);
        } else if (clazz == Vector.class) {
            serializeCollection(VECTOR, out, obj, objectStack);
        } else if (clazz == TreeSet.class) {
            TreeSet l = (TreeSet) obj;
            out.write(TREESET);
            Utils.packInt(out, l.size());
            serialize(out, l.comparator(), objectStack);
            for (Object o : l)
                serialize(out, o, objectStack);
        } else if (clazz == HashSet.class) {
            serializeCollection(HASHSET, out, obj, objectStack);
        } else if (clazz == LinkedHashSet.class) {
            serializeCollection(LINKEDHASHSET, out, obj, objectStack);
        } else if (clazz == TreeMap.class) {
            TreeMap l = (TreeMap) obj;
            out.write(TREEMAP);
            Utils.packInt(out, l.size());
            serialize(out, l.comparator(), objectStack);
            for (Object o : l.keySet()) {
                serialize(out, o, objectStack);
                serialize(out, l.get(o), objectStack);
            }
        } else if (clazz == HashMap.class) {
            serializeMap(HASHMAP, out, obj, objectStack);
        } else if (clazz == IdentityHashMap.class) {
            serializeMap(IDENTITYHASHMAP, out, obj, objectStack);
        } else if (clazz == LinkedHashMap.class) {
            serializeMap(LINKEDHASHMAP, out, obj, objectStack);
        } else if (clazz == Hashtable.class) {
            serializeMap(HASHTABLE, out, obj, objectStack);
        } else if (clazz == Properties.class) {
            serializeMap(PROPERTIES, out, obj, objectStack);
        } else if (clazz == Locale.class){
            out.write(LOCALE);
            Locale l = (Locale) obj;
            out.writeUTF(l.getLanguage());
            out.writeUTF(l.getCountry());
            out.writeUTF(l.getVariant());
        } else if (clazz == Fun.Tuple2.class){
            out.write(TUPLE2);
            Fun.Tuple2 t = (Fun.Tuple2) obj;
            serialize(out, t.a, objectStack);
            serialize(out, t.b, objectStack);
        } else if (clazz == Fun.Tuple2.class){
            out.write(TUPLE3);
            Fun.Tuple3 t = (Fun.Tuple3) obj;
            serialize(out, t.a, objectStack);
            serialize(out, t.b, objectStack);
            serialize(out, t.c, objectStack);
        } else if (clazz == Fun.Tuple4.class){
            out.write(TUPLE4);
            Fun.Tuple4 t = (Fun.Tuple4) obj;
            serialize(out, t.a, objectStack);
            serialize(out, t.b, objectStack);
            serialize(out, t.c, objectStack);
            serialize(out, t.d, objectStack);
        } else {
            serializeUnknownObject(out, obj, objectStack);
        }

    }


    protected void serializeClass(DataOutput out, Class clazz) throws IOException {
        //TODO override in SerializerPojo
        out.writeUTF(clazz.getName());
    }


    static void serializeString(DataOutput out, String obj) throws IOException {
        final int len = obj.length();
        Utils.packInt(out, len);
        for (int i = 0; i < len; i++) {
            int c = (int) obj.charAt(i); //TODO investigate if c could be negative here
            Utils.packInt(out, c);
        }

    }

    private void serializeMap(int header, DataOutput out, Object obj, FastArrayList<Object> objectStack) throws IOException {
        Map l = (Map) obj;
        out.write(header);
        Utils.packInt(out, l.size());
        for (Object o : l.keySet()) {
            serialize(out, o, objectStack);
            serialize(out, l.get(o), objectStack);
        }
    }

    private void serializeCollection(int header, DataOutput out, Object obj, FastArrayList<Object> objectStack) throws IOException {
        Collection l = (Collection) obj;
        out.write(header);
        Utils.packInt(out, l.size());

        for (Object o : l)
            serialize(out, o, objectStack);

    }

    private void serializeByteArray(DataOutput out, byte[] b) throws IOException {
        boolean allEqual = b.length>0;
        //check if all values in byte[] are equal
        for(int i=1;i<b.length;i++){
            if(b[i-1]!=b[i]){
                allEqual=false;
                break;
            }
        }
        if(allEqual){
            out.write(ARRAY_BYTE_ALL_EQUAL);
            Utils.packInt(out, b.length);
            out.write(b[0]);
        }else{
            out.write(ARRAY_BYTE);
            Utils.packInt(out, b.length);
            out.write(b);
        }
    }


    private void writeLongArray(DataOutput da, long[] obj) throws IOException {
        long max = Long.MIN_VALUE;
        long min = Long.MAX_VALUE;
        for (long i : obj) {
            max = Math.max(max, i);
            min = Math.min(min, i);
        }

        if (0 <= min && max <= 255) {
            da.write(ARRAY_LONG_B);
            Utils.packInt(da, obj.length);
            for (long l : obj)
                da.write((int) l);
        } else if (0 <= min) {
            da.write(ARRAY_LONG_PACKED);
            Utils.packInt(da, obj.length);
            for (long l : obj)
                Utils.packLong(da, l);
        } else if (Short.MIN_VALUE <= min && max <= Short.MAX_VALUE) {
            da.write(ARRAY_LONG_S);
            Utils.packInt(da, obj.length);
            for (long l : obj)
                da.writeShort((short) l);
        } else if (Integer.MIN_VALUE <= min && max <= Integer.MAX_VALUE) {
            da.write(ARRAY_LONG_I);
            Utils.packInt(da, obj.length);
            for (long l : obj)
                da.writeInt((int) l);
        } else {
            da.write(ARRAY_LONG_L);
            Utils.packInt(da, obj.length);
            for (long l : obj)
                da.writeLong(l);
        }

    }


    private void writeIntArray(DataOutput da, int[] obj) throws IOException {
        int max = Integer.MIN_VALUE;
        int min = Integer.MAX_VALUE;
        for (int i : obj) {
            max = Math.max(max, i);
            min = Math.min(min, i);
        }

        boolean fitsInByte = 0 <= min && max <= 255;
        boolean fitsInShort = Short.MIN_VALUE >= min && max <= Short.MAX_VALUE;


        if (obj.length <= 255 && fitsInByte) {
            da.write(ARRAY_INT_B_255);
            da.write(obj.length);
            for (int i : obj)
                da.write(i);
        } else if (fitsInByte) {
            da.write(ARRAY_INT_B_INT);
            Utils.packInt(da, obj.length);
            for (int i : obj)
                da.write(i);
        } else if (0 <= min) {
            da.write(ARRAY_INT_PACKED);
            Utils.packInt(da, obj.length);
            for (int l : obj)
                Utils.packInt(da, l);
        } else if (fitsInShort) {
            da.write(ARRAY_INT_S);
            Utils.packInt(da, obj.length);
            for (int i : obj)
                da.writeShort(i);
        } else {
            da.write(ARRAY_INT_I);
            Utils.packInt(da, obj.length);
            for (int i : obj)
                da.writeInt(i);
        }

    }


    private void writeInteger(DataOutput da, final int val) throws IOException {
        if (val == -1)
            da.write(INTEGER_MINUS_1);
        else if (val == 0)
            da.write(INTEGER_0);
        else if (val == 1)
            da.write(INTEGER_1);
        else if (val == 2)
            da.write(INTEGER_2);
        else if (val == 3)
            da.write(INTEGER_3);
        else if (val == 4)
            da.write(INTEGER_4);
        else if (val == 5)
            da.write(INTEGER_5);
        else if (val == 6)
            da.write(INTEGER_6);
        else if (val == 7)
            da.write(INTEGER_7);
        else if (val == 8)
            da.write(INTEGER_8);
        else if (val == Integer.MIN_VALUE)
            da.write(INTEGER_MINUS_MAX);
        else if (val > 0 && val < 255) {
            da.write(INTEGER_255);
            da.write(val);
        } else if (val < 0) {
            da.write(INTEGER_PACK_NEG);
            Utils.packInt(da, -val);
        } else {
            da.write(INTEGER_PACK);
            Utils.packInt(da, val);
        }
    }

    private void writeLong(DataOutput da, final long val) throws IOException {
        if (val == -1)
            da.write(LONG_MINUS_1);
        else if (val == 0)
            da.write(LONG_0);
        else if (val == 1)
            da.write(LONG_1);
        else if (val == 2)
            da.write(LONG_2);
        else if (val == 3)
            da.write(LONG_3);
        else if (val == 4)
            da.write(LONG_4);
        else if (val == 5)
            da.write(LONG_5);
        else if (val == 6)
            da.write(LONG_6);
        else if (val == 7)
            da.write(LONG_7);
        else if (val == 8)
            da.write(LONG_8);
        else if (val == Long.MIN_VALUE)
            da.write(LONG_MINUS_MAX);
        else if (val > 0 && val < 255) {
            da.write(LONG_255);
            da.write((int) val);
        } else if (val < 0) {
            da.write(LONG_PACK_NEG);
            Utils.packLong(da, -val);
        } else {
            da.write(LONG_PACK);
            Utils.packLong(da, val);
        }
    }



    static String deserializeString(DataInput buf) throws IOException {
        int len = Utils.unpackInt(buf);
        char[] b = new char[len];
        for (int i = 0; i < len; i++)
            b[i] = (char) Utils.unpackInt(buf);

        return new String(b);
    }


    @Override
    public Object deserialize(DataInput is, int capacity) throws IOException {
        if(capacity==0) return null;
        return deserialize(is, null);
    }

    public Object deserialize(DataInput is, FastArrayList<Object> objectStack) throws IOException {

        Object ret = null;

        final int head = is.readUnsignedByte();

        /** first try to deserialize object without allocating object stack*/
        switch (head) {
            case NULL:
                break;
            case BOOLEAN_TRUE:
                ret = Boolean.TRUE;
                break;
            case BOOLEAN_FALSE:
                ret = Boolean.FALSE;
                break;
            case INTEGER_MINUS_1:
                ret = -1;
                break;
            case INTEGER_0:
                ret = 0;
                break;
            case INTEGER_1:
                ret = 1;
                break;
            case INTEGER_2:
                ret = 2;
                break;
            case INTEGER_3:
                ret = 3;
                break;
            case INTEGER_4:
                ret = 4;
                break;
            case INTEGER_5:
                ret = 5;
                break;
            case INTEGER_6:
                ret = 6;
                break;
            case INTEGER_7:
                ret = 7;
                break;
            case INTEGER_8:
                ret = 8;
                break;
            case INTEGER_MINUS_MAX:
                ret = Integer.MIN_VALUE;
                break;
            case INTEGER_255:
                ret = is.readUnsignedByte();
                break;
            case INTEGER_PACK_NEG:
                ret = -Utils.unpackInt(is);
                break;
            case INTEGER_PACK:
                ret = Utils.unpackInt(is);
                break;
            case LONG_MINUS_1:
                ret = (long) -1;
                break;
            case LONG_0:
                ret = (long) 0;
                break;
            case LONG_1:
                ret = (long) 1;
                break;
            case LONG_2:
                ret = (long) 2;
                break;
            case LONG_3:
                ret = (long) 3;
                break;
            case LONG_4:
                ret = (long) 4;
                break;
            case LONG_5:
                ret = (long) 5;
                break;
            case LONG_6:
                ret = (long) 6;
                break;
            case LONG_7:
                ret = (long) 7;
                break;
            case LONG_8:
                ret = (long) 8;
                break;
            case LONG_255:
                ret = (long) is.readUnsignedByte();
                break;
            case LONG_PACK_NEG:
                ret = -Utils.unpackLong(is);
                break;
            case LONG_PACK:
                ret = Utils.unpackLong(is);
                break;
            case LONG_MINUS_MAX:
                ret = Long.MIN_VALUE;
                break;
            case SHORT_MINUS_1:
                ret = (short) -1;
                break;
            case SHORT_0:
                ret = (short) 0;
                break;
            case SHORT_1:
                ret = (short) 1;
                break;
            case SHORT_255:
                ret = (short) is.readUnsignedByte();
                break;
            case SHORT_FULL:
                ret = is.readShort();
                break;
            case BYTE_MINUS_1:
                ret = (byte) -1;
                break;
            case BYTE_0:
                ret = (byte) 0;
                break;
            case BYTE_1:
                ret = (byte) 1;
                break;
            case BYTE_FULL:
                ret = is.readByte();
                break;
            case SHORT_ARRAY:
                int size = Utils.unpackInt(is);
                ret = new short[size];
                for(int i=0;i<size;i++) ((short[])ret)[i] = is.readShort();
                break;
            case BOOLEAN_ARRAY:
                size = Utils.unpackInt(is);
                ret = new boolean[size];
                for(int i=0;i<size;i++) ((boolean[])ret)[i] = is.readBoolean();
                break;
            case DOUBLE_ARRAY:
                size = Utils.unpackInt(is);
                ret = new double[size];
                for(int i=0;i<size;i++) ((double[])ret)[i] = is.readDouble();
                break;
            case FLOAT_ARRAY:
                size = Utils.unpackInt(is);
                ret = new float[size];
                for(int i=0;i<size;i++) ((float[])ret)[i] = is.readFloat();
                break;
            case CHAR_ARRAY:
                size = Utils.unpackInt(is);
                ret = new char[size];
                for(int i=0;i<size;i++) ((char[])ret)[i] = is.readChar();
                break;
            case CHAR:
                ret = is.readChar();
                break;
            case FLOAT_MINUS_1:
                ret = (float) -1;
                break;
            case FLOAT_0:
                ret = (float) 0;
                break;
            case FLOAT_1:
                ret = (float) 1;
                break;
            case FLOAT_255:
                ret = (float) is.readUnsignedByte();
                break;
            case FLOAT_SHORT:
                ret = (float) is.readShort();
                break;
            case FLOAT_FULL:
                ret = is.readFloat();
                break;
            case DOUBLE_MINUS_1:
                ret = (double) -1;
                break;
            case DOUBLE_0:
                ret = (double) 0;
                break;
            case DOUBLE_1:
                ret = (double) 1;
                break;
            case DOUBLE_255:
                ret = (double) is.readUnsignedByte();
                break;
            case DOUBLE_SHORT:
                ret = (double) is.readShort();
                break;
            case DOUBLE_FULL:
                ret = is.readDouble();
                break;
            case BIGINTEGER:
                ret = new BigInteger(deserializeArrayByte(is));
                break;
            case BIGDECIMAL:
                ret = new BigDecimal(new BigInteger(deserializeArrayByte(is)), Utils.unpackInt(is));
                break;
            case STRING:
                ret = deserializeString(is);
                break;
            case STRING_EMPTY:
                ret = Utils.EMPTY_STRING;
                break;
            case CLASS:
                ret = deserializeClass(is);
                break;
            case DATE:
                ret = new Date(is.readLong());
                break;
            case UUID:
                ret = new UUID(is.readLong(), is.readLong());
                break;
            case ARRAY_INT_B_255:
                ret = deserializeArrayIntB255(is);
                break;
            case ARRAY_INT_B_INT:
                ret = deserializeArrayIntBInt(is);
                break;
            case ARRAY_INT_S:
                ret = deserializeArrayIntSInt(is);
                break;
            case ARRAY_INT_I:
                ret = deserializeArrayIntIInt(is);
                break;
            case ARRAY_INT_PACKED:
                ret = deserializeArrayIntPack(is);
                break;
            case ARRAY_LONG_B:
                ret = deserializeArrayLongB(is);
                break;
            case ARRAY_LONG_S:
                ret = deserializeArrayLongS(is);
                break;
            case ARRAY_LONG_I:
                ret = deserializeArrayLongI(is);
                break;
            case ARRAY_LONG_L:
                ret = deserializeArrayLongL(is);
                break;
            case ARRAY_LONG_PACKED:
                ret = deserializeArrayLongPack(is);
                break;
            case ARRAYLIST_PACKED_LONG:
                ret = deserializeArrayListPackedLong(is);
                break;
            case ARRAY_BYTE_ALL_EQUAL:
                byte[] b = new byte[Utils.unpackInt(is)];
                Arrays.fill(b, is.readByte());
                ret = b;
                break;
            case ARRAY_BYTE:
                ret =  deserializeArrayByte(is);
                break;
            case LOCALE :
                ret = new Locale(is.readUTF(),is.readUTF(),is.readUTF());
                break;
            case COMPARABLE_COMPARATOR:
                ret = Utils.COMPARABLE_COMPARATOR;
                break;
            case SerializationHeader.LONG_SERIALIZER:
                ret = LONG_SERIALIZER;
                break;
            case SerializationHeader.INTEGER_SERIALIZER:
                ret = INTEGER_SERIALIZER;
                break;
            case SerializationHeader.EMPTY_SERIALIZER:
                ret = EMPTY_SERIALIZER;
                break;
            case SerializationHeader.CRC32_SERIALIZER:
                ret = Serializer.CRC32_CHECKSUM;
                break;
            case B_TREE_SERIALIZER_POS_LONG:
                ret = BTreeKeySerializer.ZERO_OR_POSITIVE_LONG;
                break;
            case B_TREE_SERIALIZER_POS_INT:
                ret = BTreeKeySerializer.ZERO_OR_POSITIVE_INT;
                break;
            case B_TREE_SERIALIZER_STRING:
                ret = BTreeKeySerializer.STRING;
                break;
            case COMPARABLE_COMPARATOR_WITH_NULLS:
                ret = Utils.COMPARABLE_COMPARATOR_WITH_NULLS;
                break;
            case B_TREE_BASIC_KEY_SERIALIZER:
                ret = new BTreeKeySerializer.BasicKeySerializer(this);
                break;
            case SerializationHeader.BASIC_SERIALIZER:
                ret = BASIC_SERIALIZER;
                break;
            case SerializationHeader.STRING_SERIALIZER:
                ret = Serializer.STRING_SERIALIZER;
                break;
            case TUPLE2:
                ret = new Fun.Tuple2(deserialize(is, objectStack), deserialize(is, objectStack));
                break;
            case TUPLE3:
                ret = new Fun.Tuple3(deserialize(is, objectStack), deserialize(is, objectStack), deserialize(is, objectStack));
                break;
            case TUPLE4:
                ret = new Fun.Tuple4(deserialize(is, objectStack), deserialize(is, objectStack), deserialize(is, objectStack), deserialize(is, objectStack));
                break;
            case FUN_HI:
                ret = Fun.HI;
                break;
            case THIS_SERIALIZER:
                ret = this;
                break;
            case JAVA_SERIALIZATION:
                throw new InternalError("Wrong header, data were probably serialized with java.lang.ObjectOutputStream, not with JDBM serialization");
            case ARRAY_OBJECT_PACKED_LONG:
                ret = deserializeArrayObjectPackedLong(is);
                break;
            case ARRAY_OBJECT_ALL_NULL:
                ret = deserializeArrayObjectAllNull(is);
                break;
            case ARRAY_OBJECT_NO_REFS:
                ret = deserializeArrayObjectNoRefs(is);
                break;

            case -1:
                throw new EOFException();

        }

        if (ret != null || head == NULL) {
            if (objectStack != null)
                objectStack.add(ret);
            return ret;
        }

        /**  something else which needs object stack initialized*/

        if (objectStack == null)
            objectStack = new FastArrayList();
        int oldObjectStackSize = objectStack.size();

        switch (head) {
            case OBJECT_STACK:
                ret = objectStack.get(Utils.unpackInt(is));
                break;
            case ARRAYLIST:
                ret = deserializeArrayList(is, objectStack);
                break;
            case ARRAY_OBJECT:
                ret = deserializeArrayObject(is, objectStack);
                break;
            case LINKEDLIST:
                ret = deserializeLinkedList(is, objectStack);
                break;
            case TREESET:
                ret = deserializeTreeSet(is, objectStack);
                break;
            case HASHSET:
                ret = deserializeHashSet(is, objectStack);
                break;
            case LINKEDHASHSET:
                ret = deserializeLinkedHashSet(is, objectStack);
                break;
            case VECTOR:
                ret = deserializeVector(is, objectStack);
                break;
            case TREEMAP:
                ret = deserializeTreeMap(is, objectStack);
                break;
            case HASHMAP:
                ret = deserializeHashMap(is, objectStack);
                break;
            case IDENTITYHASHMAP:
                ret = deserializeIdentityHashMap(is, objectStack);
                break;
            case LINKEDHASHMAP:
                ret = deserializeLinkedHashMap(is, objectStack);
                break;
            case HASHTABLE:
                ret = deserializeHashtable(is, objectStack);
                break;
            case PROPERTIES:
                ret = deserializeProperties(is, objectStack);
                break;
            case SERIALIZER_COMPRESSION_WRAPPER:
                ret = CompressLZF.CompressionWrapper((Serializer) deserialize(is, objectStack));
                break;
            default:
                ret = deserializeUnknownHeader(is, head, objectStack);
                break;
        }

        if (head != OBJECT_STACK && objectStack.size() == oldObjectStackSize) {
            //check if object was not already added to stack as part of collection
            objectStack.add(ret);
        }


        return ret;
    }

    private byte[] deserializeArrayByte(DataInput is) throws IOException {
        byte[] bb = new byte[Utils.unpackInt(is)];
        is.readFully(bb);
        return bb;
    }


    protected  Class deserializeClass(DataInput is) throws IOException {
        //TODO override 'deserializeClass' in SerializerPojo
        try {
            return Class.forName(is.readUTF());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }




    private long[] deserializeArrayLongL(DataInput is) throws IOException {
        int size = Utils.unpackInt(is);
        long[] ret = new long[size];
        for (int i = 0; i < size; i++)
            ret[i] = is.readLong();
        return ret;
    }


    private long[] deserializeArrayLongI(DataInput is) throws IOException {
        int size = Utils.unpackInt(is);
        long[] ret = new long[size];
        for (int i = 0; i < size; i++)
            ret[i] = is.readInt();
        return ret;
    }


    private long[] deserializeArrayLongS(DataInput is) throws IOException {
        int size = Utils.unpackInt(is);
        long[] ret = new long[size];
        for (int i = 0; i < size; i++)
            ret[i] = is.readShort();
        return ret;
    }


    private long[] deserializeArrayLongB(DataInput is) throws IOException {
        int size = Utils.unpackInt(is);
        long[] ret = new long[size];
        for (int i = 0; i < size; i++) {
            ret[i] = is.readUnsignedByte();
            if (ret[i] < 0)
                throw new EOFException();
        }
        return ret;
    }


    private int[] deserializeArrayIntIInt(DataInput is) throws IOException {
        int size = Utils.unpackInt(is);
        int[] ret = new int[size];
        for (int i = 0; i < size; i++)
            ret[i] = is.readInt();
        return ret;
    }


    private int[] deserializeArrayIntSInt(DataInput is) throws IOException {
        int size = Utils.unpackInt(is);
        int[] ret = new int[size];
        for (int i = 0; i < size; i++)
            ret[i] = is.readShort();
        return ret;
    }


    private int[] deserializeArrayIntBInt(DataInput is) throws IOException {
        int size = Utils.unpackInt(is);
        int[] ret = new int[size];
        for (int i = 0; i < size; i++) {
            ret[i] = is.readUnsignedByte();
            if (ret[i] < 0)
                throw new EOFException();
        }
        return ret;
    }


    private int[] deserializeArrayIntPack(DataInput is) throws IOException {
        int size = Utils.unpackInt(is);
        if (size < 0)
            throw new EOFException();

        int[] ret = new int[size];
        for (int i = 0; i < size; i++) {
            ret[i] = Utils.unpackInt(is);
        }
        return ret;
    }

    private long[] deserializeArrayLongPack(DataInput is) throws IOException {
        int size = Utils.unpackInt(is);
        if (size < 0)
            throw new EOFException();

        long[] ret = new long[size];
        for (int i = 0; i < size; i++) {
            ret[i] = Utils.unpackLong(is);
        }
        return ret;
    }

    private int[] deserializeArrayIntB255(DataInput is) throws IOException {
        int size = is.readUnsignedByte();
        if (size < 0)
            throw new EOFException();

        int[] ret = new int[size];
        for (int i = 0; i < size; i++) {
            ret[i] = is.readUnsignedByte();
            if (ret[i] < 0)
                throw new EOFException();
        }
        return ret;
    }


    private Object[] deserializeArrayObject(DataInput is, FastArrayList<Object> objectStack) throws IOException {
        int size = Utils.unpackInt(is);
        Class clazz = deserializeClass(is);
        Object[] s = (Object[]) Array.newInstance(clazz, size);
        objectStack.add(s);
        for (int i = 0; i < size; i++){
            s[i] = deserialize(is, objectStack);
        }
        return s;
    }

    private Object[] deserializeArrayObjectNoRefs(DataInput is) throws IOException {
        int size = Utils.unpackInt(is);
        Class clazz = deserializeClass(is);
        Object[] s = (Object[]) Array.newInstance(clazz, size);
        for (int i = 0; i < size; i++){
            s[i] = deserialize(is, null);
        }
        return s;
    }


    private Object[] deserializeArrayObjectAllNull(DataInput is) throws IOException {
        int size = Utils.unpackInt(is);
        Class clazz = deserializeClass(is);
        Object[] s = (Object[]) Array.newInstance(clazz, size);
        return s;
    }


    private Object[] deserializeArrayObjectPackedLong(DataInput is) throws IOException {
        int size = is.readUnsignedByte();
        Object[] s = new Object[size];
        for (int i = 0; i < size; i++) {
            long l = Utils.unpackLong(is);
            if (l == 0)
                s[i] = null;
            else
                s[i] = l - 1;
        }
        return s;
    }


    private ArrayList<Object> deserializeArrayList(DataInput is, FastArrayList<Object> objectStack) throws IOException {
        int size = Utils.unpackInt(is);
        ArrayList<Object> s = new ArrayList<Object>(size);
        objectStack.add(s);
        for (int i = 0; i < size; i++) {
            s.add(deserialize(is, objectStack));
        }
        return s;
    }

    private ArrayList<Object> deserializeArrayListPackedLong(DataInput is) throws IOException {
        int size = is.readUnsignedByte();
        if (size < 0)
            throw new EOFException();

        ArrayList<Object> s = new ArrayList<Object>(size);
        for (int i = 0; i < size; i++) {
            long l = Utils.unpackLong(is);
            if (l == 0)
                s.add(null);
            else
                s.add(l - 1);
        }
        return s;
    }


    private java.util.LinkedList deserializeLinkedList(DataInput is, FastArrayList<Object> objectStack) throws IOException {
        int size = Utils.unpackInt(is);
        java.util.LinkedList s = new java.util.LinkedList();
        objectStack.add(s);
        for (int i = 0; i < size; i++)
            s.add(deserialize(is, objectStack));
        return s;
    }


    private Vector<Object> deserializeVector(DataInput is, FastArrayList<Object> objectStack) throws IOException {
        int size = Utils.unpackInt(is);
        Vector<Object> s = new Vector<Object>(size);
        objectStack.add(s);
        for (int i = 0; i < size; i++)
            s.add(deserialize(is, objectStack));
        return s;
    }


    private HashSet<Object> deserializeHashSet(DataInput is, FastArrayList<Object> objectStack) throws IOException {
        int size = Utils.unpackInt(is);
        HashSet<Object> s = new HashSet<Object>(size);
        objectStack.add(s);
        for (int i = 0; i < size; i++)
            s.add(deserialize(is, objectStack));
        return s;
    }


    private LinkedHashSet<Object> deserializeLinkedHashSet(DataInput is, FastArrayList<Object> objectStack) throws IOException {
        int size = Utils.unpackInt(is);
        LinkedHashSet<Object> s = new LinkedHashSet<Object>(size);
        objectStack.add(s);
        for (int i = 0; i < size; i++)
            s.add(deserialize(is, objectStack));
        return s;
    }


    private TreeSet<Object> deserializeTreeSet(DataInput is, FastArrayList<Object> objectStack) throws IOException {
        int size = Utils.unpackInt(is);
        TreeSet<Object> s = new TreeSet<Object>();
        objectStack.add(s);
        Comparator comparator = (Comparator) deserialize(is, objectStack);
        if (comparator != null)
            s = new TreeSet<Object>(comparator);

        for (int i = 0; i < size; i++)
            s.add(deserialize(is, objectStack));
        return s;
    }


    private TreeMap<Object, Object> deserializeTreeMap(DataInput is, FastArrayList<Object> objectStack) throws IOException {
        int size = Utils.unpackInt(is);

        TreeMap<Object, Object> s = new TreeMap<Object, Object>();
        objectStack.add(s);
        Comparator comparator = (Comparator) deserialize(is, objectStack);
        if (comparator != null)
            s = new TreeMap<Object, Object>(comparator);
        for (int i = 0; i < size; i++)
            s.put(deserialize(is, objectStack), deserialize(is, objectStack));
        return s;
    }


    private HashMap<Object, Object> deserializeHashMap(DataInput is, FastArrayList<Object> objectStack) throws IOException {
        int size = Utils.unpackInt(is);

        HashMap<Object, Object> s = new HashMap<Object, Object>(size);
        objectStack.add(s);
        for (int i = 0; i < size; i++)
            s.put(deserialize(is, objectStack), deserialize(is, objectStack));
        return s;
    }

    private IdentityHashMap<Object, Object> deserializeIdentityHashMap(DataInput is, FastArrayList<Object> objectStack) throws IOException {
        int size = Utils.unpackInt(is);

        IdentityHashMap<Object, Object> s = new IdentityHashMap<Object, Object>(size);
        objectStack.add(s);
        for (int i = 0; i < size; i++)
            s.put(deserialize(is, objectStack), deserialize(is, objectStack));
        return s;
    }

    private LinkedHashMap<Object, Object> deserializeLinkedHashMap(DataInput is, FastArrayList<Object> objectStack) throws IOException {
        int size = Utils.unpackInt(is);

        LinkedHashMap<Object, Object> s = new LinkedHashMap<Object, Object>(size);
        objectStack.add(s);
        for (int i = 0; i < size; i++)
            s.put(deserialize(is, objectStack), deserialize(is, objectStack));
        return s;
    }


    private Hashtable<Object, Object> deserializeHashtable(DataInput is, FastArrayList<Object> objectStack) throws IOException {
        int size = Utils.unpackInt(is);

        Hashtable<Object, Object> s = new Hashtable<Object, Object>(size);
        objectStack.add(s);
        for (int i = 0; i < size; i++)
            s.put(deserialize(is, objectStack), deserialize(is, objectStack));
        return s;
    }


    private Properties deserializeProperties(DataInput is, FastArrayList<Object> objectStack) throws IOException {
        int size = Utils.unpackInt(is);

        Properties s = new Properties();
        objectStack.add(s);
        for (int i = 0; i < size; i++)
            s.put(deserialize(is, objectStack), deserialize(is, objectStack));
        return s;
    }

    /** override this method to extend SerializerBase functionality*/
    protected void serializeUnknownObject(DataOutput out, Object obj, FastArrayList<Object> objectStack) throws IOException {
        throw new InternalError("Could not deserialize unknown object: "+obj.getClass().getName());
    }
    /** override this method to extend SerializerBase functionality*/
    protected Object deserializeUnknownHeader(DataInput is, int head, FastArrayList<Object> objectStack) throws IOException {
        throw new InternalError("Unknown serialization header: " + head);
    }


}
