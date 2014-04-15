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

/**
 * Serializer which uses 'header byte' to serialize/deserialize
 * most of classes from 'java.lang' and 'java.util' packages.
 *
 * @author Jan Kotek
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class SerializerBase implements Serializer<Object>{


    protected static final String EMPTY_STRING = "";


    /**
     * Utility class similar to ArrayList, but with fast identity search.
     */
    protected final static class FastArrayList<K> {

        public int size ;
        public K[] data ;

        public FastArrayList(){
            size=0;
            data = (K[]) new Object[1];
        }

        public boolean forwardRefs = false;


        public void add(K o) {
            if (data.length == size) {
                //grow array if necessary
                data = Arrays.copyOf(data, data.length * 2);
            }

            data[size] = o;
            size++;
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
        public int identityIndexOf(Object obj) {
            for (int i = 0; i < size; i++) {
                if (obj == data[i]){
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
                out.write(Header.OBJECT_STACK);
                DataOutput2.packInt(out, indexInObjectStack);
                return;
            }
            //add this object to objectStack
            objectStack.add(obj);
        }

        if (obj == null) {
            out.write(Header.NULL);
            return;
        }

        final Class clazz = obj.getClass();

        /** first try to serialize object without initializing object stack*/
        if (clazz == Integer.class) {
            serializeInt(out,  obj);
            return;
        } else if (clazz == Long.class) {
            serializeLong(out,  obj);
            return;
        } else if (clazz == String.class) {
            serializeString(out,  obj);
            return;
        }else if (clazz == Boolean.class) {
            out.write(((Boolean)obj)?Header.BOOLEAN_TRUE:Header.BOOLEAN_FALSE);
            return;
        } else if (clazz == Byte.class) {
            serializeByte(out,  obj);
            return;
        } else if (clazz == Character.class) {
            serializerChar(out,  obj);
            return;
        } else if (clazz == Short.class) {
            serializeShort(out,  obj);
            return;
        } else if (clazz == Float.class) {
            serializeFloat(out,  obj);
            return;
        } else if (clazz == Double.class) {
            serializeDouble(out,  obj);
            return;
        }

        serialize2(out, obj, objectStack, clazz);

    }

    private void serialize2(DataOutput out, Object obj, FastArrayList<Object> objectStack, Class<?> clazz) throws IOException {
        if (obj instanceof byte[]) {
            byte[] b = (byte[]) obj;
            serializeByteArray(out, b);
            return;

        } else if (obj instanceof boolean[]) {
            out.write(Header.ARRAY_BOOLEAN);
            boolean[] a_bool = (boolean[]) obj;
            DataOutput2.packInt(out, a_bool.length);//write the number of booleans not the number of bytes
            byte[] a = booleanToByteArray(a_bool);
            out.write(a);
            return;
        } else if (obj instanceof short[]) {
            out.write(Header.ARRAY_SHORT);
            short[] a = (short[]) obj;
            DataOutput2.packInt(out, a.length);
            for(short s:a) out.writeShort(s);
            return;
        } else if (obj instanceof char[]) {
            out.write(Header.ARRAY_CHAR);
            char[] a = (char[]) obj;
            DataOutput2.packInt(out, a.length);
            for(char s:a) out.writeChar(s);
            return;
        } else if (obj instanceof float[]) {
            out.write(Header.ARRAY_FLOAT);
            float[] a = (float[]) obj;
            DataOutput2.packInt(out, a.length);
            for(float s:a) out.writeFloat(s);
            return;
        } else if (obj instanceof double[]) {
            out.write(Header.ARRAY_DOUBLE);
            double[] a = (double[]) obj;
            DataOutput2.packInt(out, a.length);
            for(double s:a) out.writeDouble(s);
            return;
        } else if (obj instanceof int[]) {
            serializeIntArray(out, (int[]) obj);
            return;
        } else if (obj instanceof long[]) {
            serializeLongArray(out, (long[]) obj);
            return;
        } else if (clazz == BigInteger.class) {
            out.write(Header.BIGINTEGER);
            byte[] buf = ((BigInteger) obj).toByteArray();
            DataOutput2.packInt(out, buf.length);
            out.write(buf);
            return;
        } else if (clazz == BigDecimal.class) {
            out.write(Header.BIGDECIMAL);
            BigDecimal d = (BigDecimal) obj;
            byte[] buf = d.unscaledValue().toByteArray();
            DataOutput2.packInt(out, buf.length);
            out.write(buf);
            DataOutput2.packInt(out, d.scale());
            return;
        } else if (obj instanceof Class) {
            out.write(Header.CLASS);
            serializeClass(out, (Class) obj);
            return;
        } else if (clazz == Date.class) {
            out.write(Header.DATE);
            out.writeLong(((Date) obj).getTime());
            return;
        } else if (clazz == UUID.class) {
            out.write(Header.UUID);
            out.writeLong(((UUID) obj).getMostSignificantBits());
            out.writeLong(((UUID) obj).getLeastSignificantBits());
            return;
        } else if(obj == Fun.HI){
            out.write(Header.FUN_HI);
            return;
        }

        /*
         * MapDB classes
         */
        //try mapdb singletons
        final Integer mapdbSingletonHeader = singletons.all.get(obj);
        if(mapdbSingletonHeader!=null){
            out.write(Header.MAPDB);
            DataOutput2.packInt(out, mapdbSingletonHeader);
            return;
        }


        if(obj instanceof Atomic.Long ){
            out.write(Header.MA_LONG);
            DataOutput2.packLong(out,((Atomic.Long) obj).recid);
            return;
        }else if(obj instanceof Atomic.Integer ){
            out.write(Header.MA_INT);
            DataOutput2.packLong(out, ((Atomic.Integer) obj).recid);
            return;
        }else if(obj instanceof Atomic.Boolean ){
            out.write(Header.MA_BOOL);
            DataOutput2.packLong(out, ((Atomic.Boolean) obj).recid);
            return;
        }else if(obj instanceof Atomic.String ){
            out.write(Header.MA_STRING);
            DataOutput2.packLong(out, ((Atomic.String) obj).recid);
            return;
        } else if(obj == this){
            out.write(Header.MAPDB);
            DataOutput2.packInt(out, HeaderMapDB.THIS_SERIALIZER);
            return;
        }


        /** classes bellow need object stack, so initialize it if not already initialized*/
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
                out.write(Header.ARRAY_OBJECT_ALL_NULL);
                DataOutput2.packInt(out, b.length);

                // Write classfor components
                Class<?> componentType = obj.getClass().getComponentType();
                serializeClass(out, componentType);

            }else if (packableLongs) {
                //packable Longs is special case,  it is often used in MapDB to reference fields
                out.write(Header.ARRAY_OBJECT_PACKED_LONG);
                out.write(b.length);
                for (Object o : b) {
                    if (o == null)
                        DataOutput2.packLong(out, 0);
                    else
                        DataOutput2.packLong(out, (Long) o + 1);
                }

            } else {
                out.write(Header.ARRAY_OBJECT);
                DataOutput2.packInt(out, b.length);

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
                //packable Longs is special case,  it is often used in MapDB to reference fields
                for (Object o : l) {
                    if (o != null && (o.getClass() != Long.class || ((Long) o < 0 && (Long) o != Long.MAX_VALUE))) {
                        packableLongs = false;
                        break;
                    }
                }
            }
            if (packableLongs) {
                out.write(Header.ARRAYLIST_PACKED_LONG);
                out.write(l.size());
                for (Object o : l) {
                    if (o == null)
                        DataOutput2.packLong(out, 0);
                    else
                        DataOutput2.packLong(out, (Long) o + 1);
                }
            } else {
                serializeCollection(Header.ARRAYLIST, out, obj, objectStack);
            }

        } else if (clazz == LinkedList.class) {
            serializeCollection(Header.LINKEDLIST, out, obj, objectStack);
        } else if (clazz == TreeSet.class) {
            TreeSet l = (TreeSet) obj;
            out.write(Header.TREESET);
            DataOutput2.packInt(out, l.size());
            serialize(out, l.comparator(), objectStack);
            for (Object o : l)
                serialize(out, o, objectStack);
        } else if (clazz == HashSet.class) {
            serializeCollection(Header.HASHSET, out, obj, objectStack);
        } else if (clazz == LinkedHashSet.class) {
            serializeCollection(Header.LINKEDHASHSET, out, obj, objectStack);
        } else if (clazz == TreeMap.class) {
            TreeMap l = (TreeMap) obj;
            out.write(Header.TREEMAP);
            DataOutput2.packInt(out, l.size());
            serialize(out, l.comparator(), objectStack);
            for (Object o : l.keySet()) {
                serialize(out, o, objectStack);
                serialize(out, l.get(o), objectStack);
            }
        } else if (clazz == HashMap.class) {
            serializeMap(Header.HASHMAP, out, obj, objectStack);
        } else if (clazz == LinkedHashMap.class) {
            serializeMap(Header.LINKEDHASHMAP, out, obj, objectStack);
        } else if (clazz == Properties.class) {
            serializeMap(Header.PROPERTIES, out, obj, objectStack);
        } else if (clazz == Fun.Tuple2.class){
            out.write(Header.TUPLE2);
            Fun.Tuple2 t = (Fun.Tuple2) obj;
            serialize(out, t.a, objectStack);
            serialize(out, t.b, objectStack);
        } else if (clazz == Fun.Tuple3.class){
            out.write(Header.TUPLE3);
            Fun.Tuple3 t = (Fun.Tuple3) obj;
            serialize(out, t.a, objectStack);
            serialize(out, t.b, objectStack);
            serialize(out, t.c, objectStack);
        } else if (clazz == Fun.Tuple4.class){
            out.write(Header.TUPLE4);
            Fun.Tuple4 t = (Fun.Tuple4) obj;
            serialize(out, t.a, objectStack);
            serialize(out, t.b, objectStack);
            serialize(out, t.c, objectStack);
            serialize(out, t.d, objectStack);
        } else if (clazz == Fun.Tuple5.class){
            out.write(Header.TUPLE5);
            Fun.Tuple5 t = (Fun.Tuple5) obj;
            serialize(out, t.a, objectStack);
            serialize(out, t.b, objectStack);
            serialize(out, t.c, objectStack);
            serialize(out, t.d, objectStack);
            serialize(out, t.e, objectStack);
        } else if (clazz == Fun.Tuple6.class){
            out.write(Header.TUPLE6);
            Fun.Tuple6 t = (Fun.Tuple6) obj;
            serialize(out, t.a, objectStack);
            serialize(out, t.b, objectStack);
            serialize(out, t.c, objectStack);
            serialize(out, t.d, objectStack);
            serialize(out, t.e, objectStack);
            serialize(out, t.f, objectStack);
        } else if (clazz == BTreeKeySerializer.Tuple2KeySerializer.class){
            out.write(Header.MAPDB);
            DataOutput2.packInt(out, HeaderMapDB.SERIALIZER_KEY_TUPLE2);
            BTreeKeySerializer.Tuple2KeySerializer s = (BTreeKeySerializer.Tuple2KeySerializer) obj;
            serialize(out, s.aComparator,objectStack);
            serialize(out, s.aSerializer,objectStack);
            serialize(out, s.bSerializer,objectStack);
        } else if (clazz == BTreeKeySerializer.Tuple3KeySerializer.class){
            out.write(Header.MAPDB);
            DataOutput2.packInt(out, HeaderMapDB.SERIALIZER_KEY_TUPLE3);
            BTreeKeySerializer.Tuple3KeySerializer s = (BTreeKeySerializer.Tuple3KeySerializer) obj;
            serialize(out, s.aComparator,objectStack);
            serialize(out, s.bComparator,objectStack);
            serialize(out, s.aSerializer, objectStack);
            serialize(out, s.bSerializer, objectStack);
            serialize(out, s.cSerializer,objectStack);
        } else if (clazz == BTreeKeySerializer.Tuple4KeySerializer.class){
            out.write(Header.MAPDB);
            DataOutput2.packInt(out, HeaderMapDB.SERIALIZER_KEY_TUPLE4);
            BTreeKeySerializer.Tuple4KeySerializer s = (BTreeKeySerializer.Tuple4KeySerializer) obj;
            serialize(out, s.aComparator,objectStack);
            serialize(out, s.bComparator,objectStack);
            serialize(out, s.cComparator,objectStack);
            serialize(out, s.aSerializer,objectStack);
            serialize(out, s.bSerializer, objectStack);
            serialize(out, s.cSerializer, objectStack);
            serialize(out, s.dSerializer,objectStack);
        } else if (clazz == BTreeKeySerializer.Tuple5KeySerializer.class){
            out.write(Header.MAPDB);
            DataOutput2.packInt(out, HeaderMapDB.SERIALIZER_KEY_TUPLE5);
            BTreeKeySerializer.Tuple5KeySerializer s = (BTreeKeySerializer.Tuple5KeySerializer) obj;
            serialize(out, s.aComparator,objectStack);
            serialize(out, s.bComparator,objectStack);
            serialize(out, s.cComparator,objectStack);
            serialize(out, s.dComparator,objectStack);
            serialize(out, s.aSerializer,objectStack);
            serialize(out, s.bSerializer, objectStack);
            serialize(out, s.cSerializer, objectStack);
            serialize(out, s.dSerializer,objectStack);
            serialize(out, s.eSerializer,objectStack);
        } else if (clazz == BTreeKeySerializer.Tuple6KeySerializer.class){
            out.write(Header.MAPDB);
            DataOutput2.packInt(out, HeaderMapDB.SERIALIZER_KEY_TUPLE6);
            BTreeKeySerializer.Tuple6KeySerializer s = (BTreeKeySerializer.Tuple6KeySerializer) obj;
            serialize(out, s.aComparator,objectStack);
            serialize(out, s.bComparator,objectStack);
            serialize(out, s.cComparator,objectStack);
            serialize(out, s.dComparator,objectStack);
            serialize(out, s.eComparator,objectStack);
            serialize(out, s.aSerializer,objectStack);
            serialize(out, s.bSerializer,objectStack);
            serialize(out, s.cSerializer, objectStack);
            serialize(out, s.dSerializer, objectStack);
            serialize(out, s.eSerializer,objectStack);
            serialize(out, s.fSerializer,objectStack);
        }else if(clazz == BTreeKeySerializer.BasicKeySerializer.class){
            out.write(Header.MAPDB);
            DataOutput2.packInt(out, HeaderMapDB.B_TREE_BASIC_KEY_SERIALIZER);
            serialize(out,((BTreeKeySerializer.BasicKeySerializer)obj).defaultSerializer,objectStack);
        } else if (clazz == Fun.ArrayComparator.class){
            out.write(Header.MAPDB);
            DataOutput2.packInt(out, HeaderMapDB.COMPARATOR_ARRAY);
            serialize(out, ((Fun.ArrayComparator)obj).comparators,objectStack);
        } else if (clazz == CompressionWrapper.class){
            out.write(Header.MAPDB);
            DataOutput2.packInt(out, HeaderMapDB.SERIALIZER_COMPRESSION_WRAPPER);
            serialize(out, ((CompressionWrapper)obj).serializer,objectStack);
        }else if(obj instanceof Fun.Tuple2Comparator ){
            out.write(Header.MAPDB);
            DataOutput2.packInt(out, HeaderMapDB.TUPLE2_COMPARATOR);
            Fun.Tuple2Comparator c = (Fun.Tuple2Comparator) obj;
            serialize(out,c.a,objectStack );
            serialize(out,c.b,objectStack );
        }else if(obj instanceof Fun.Tuple3Comparator ){
            out.write(Header.MAPDB);
            DataOutput2.packInt(out, HeaderMapDB.TUPLE3_COMPARATOR);
            Fun.Tuple3Comparator c = (Fun.Tuple3Comparator) obj;
            serialize(out,c.a,objectStack );
            serialize(out,c.b,objectStack );
            serialize(out,c.c,objectStack );
        }else if(obj instanceof Fun.Tuple4Comparator ){
            out.write(Header.MAPDB);
            DataOutput2.packInt(out, HeaderMapDB.TUPLE4_COMPARATOR);
            Fun.Tuple4Comparator c = (Fun.Tuple4Comparator) obj;
            serialize(out,c.a,objectStack );
            serialize(out,c.b,objectStack );
            serialize(out,c.c,objectStack );
            serialize(out,c.d,objectStack );
        }else if(obj instanceof Fun.Tuple5Comparator ){
            out.write(Header.MAPDB);
            DataOutput2.packInt(out, HeaderMapDB.TUPLE5_COMPARATOR);
            Fun.Tuple5Comparator c = (Fun.Tuple5Comparator) obj;
            serialize(out,c.a,objectStack );
            serialize(out,c.b,objectStack );
            serialize(out,c.c,objectStack );
            serialize(out,c.d,objectStack );
            serialize(out,c.e,objectStack );
        }else if(obj instanceof Fun.Tuple6Comparator ){
            out.write(Header.MAPDB);
            DataOutput2.packInt(out, HeaderMapDB.TUPLE6_COMPARATOR);
            Fun.Tuple6Comparator c = (Fun.Tuple6Comparator) obj;
            serialize(out,c.a,objectStack );
            serialize(out,c.b,objectStack );
            serialize(out,c.c,objectStack );
            serialize(out,c.d,objectStack );
            serialize(out,c.e,objectStack );
            serialize(out,c.f,objectStack );
        }else if(obj instanceof Atomic.Var ){
            out.write(Header.MA_VAR);
            Atomic.Var v = (Atomic.Var) obj;
            DataOutput2.packLong(out,v.recid);
            serialize(out, v.serializer,objectStack);
        } else {
            serializeUnknownObject(out, obj, objectStack);
        }
    }

    private void serializeString(DataOutput out, Object obj) throws IOException {
        String val = (String) obj;
        int len = val.length();
        if(len == 0){
            out.write(Header.STRING_0);
        }else{
            if (len<=10){
                out.write(Header.STRING_0+len);
            }else{
                out.write(Header.STRING);
                DataOutput2.packInt(out, len);
            }
            for (int i = 0; i < len; i++)
                DataOutput2.packInt(out,(int)((String) obj).charAt(i));
        }
    }

    private void serializeLongArray(DataOutput out, final long[] val) throws IOException {

        long max = Long.MIN_VALUE;
        long min = Long.MAX_VALUE;
        for (long i : val) {
            max = Math.max(max, i);
            min = Math.min(min, i);
        }
        if (Byte.MIN_VALUE<=min && max<=Byte.MAX_VALUE) {
            out.write(Header.ARRAY_LONG_BYTE);
            DataOutput2.packInt(out, val.length);
            for (long i : val) out.write((int) i);
        }else if (Short.MIN_VALUE <= min && max <= Short.MAX_VALUE){
            out.write(Header.ARRAY_LONG_SHORT);
            DataOutput2.packInt(out, val.length);
            for (long i : val) out.writeShort((int) i);
        } else if (0 <= min) {
            out.write(Header.ARRAY_LONG_PACKED);
            DataOutput2.packInt(out, val.length);
            for (long l : val) DataOutput2.packLong(out, l);
        }else if (Integer.MIN_VALUE <= min && max <= Integer.MAX_VALUE){
            out.write(Header.ARRAY_LONG_INT);
            DataOutput2.packInt(out, val.length);
            for (long i : val) out.writeInt((int) i);
        } else {
            out.write(Header.ARRAY_LONG);
            DataOutput2.packInt(out, val.length);
            for (long i : val) out.writeLong(i);
        }
    }

    private void serializeIntArray(DataOutput out, final int[] val) throws IOException {
        int max = Integer.MIN_VALUE;
        int min = Integer.MAX_VALUE;
        for (int i : val) {
            max = Math.max(max, i);
            min = Math.min(min, i);
        }
        if (Byte.MIN_VALUE<=min && max<=Byte.MAX_VALUE) {
            out.write(Header.ARRAY_INT_BYTE);
            DataOutput2.packInt(out, val.length);
            for (int i : val) out.write(i);
        }else if (Short.MIN_VALUE <= min && max <= Short.MAX_VALUE){
            out.write(Header.ARRAY_INT_SHORT);
            DataOutput2.packInt(out, val.length);
            for (int i : val) out.writeShort(i);
        } else if (0 <= min) {
            out.write(Header.ARRAY_INT_PACKED);
            DataOutput2.packInt(out, val.length);
            for (int l : val) DataOutput2.packInt(out, l);
        } else {
            out.write(Header.ARRAY_INT);
            DataOutput2.packInt(out, val.length);
            for (int i : val) out.writeInt(i);
        }
    }

    private void serializeDouble(DataOutput out, Object obj) throws IOException {
        double v = (Double) obj;
        if (v == -1D){
            out.write(Header.DOUBLE_M1);
        }else if (v == 0D){
            out.write(Header.DOUBLE_0);
        }else if (v == 1D){
            out.write(Header.DOUBLE_1);
        }else if (v >= 0 && v <= 255 && (int) v == v) {
            out.write(Header.DOUBLE_255);
            out.write((int) v);
        } else if (v >= Short.MIN_VALUE && v <= Short.MAX_VALUE && (short) v == v) {
            out.write(Header.DOUBLE_SHORT);
            out.writeShort((int) v);
        } else if (v >= Integer.MIN_VALUE && v <= Integer.MAX_VALUE && (int) v == v) {
            out.write(Header.DOUBLE_INT);
            out.writeInt((int) v);
        } else {
            out.write(Header.DOUBLE);
            out.writeDouble(v);
        }
    }

    private void serializeFloat(DataOutput out, Object obj) throws IOException {
        float v = (Float) obj;
        if (v == -1f)
            out.write(Header.FLOAT_M1);
        else if (v == 0f)
            out.write(Header.FLOAT_0);
        else if (v == 1f)
            out.write(Header.FLOAT_1);
        else if (v >= 0 && v <= 255 && (int) v == v) {
            out.write(Header.FLOAT_255);
            out.write((int) v);
        } else if (v >= Short.MIN_VALUE && v <= Short.MAX_VALUE && (short) v == v) {
            out.write(Header.FLOAT_SHORT);
            out.writeShort((int) v);
        } else {
            out.write(Header.FLOAT);
            out.writeFloat(v);
        }
    }

    private void serializeShort(DataOutput out, Object obj) throws IOException {
        short val = (Short) obj;
        if (val == -1){
            out.write(Header.SHORT_M1);
        }else if (val == 0){
            out.write(Header.SHORT_0);
        }else if (val == 1){
            out.write(Header.SHORT_1);
        }else if (val > 0 && val < 255) {
            out.write(Header.SHORT_255);
            out.write(val);
        }else if (val < 0 && val > -255) {
            out.write(Header.SHORT_M255);
            out.write(-val);
        } else {
            out.write(Header.SHORT);
            out.writeShort(val);
        }
    }

    private void serializerChar(DataOutput out, Object obj) throws IOException {
        char val = (Character)obj;
        if(val==0){
            out.write(Header.CHAR_0);
        }else if(val==1){
            out.write(Header.CHAR_1);
        }else if (val<=255){
            out.write(Header.CHAR_255);
            out.write(val);
        }else{
            out.write(Header.CHAR);
            out.writeChar((Character) obj);
        }
    }

    private void serializeByte(DataOutput out, Object obj) throws IOException {
        byte val = (Byte) obj;
        if (val == -1)
            out.write(Header.BYTE_M1);
        else if (val == 0)
            out.write(Header.BYTE_0);
        else if (val == 1)
            out.write(Header.BYTE_1);
        else {
            out.write(Header.BYTE);
            out.writeByte(val);
        }
    }

    private void serializeLong(DataOutput out, Object obj) throws IOException {
        long val = (Long) obj;
        if(val>=-9 && val<=16){
            out.write((int) (Header.LONG_M9 + (val + 9)));
            return;
        }else if (val == Long.MIN_VALUE){
            out.write(Header.LONG_MIN_VALUE);
            return;
        }else if (val == Long.MAX_VALUE){
            out.write(Header.LONG_MAX_VALUE);
            return;
        } else if(((Math.abs(val)>>>56)&0xFF)!=0){
            out.write(Header.LONG);
            out.writeLong(val);
            return;
        }

        int neg = 0;
        if(val<0){
            neg = -1;
            val =-val;
        }

        //calculate N bytes
        int size = 48;
        while(((val>>size)&0xFFL)==0 ){
            size-=8;
        }

        //write header
        out.write(Header.LONG_F1 + (size/8)*2 + neg);

        //write data
        while(size>=0){
            out.write((int) ((val>>size)&0xFFL));
            size-=8;
        }
    }

    private void serializeInt(DataOutput out, Object obj) throws IOException {
        int val = (Integer) obj;

        switch(val){
            case -9:
            case -8:
            case -7:
            case -6:
            case -5:
            case -4:
            case -3:
            case -2:
            case -1:
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
            case 8:
            case 9:
            case 10:
            case 11:
            case 12:
            case 13:
            case 14:
            case 15:
            case 16:
                out.write( (Header.INT_M9 + (val + 9)));
                return;
            case Integer.MIN_VALUE:
                out.write(Header.INT_MIN_VALUE);
                return;
            case Integer.MAX_VALUE:
                out.write(Header.INT_MAX_VALUE);
                return;

        }
        if(((Math.abs(val)>>>24)&0xFF)!=0){
            out.write(Header.INT);
            out.writeInt(val);
            return;
        }

        int neg = 0;
        if(val<0){
            neg = -1;
            val =-val;
        }

        //calculate N bytes
        int size = 24;
        while(((val>>size)&0xFFL)==0 ){
            size-=8;
        }

        //write header
        out.write(Header.INT_F1 + (size/8)*2 + neg);

        //write data
        while(size>=0){
            out.write((int) ((val>>size)&0xFFL));
            size-=8;
        }
    }

    protected void serializeClass(DataOutput out, Class clazz) throws IOException {
        //TODO override in SerializerPojo
        out.writeUTF(clazz.getName());
    }


    private void serializeMap(int header, DataOutput out, Object obj, FastArrayList<Object> objectStack) throws IOException {
        Map l = (Map) obj;
        out.write(header);
        DataOutput2.packInt(out, l.size());
        for (Object o : l.keySet()) {
            serialize(out, o, objectStack);
            serialize(out, l.get(o), objectStack);
        }
    }

    private void serializeCollection(int header, DataOutput out, Object obj, FastArrayList<Object> objectStack) throws IOException {
        Collection l = (Collection) obj;
        out.write(header);
        DataOutput2.packInt(out, l.size());

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
            out.write(Header.ARRAY_BYTE_ALL_EQUAL);
            DataOutput2.packInt(out, b.length);
            out.write(b[0]);
        }else{
            out.write(Header.ARRAY_BYTE);
            DataOutput2.packInt(out, b.length);
            out.write(b);
        }
    }


    static String deserializeString(DataInput buf, int len) throws IOException {
        char[] b = new char[len];
        for (int i = 0; i < len; i++)
            b[i] = (char) DataInput2.unpackInt(buf);

        return new String(b);
    }


    @Override
    public Object deserialize(DataInput is, int capacity) throws IOException {
        if(capacity==0) return null;
        return deserialize(is, null);
    }

    public Object deserialize(DataInput is, FastArrayList<Object> objectStack) throws IOException {

        Object ret = null;

        int ir = 0;
        long lr = 0;
        final int head = is.readUnsignedByte();

        /** first try to deserialize object without allocating object stack*/
        switch (head) {
            case Header.ZERO_FAIL:
                throw new IOError(new IOException("Zero Header, data corrupted"));
            case Header.NULL:
                break;
            case Header.BOOLEAN_TRUE:
                ret = Boolean.TRUE;
                break;
            case Header.BOOLEAN_FALSE:
                ret = Boolean.FALSE;
                break;
            case Header.INT_M9:
            case Header.INT_M8:
            case Header.INT_M7:
            case Header.INT_M6:
            case Header.INT_M5:
            case Header.INT_M4:
            case Header.INT_M3:
            case Header.INT_M2:
            case Header.INT_M1:
            case Header.INT_0:
            case Header.INT_1:
            case Header.INT_2:
            case Header.INT_3:
            case Header.INT_4:
            case Header.INT_5:
            case Header.INT_6:
            case Header.INT_7:
            case Header.INT_8:
            case Header.INT_9:
            case Header.INT_10:
            case Header.INT_11:
            case Header.INT_12:
            case Header.INT_13:
            case Header.INT_14:
            case Header.INT_15:
            case Header.INT_16:
                ret = (head-Header.INT_M9-9);
                break;
            case Header.INT_MIN_VALUE:
                ret = Integer.MIN_VALUE;
                break;
            case Header.INT_MAX_VALUE:
                ret = Integer.MAX_VALUE;
                break;

            case Header.INT_MF3:
            case Header.INT_F3:
                ir =  (is.readUnsignedByte()&0xFF);
            case Header.INT_MF2:
            case Header.INT_F2:
                ir = (ir<<8) | (is.readUnsignedByte()&0xFF);
            case Header.INT_MF1:
            case Header.INT_F1:
                ir = (ir<<8) | (is.readUnsignedByte()&0xFF);
                if(head%2==0)
                    ir=-ir;
                ret = ir;
                break;

            case Header.INT:
                ret = is.readInt();
                break;

            case Header.LONG_M9:
            case Header.LONG_M8:
            case Header.LONG_M7:
            case Header.LONG_M6:
            case Header.LONG_M5:
            case Header.LONG_M4:
            case Header.LONG_M3:
            case Header.LONG_M2:
            case Header.LONG_M1:
            case Header.LONG_0:
            case Header.LONG_1:
            case Header.LONG_2:
            case Header.LONG_3:
            case Header.LONG_4:
            case Header.LONG_5:
            case Header.LONG_6:
            case Header.LONG_7:
            case Header.LONG_8:
            case Header.LONG_9:
            case Header.LONG_10:
            case Header.LONG_11:
            case Header.LONG_12:
            case Header.LONG_13:
            case Header.LONG_14:
            case Header.LONG_15:
            case Header.LONG_16:
                ret = (long) (head - Header.LONG_M9 - 9);
                break;
            case Header.LONG_MIN_VALUE:
                ret = Long.MIN_VALUE;
                break;
            case Header.LONG_MAX_VALUE:
                ret = Long.MAX_VALUE;
                break;

            case Header.LONG_MF7:
            case Header.LONG_F7:
                lr = is.readUnsignedByte()&0xFFL;
            case Header.LONG_MF6:
            case Header.LONG_F6:
                lr = (lr<<8) | (is.readUnsignedByte()&0xFFL);
            case Header.LONG_MF5:
            case Header.LONG_F5:
                lr = (lr<<8) | (is.readUnsignedByte()&0xFFL);
            case Header.LONG_MF4:
            case Header.LONG_F4:
                lr = (lr<<8) | (is.readUnsignedByte()&0xFFL);
            case Header.LONG_MF3:
            case Header.LONG_F3:
                lr = (lr<<8) | (is.readUnsignedByte()&0xFFL);
            case Header.LONG_MF2:
            case Header.LONG_F2:
                lr = (lr<<8) | (is.readUnsignedByte()&0xFFL);
            case Header.LONG_MF1:
            case Header.LONG_F1:
                lr = (lr<<8) | (is.readUnsignedByte()&0xFFL);
                if(head%2==1) lr=-lr;
                ret = lr;
                break;

            case Header.LONG:
                ret = is.readLong();
                break;

            case Header.BYTE_M1:
                ret = (byte) -1;
                break;
            case Header.BYTE_0:
                ret = (byte) 0;
                break;
            case Header.BYTE_1:
                ret = (byte) 1;
                break;
            case Header.BYTE:
                ret = is.readByte();
                break;

            case Header.CHAR_0:
                ret = (char) 0;
                break;
            case Header.CHAR_1:
                ret = (char) 1;
                break;
            case Header.CHAR_255:
                ret = (char) is.readUnsignedByte();
                break;
            case Header.CHAR:
                ret = is.readChar();
                break;


            case Header.SHORT_M1:
                ret = (short) -1;
                break;
            case Header.SHORT_0:
                ret = (short) 0;
                break;
            case Header.SHORT_1:
                ret = (short) 1;
                break;
            case Header.SHORT_255:
                ret = (short) is.readUnsignedByte();
                break;
            case Header.SHORT_M255:
                ret = ((short) -is.readUnsignedByte());
                break;
            case Header.SHORT:
                ret = is.readShort();
                break;

            case Header.FLOAT_M1:
                ret = (float) -1;
                break;
            case Header.FLOAT_0:
                ret = (float) 0;
                break;
            case Header.FLOAT_1:
                ret = (float) 1;
                break;
            case Header.FLOAT_255:
                ret = (float) is.readUnsignedByte();
                break;
            case Header.FLOAT_SHORT:
                ret = (float) is.readShort();
                break;
            case Header.FLOAT:
                ret = is.readFloat();
                break;
            case Header.DOUBLE_M1:
                ret = -1D;
                break;
            case Header.DOUBLE_0:
                ret = 0D;
                break;
            case Header.DOUBLE_1:
                ret = 1D;
                break;
            case Header.DOUBLE_255:
                ret = (double) is.readUnsignedByte();
                break;
            case Header.DOUBLE_SHORT:
                ret = (double) is.readShort();
                break;
            case Header.DOUBLE_INT:
                ret = (double) is.readInt();
                break;
            case Header.DOUBLE:
                ret = is.readDouble();
                break;

            case Header.STRING:
                ret = deserializeString(is, DataInput2.unpackInt(is));
                break;
            case Header.STRING_0:
                ret = EMPTY_STRING;
                break;
            case Header.STRING_1:
            case Header.STRING_2:
            case Header.STRING_3:
            case Header.STRING_4:
            case Header.STRING_5:
            case Header.STRING_6:
            case Header.STRING_7:
            case Header.STRING_8:
            case Header.STRING_9:
            case Header.STRING_10:
                ret = deserializeString(is, head-Header.STRING_0);
                break;

            case -1:
                throw new EOFException();
        }

        if(ret==null){
            ret = deserialize2(head,is);
        }

        if (ret != null || head == Header.NULL) {
            if (objectStack != null)
                objectStack.add(ret);
            return ret;
        }

        /**  something else which needs object stack initialized*/

        if (objectStack == null)
            objectStack = new FastArrayList();
        int oldObjectStackSize = objectStack.size;

        ret = deserialize3(is, objectStack, head);

        if (head != Header.OBJECT_STACK && objectStack.size == oldObjectStackSize) {
            //check if object was not already added to stack as part of collection
            objectStack.add(ret);
        }


        return ret;
    }



    private Object deserialize3(DataInput is, FastArrayList<Object> objectStack, int head) throws IOException {
        Object ret;
        switch (head) {
            case Header.OBJECT_STACK:
                ret = objectStack.data[DataInput2.unpackInt(is)];
                break;
            case Header.ARRAYLIST:
                ret = deserializeArrayList(is, objectStack);
                break;
            case Header.ARRAY_OBJECT:
                ret = deserializeArrayObject(is, objectStack);
                break;
            case Header.LINKEDLIST:
                ret = deserializeLinkedList(is, objectStack);
                break;
            case Header.TREESET:
                ret = deserializeTreeSet(is, objectStack);
                break;
            case Header.HASHSET:
                ret = deserializeHashSet(is, objectStack);
                break;
            case Header.LINKEDHASHSET:
                ret = deserializeLinkedHashSet(is, objectStack);
                break;
            case Header.TREEMAP:
                ret = deserializeTreeMap(is, objectStack);
                break;
            case Header.HASHMAP:
                ret = deserializeHashMap(is, objectStack);
                break;
            case Header.LINKEDHASHMAP:
                ret = deserializeLinkedHashMap(is, objectStack);
                break;
            case Header.PROPERTIES:
                ret = deserializeProperties(is, objectStack);
                break;
            case Header.MA_LONG:
                ret = new Atomic.Long(getEngine(),DataInput2.unpackLong(is));
                break;
            case Header.MA_INT:
                ret = new Atomic.Integer(getEngine(),DataInput2.unpackLong(is));
                break;
            case Header.MA_BOOL:
                ret = new Atomic.Boolean(getEngine(),DataInput2.unpackLong(is));
                break;
            case Header.MA_STRING:
                ret = new Atomic.String(getEngine(),DataInput2.unpackLong(is));
                break;
            case Header.TUPLE2:
                ret = new Fun.Tuple2(this, is, objectStack);
                break;
            case Header.TUPLE3:
                ret = new Fun.Tuple3(this, is, objectStack,0);
                break;
            case Header.TUPLE4:
                ret = new Fun.Tuple4(this, is, objectStack);
                break;
            case Header.TUPLE5:
                ret = new Fun.Tuple5(this, is, objectStack);
                break;
            case Header.TUPLE6:
                ret = new Fun.Tuple6(this, is, objectStack);
                break;
            case Header.MA_VAR:
                ret = new Atomic.Var(getEngine(), this,is, objectStack);
                break;
            case Header.MAPDB:
                ret = deserializeMapDB(is,objectStack);
                break;

            default:
                ret = deserializeUnknownHeader(is, head, objectStack);
                break;
        }
        return ret;
    }

    private Object deserialize2(int head, DataInput is) throws IOException {
        Object ret;
        switch (head){
            case Header.ARRAY_BYTE_ALL_EQUAL:
                byte[] b = new byte[DataInput2.unpackInt(is)];
                Arrays.fill(b, is.readByte());
                ret = b;
                break;
            case Header.ARRAY_BYTE:
                ret = deserializeArrayByte(is);
                break;

            case Header.ARRAY_BOOLEAN:
                ret = readBooleanArray(DataInput2.unpackInt(is),is);
                break;
            case Header.ARRAY_SHORT:
                int size = DataInput2.unpackInt(is);
                ret = new short[size];
                for(int i=0;i<size;i++) ((short[])ret)[i] = is.readShort();
                break;
            case Header.ARRAY_DOUBLE:
                size = DataInput2.unpackInt(is);
                ret = new double[size];
                for(int i=0;i<size;i++) ((double[])ret)[i] = is.readDouble();
                break;
            case Header.ARRAY_FLOAT:
                size = DataInput2.unpackInt(is);
                ret = new float[size];
                for(int i=0;i<size;i++) ((float[])ret)[i] = is.readFloat();
                break;
            case Header.ARRAY_CHAR:
                size = DataInput2.unpackInt(is);
                ret = new char[size];
                for(int i=0;i<size;i++) ((char[])ret)[i] = is.readChar();
                break;

            case Header.ARRAY_INT_BYTE:
                size = DataInput2.unpackInt(is);
                ret=new int[size];
                for(int i=0;i<size;i++) ((int[])ret)[i] = is.readByte();
                break;
            case Header.ARRAY_INT_SHORT:
                size = DataInput2.unpackInt(is);
                ret=new int[size];
                for(int i=0;i<size;i++) ((int[])ret)[i] = is.readShort();
                break;
            case Header.ARRAY_INT_PACKED:
                size = DataInput2.unpackInt(is);
                ret=new int[size];
                for(int i=0;i<size;i++) ((int[])ret)[i] = DataInput2.unpackInt(is);
                break;
            case Header.ARRAY_INT:
                size = DataInput2.unpackInt(is);
                ret=new int[size];
                for(int i=0;i<size;i++) ((int[])ret)[i] = is.readInt();
                break;

            case Header.ARRAY_LONG_BYTE:
                size = DataInput2.unpackInt(is);
                ret=new long[size];
                for(int i=0;i<size;i++) ((long[])ret)[i] = is.readByte();
                break;
            case Header.ARRAY_LONG_SHORT:
                size = DataInput2.unpackInt(is);
                ret=new long[size];
                for(int i=0;i<size;i++) ((long[])ret)[i] = is.readShort();
                break;
            case Header.ARRAY_LONG_PACKED:
                size = DataInput2.unpackInt(is);
                ret=new long[size];
                for(int i=0;i<size;i++) ((long[])ret)[i] = DataInput2.unpackLong(is);
                break;
            case Header.ARRAY_LONG_INT:
                size = DataInput2.unpackInt(is);
                ret=new long[size];
                for(int i=0;i<size;i++) ((long[])ret)[i] = is.readInt();
                break;
            case Header.ARRAY_LONG:
                size = DataInput2.unpackInt(is);
                ret=new long[size];
                for(int i=0;i<size;i++) ((long[])ret)[i] = is.readLong();
                break;

            case Header.BIGINTEGER:
                ret = new BigInteger(deserializeArrayByte(is));
                break;
            case Header.BIGDECIMAL:
                ret = new BigDecimal(new BigInteger(deserializeArrayByte(is)), DataInput2.unpackInt(is));
                break;

            case Header.CLASS:
                ret = deserializeClass(is);
                break;
            case Header.DATE:
                ret = new Date(is.readLong());
                break;
            case Header.UUID:
                ret = new UUID(is.readLong(), is.readLong());
                break;

            case Header.ARRAYLIST_PACKED_LONG:
                ret = deserializeArrayListPackedLong(is);
                break;

            case Header.FUN_HI:
                ret = Fun.HI;
                break;
            case Header.JAVA_SERIALIZATION:
                throw new AssertionError("Wrong header, data were probably serialized with java.lang.ObjectOutputStream, not with MapDB serialization");
            case Header.ARRAY_OBJECT_PACKED_LONG:
                ret = deserializeArrayObjectPackedLong(is);
                break;
            case Header.ARRAY_OBJECT_ALL_NULL:
                ret = deserializeArrayObjectAllNull(is);
                break;
            case Header.ARRAY_OBJECT_NO_REFS:
                ret = deserializeArrayObjectNoRefs(is);
                break;

            default:
                ret = null;
                break;
        }

        return ret;
    }

    protected interface HeaderMapDB{
        int B_TREE_SERIALIZER_POS_LONG = 1;
        int B_TREE_SERIALIZER_STRING = 2;
        int B_TREE_SERIALIZER_POS_INT = 3;
        int SERIALIZER_LONG = 4;
        int SERIALIZER_INT = 5;
        int SERIALIZER_ILLEGAL_ACCESS = 6;

        int SERIALIZER_KEY_TUPLE2 = 7;
        int SERIALIZER_KEY_TUPLE3 = 8;
        int SERIALIZER_KEY_TUPLE4 = 9;
        int FUN_COMPARATOR = 10;
        int COMPARABLE_COMPARATOR = 11;
        int THIS_SERIALIZER = 12;
        int SERIALIZER_BASIC = 13;
        int SERIALIZER_STRING_NOSIZE = 14;
        int B_TREE_BASIC_KEY_SERIALIZER = 15;
        int SERIALIZER_BOOLEAN = 16;
        int SERIALIZER_BYTE_ARRAY_NOSIZE = 17;
        int SERIALIZER_JAVA = 18;
        int SERIALIZER_UUID = 19;
        int SERIALIZER_STRING = 20;
        int BYTE_ARRAY_SERIALIZER = 21;
        int TUPLE2_COMPARATOR = 22;
        int TUPLE3_COMPARATOR = 23;
        int TUPLE4_COMPARATOR = 24;
        int TUPLE2_COMPARATOR_STATIC = 25;
        int TUPLE3_COMPARATOR_STATIC = 26;
        int TUPLE4_COMPARATOR_STATIC = 27;
        int FUN_COMPARATOR_REVERSE = 28;
        int SERIALIZER_CHAR_ARRAY = 29;
        int SERIALIZER_INT_ARRAY = 30;
        int SERIALIZER_LONG_ARRAY = 31;
        int SERIALIZER_DOUBLE_ARRAY = 32;

        int HASHER_BASIC = 33;
        int HASHER_BYTE_ARRAY = 34;
        int HASHER_CHAR_ARRAY = 35;
        int HASHER_INT_ARRAY = 36;
        int HASHER_LONG_ARRAY = 37;
        int HASHER_DOUBLE_ARRAY = 38;

        int COMPARATOR_BYTE_ARRAY = 39;
        int COMPARATOR_CHAR_ARRAY = 40;
        int COMPARATOR_INT_ARRAY = 41;
        int COMPARATOR_LONG_ARRAY = 42;
        int COMPARATOR_DOUBLE_ARRAY = 43;
        int COMPARATOR_COMPARABLE_ARRAY = 44;
        int COMPARATOR_ARRAY = 45;

        int SERIALIZER_STRING_ASCII = 46;

        int SERIALIZER_COMPRESSION_WRAPPER = 47;
        int B_TREE_COMPRESSION_SERIALIZER = 48; //TODO is this going to be used?

        int SERIALIZER_STRING_INTERN = 49;
        int FUN_EMPTY_ITERATOR = 50;

        int TUPLE5_COMPARATOR = 51;
        int TUPLE6_COMPARATOR = 52;
        int TUPLE5_COMPARATOR_STATIC = 53;
        int TUPLE6_COMPARATOR_STATIC = 54;
        int SERIALIZER_KEY_TUPLE5 = 55;
        int SERIALIZER_KEY_TUPLE6 = 56;
        int HASHER_ARRAY = 57;

    }

    protected static final class singletons{
        static final Map<Object,Integer> all = new IdentityHashMap<Object, Integer>();
        static final LongHashMap<Object> reverse = new LongHashMap<Object>();

        static {
            all.put(BTreeKeySerializer.STRING, HeaderMapDB.B_TREE_SERIALIZER_STRING);
            all.put(BTreeKeySerializer.ZERO_OR_POSITIVE_LONG, HeaderMapDB.B_TREE_SERIALIZER_POS_LONG);
            all.put(BTreeKeySerializer.ZERO_OR_POSITIVE_INT, HeaderMapDB.B_TREE_SERIALIZER_POS_INT);

            all.put(BTreeMap.COMPARABLE_COMPARATOR,HeaderMapDB.COMPARABLE_COMPARATOR);
            all.put(Fun.COMPARATOR,HeaderMapDB.FUN_COMPARATOR);
            all.put(Fun.REVERSE_COMPARATOR,HeaderMapDB.FUN_COMPARATOR_REVERSE);
            all.put(Fun.EMPTY_ITERATOR,HeaderMapDB.FUN_EMPTY_ITERATOR);
            all.put(Fun.TUPLE2_COMPARATOR,HeaderMapDB.TUPLE2_COMPARATOR_STATIC);
            all.put(Fun.TUPLE3_COMPARATOR,HeaderMapDB.TUPLE3_COMPARATOR_STATIC);
            all.put(Fun.TUPLE4_COMPARATOR,HeaderMapDB.TUPLE4_COMPARATOR_STATIC);
            all.put(Fun.TUPLE5_COMPARATOR,HeaderMapDB.TUPLE5_COMPARATOR_STATIC);
            all.put(Fun.TUPLE6_COMPARATOR,HeaderMapDB.TUPLE6_COMPARATOR_STATIC);

            all.put(Serializer.STRING_NOSIZE,HeaderMapDB.SERIALIZER_STRING_NOSIZE);
            all.put(Serializer.STRING_ASCII,HeaderMapDB.SERIALIZER_STRING_ASCII);
            all.put(Serializer.STRING_INTERN,HeaderMapDB.SERIALIZER_STRING_INTERN);
            all.put(Serializer.LONG,HeaderMapDB.SERIALIZER_LONG);
            all.put(Serializer.INTEGER,HeaderMapDB.SERIALIZER_INT);
            all.put(Serializer.ILLEGAL_ACCESS,HeaderMapDB.SERIALIZER_ILLEGAL_ACCESS);
            all.put(Serializer.BASIC,HeaderMapDB.SERIALIZER_BASIC);
            all.put(Serializer.BOOLEAN,HeaderMapDB.SERIALIZER_BOOLEAN);
            all.put(Serializer.BYTE_ARRAY_NOSIZE,HeaderMapDB.SERIALIZER_BYTE_ARRAY_NOSIZE);
            all.put(Serializer.BYTE_ARRAY,HeaderMapDB.BYTE_ARRAY_SERIALIZER);
            all.put(Serializer.JAVA,HeaderMapDB.SERIALIZER_JAVA);
            all.put(Serializer.UUID,HeaderMapDB.SERIALIZER_UUID);
            all.put(Serializer.STRING,HeaderMapDB.SERIALIZER_STRING);
            all.put(Serializer.CHAR_ARRAY,HeaderMapDB.SERIALIZER_CHAR_ARRAY);
            all.put(Serializer.INT_ARRAY,HeaderMapDB.SERIALIZER_INT_ARRAY);
            all.put(Serializer.LONG_ARRAY,HeaderMapDB.SERIALIZER_LONG_ARRAY);
            all.put(Serializer.DOUBLE_ARRAY,HeaderMapDB.SERIALIZER_DOUBLE_ARRAY);

            all.put(Hasher.BASIC,HeaderMapDB.HASHER_BASIC);
            all.put(Hasher.BYTE_ARRAY,HeaderMapDB.HASHER_BYTE_ARRAY);
            all.put(Hasher.CHAR_ARRAY,HeaderMapDB.HASHER_CHAR_ARRAY);
            all.put(Hasher.INT_ARRAY,HeaderMapDB.HASHER_INT_ARRAY);
            all.put(Hasher.LONG_ARRAY,HeaderMapDB.HASHER_LONG_ARRAY);
            all.put(Hasher.DOUBLE_ARRAY,HeaderMapDB.HASHER_DOUBLE_ARRAY);
            all.put(Hasher.ARRAY,HeaderMapDB.HASHER_ARRAY);

            all.put(Fun.BYTE_ARRAY_COMPARATOR,HeaderMapDB.COMPARATOR_BYTE_ARRAY);
            all.put(Fun.CHAR_ARRAY_COMPARATOR,HeaderMapDB.COMPARATOR_CHAR_ARRAY);
            all.put(Fun.INT_ARRAY_COMPARATOR,HeaderMapDB.COMPARATOR_INT_ARRAY);
            all.put(Fun.LONG_ARRAY_COMPARATOR,HeaderMapDB.COMPARATOR_LONG_ARRAY);
            all.put(Fun.DOUBLE_ARRAY_COMPARATOR,HeaderMapDB.COMPARATOR_DOUBLE_ARRAY);
            all.put(Fun.COMPARABLE_ARRAY_COMPARATOR,HeaderMapDB.COMPARATOR_COMPARABLE_ARRAY);


            //important for assertSerializable
            all.put(Fun.HI,Integer.MIN_VALUE);

            for(Map.Entry<Object,Integer> e:all.entrySet()){
                reverse.put(e.getValue(),e.getKey());
            }
        }
    }

    public static void assertSerializable(Object o){
        if(o!=null && !(o instanceof Serializable)
                && !singletons.all.containsKey(o)){
            throw new IllegalArgumentException("Not serializable: "+o.getClass());
        }
    }


    protected Object deserializeMapDB(DataInput is, FastArrayList<Object> objectStack) throws IOException {
        int head = DataInput2.unpackInt(is);

        Object singleton = singletons.reverse.get(head);
        if(singleton!=null)
            return singleton;

        assert(objectStack!=null);

        switch(head){
            case HeaderMapDB.SERIALIZER_KEY_TUPLE2:
                return new BTreeKeySerializer.Tuple2KeySerializer(this, is, objectStack,0);
            case HeaderMapDB.COMPARATOR_ARRAY:
                return new Fun.ArrayComparator(this, is, objectStack);

            case HeaderMapDB.SERIALIZER_COMPRESSION_WRAPPER:
                return new CompressionWrapper(this, is, objectStack);


            case HeaderMapDB.SERIALIZER_KEY_TUPLE3:
                return new BTreeKeySerializer.Tuple3KeySerializer(this, is, objectStack);

            case HeaderMapDB.SERIALIZER_KEY_TUPLE4:
                return new BTreeKeySerializer.Tuple4KeySerializer(this, is, objectStack);
            case HeaderMapDB.SERIALIZER_KEY_TUPLE5:
                return new BTreeKeySerializer.Tuple5KeySerializer(this, is, objectStack);
            case HeaderMapDB.SERIALIZER_KEY_TUPLE6:
                return new BTreeKeySerializer.Tuple6KeySerializer(this, is, objectStack);

            case HeaderMapDB.TUPLE2_COMPARATOR:
                return new Fun.Tuple2Comparator(this, is, objectStack);
            case HeaderMapDB.TUPLE3_COMPARATOR:
                return new Fun.Tuple3Comparator(this, is, objectStack,0);
            case HeaderMapDB.TUPLE4_COMPARATOR:
                return new Fun.Tuple4Comparator(this, is, objectStack);
            case HeaderMapDB.TUPLE5_COMPARATOR:
                return new Fun.Tuple5Comparator(this, is, objectStack);
            case HeaderMapDB.TUPLE6_COMPARATOR:
                return new Fun.Tuple6Comparator(this, is, objectStack);

            case HeaderMapDB.B_TREE_BASIC_KEY_SERIALIZER:
                return new BTreeKeySerializer.BasicKeySerializer(this,is,objectStack);
            case HeaderMapDB.THIS_SERIALIZER:
                return this;

            default:
                throw new IOError(new IOException("Unknown header byte, data corrupted"));
        }
    }

    protected Engine getEngine(){
        throw new UnsupportedOperationException();
    }


    protected  Class deserializeClass(DataInput is) throws IOException {
        //TODO override 'deserializeClass' in SerializerPojo
        return SerializerPojo.classForName(is.readUTF());
    }


    private byte[] deserializeArrayByte(DataInput is) throws IOException {
        byte[] bb = new byte[DataInput2.unpackInt(is)];
        is.readFully(bb);
        return bb;
    }




    private Object[] deserializeArrayObject(DataInput is, FastArrayList<Object> objectStack) throws IOException {
        int size = DataInput2.unpackInt(is);
        Class clazz = deserializeClass(is);
        Object[] s = (Object[]) Array.newInstance(clazz, size);
        objectStack.add(s);
        for (int i = 0; i < size; i++){
            s[i] = deserialize(is, objectStack);
        }
        return s;
    }

    private Object[] deserializeArrayObjectNoRefs(DataInput is) throws IOException {
        int size = DataInput2.unpackInt(is);
        Class clazz = deserializeClass(is);
        Object[] s = (Object[]) Array.newInstance(clazz, size);
        for (int i = 0; i < size; i++){
            s[i] = deserialize(is, null);
        }
        return s;
    }


    private Object[] deserializeArrayObjectAllNull(DataInput is) throws IOException {
        int size = DataInput2.unpackInt(is);
        Class clazz = deserializeClass(is);
        return (Object[]) Array.newInstance(clazz, size);
    }


    private Object[] deserializeArrayObjectPackedLong(DataInput is) throws IOException {
        int size = is.readUnsignedByte();
        Object[] s = new Object[size];
        for (int i = 0; i < size; i++) {
            long l = DataInput2.unpackLong(is);
            if (l == 0)
                s[i] = null;
            else
                s[i] = l - 1;
        }
        return s;
    }


    private ArrayList<Object> deserializeArrayList(DataInput is, FastArrayList<Object> objectStack) throws IOException {
        int size = DataInput2.unpackInt(is);
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
            long l = DataInput2.unpackLong(is);
            if (l == 0)
                s.add(null);
            else
                s.add(l - 1);
        }
        return s;
    }


    private java.util.LinkedList deserializeLinkedList(DataInput is, FastArrayList<Object> objectStack) throws IOException {
        int size = DataInput2.unpackInt(is);
        java.util.LinkedList s = new java.util.LinkedList();
        objectStack.add(s);
        for (int i = 0; i < size; i++)
            s.add(deserialize(is, objectStack));
        return s;
    }




    private HashSet<Object> deserializeHashSet(DataInput is, FastArrayList<Object> objectStack) throws IOException {
        int size = DataInput2.unpackInt(is);
        HashSet<Object> s = new HashSet<Object>(size);
        objectStack.add(s);
        for (int i = 0; i < size; i++)
            s.add(deserialize(is, objectStack));
        return s;
    }


    private LinkedHashSet<Object> deserializeLinkedHashSet(DataInput is, FastArrayList<Object> objectStack) throws IOException {
        int size = DataInput2.unpackInt(is);
        LinkedHashSet<Object> s = new LinkedHashSet<Object>(size);
        objectStack.add(s);
        for (int i = 0; i < size; i++)
            s.add(deserialize(is, objectStack));
        return s;
    }


    private TreeSet<Object> deserializeTreeSet(DataInput is, FastArrayList<Object> objectStack) throws IOException {
        int size = DataInput2.unpackInt(is);
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
        int size = DataInput2.unpackInt(is);

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
        int size = DataInput2.unpackInt(is);

        HashMap<Object, Object> s = new HashMap<Object, Object>(size);
        objectStack.add(s);
        for (int i = 0; i < size; i++)
            s.put(deserialize(is, objectStack), deserialize(is, objectStack));
        return s;
    }


    private LinkedHashMap<Object, Object> deserializeLinkedHashMap(DataInput is, FastArrayList<Object> objectStack) throws IOException {
        int size = DataInput2.unpackInt(is);

        LinkedHashMap<Object, Object> s = new LinkedHashMap<Object, Object>(size);
        objectStack.add(s);
        for (int i = 0; i < size; i++)
            s.put(deserialize(is, objectStack), deserialize(is, objectStack));
        return s;
    }



    private Properties deserializeProperties(DataInput is, FastArrayList<Object> objectStack) throws IOException {
        int size = DataInput2.unpackInt(is);

        Properties s = new Properties();
        objectStack.add(s);
        for (int i = 0; i < size; i++)
            s.put(deserialize(is, objectStack), deserialize(is, objectStack));
        return s;
    }

    /** override this method to extend SerializerBase functionality*/
    protected void serializeUnknownObject(DataOutput out, Object obj, FastArrayList<Object> objectStack) throws IOException {
        throw new AssertionError("Could not serialize unknown object: "+obj.getClass().getName());
    }
    /** override this method to extend SerializerBase functionality*/
    protected Object deserializeUnknownHeader(DataInput is, int head, FastArrayList<Object> objectStack) throws IOException {
        throw new AssertionError("Unknown serialization header: " + head);
    }

    /**
     * Builds a byte array from the array of booleans, compressing up to 8 booleans per byte.
     *
     * @param bool The booleans to be compressed.
     * @return The fully compressed byte array.
     */
    protected static byte[] booleanToByteArray(boolean[] bool) {
        int boolLen = bool.length;
        int mod8 = boolLen%8;
        byte[] boolBytes = new byte[(boolLen/8)+((boolLen%8 == 0)?0:1)];

        boolean isFlushWith8 = mod8 == 0;
        int length = (isFlushWith8)?boolBytes.length:boolBytes.length-1;
        int x = 0;
        int boolByteIndex;
        for (boolByteIndex=0; boolByteIndex<length;) {
            byte b = (byte)	(((bool[x++]? 0x01 : 0x00) << 0) |
                    ((bool[x++]? 0x01 : 0x00) << 1) |
                    ((bool[x++]? 0x01 : 0x00) << 2) |
                    ((bool[x++]? 0x01 : 0x00) << 3) |
                    ((bool[x++]? 0x01 : 0x00) << 4) |
                    ((bool[x++]? 0x01 : 0x00) << 5) |
                    ((bool[x++]? 0x01 : 0x00) << 6) |
                    ((bool[x++]? 0x01 : 0x00) << 7));
            boolBytes[boolByteIndex++] = b;
        }
        if (!isFlushWith8) {//If length is not a multiple of 8 we must do the last byte conditionally on every element.
            byte b = (byte)	0x00;

            switch(mod8) {
                case 1:
                    b |=	((bool[x++]? 0x01 : 0x00) << 0);
                    break;
                case 2:
                    b |=	((bool[x++]? 0x01 : 0x00) << 0) |
                            ((bool[x++]? 0x01 : 0x00) << 1);
                    break;
                case 3:
                    b |=	((bool[x++]? 0x01 : 0x00) << 0) |
                            ((bool[x++]? 0x01 : 0x00) << 1) |
                            ((bool[x++]? 0x01 : 0x00) << 2);
                    break;
                case 4:
                    b |=	((bool[x++]? 0x01 : 0x00) << 0) |
                            ((bool[x++]? 0x01 : 0x00) << 1) |
                            ((bool[x++]? 0x01 : 0x00) << 2) |
                            ((bool[x++]? 0x01 : 0x00) << 3);
                    break;
                case 5:
                    b |=	((bool[x++]? 0x01 : 0x00) << 0) |
                            ((bool[x++]? 0x01 : 0x00) << 1) |
                            ((bool[x++]? 0x01 : 0x00) << 2) |
                            ((bool[x++]? 0x01 : 0x00) << 3) |
                            ((bool[x++]? 0x01 : 0x00) << 4);
                    break;
                case 6:
                    b |=	((bool[x++]? 0x01 : 0x00) << 0) |
                            ((bool[x++]? 0x01 : 0x00) << 1) |
                            ((bool[x++]? 0x01 : 0x00) << 2) |
                            ((bool[x++]? 0x01 : 0x00) << 3) |
                            ((bool[x++]? 0x01 : 0x00) << 4) |
                            ((bool[x++]? 0x01 : 0x00) << 5);
                    break;
                case 7:
                    b |=	((bool[x++]? 0x01 : 0x00) << 0) |
                            ((bool[x++]? 0x01 : 0x00) << 1) |
                            ((bool[x++]? 0x01 : 0x00) << 2) |
                            ((bool[x++]? 0x01 : 0x00) << 3) |
                            ((bool[x++]? 0x01 : 0x00) << 4) |
                            ((bool[x++]? 0x01 : 0x00) << 5) |
                            ((bool[x++]? 0x01 : 0x00) << 6);
                    break;
                case 8:
                    b |=	((bool[x++]? 0x01 : 0x00) << 0) |
                            ((bool[x++]? 0x01 : 0x00) << 1) |
                            ((bool[x++]? 0x01 : 0x00) << 2) |
                            ((bool[x++]? 0x01 : 0x00) << 3) |
                            ((bool[x++]? 0x01 : 0x00) << 4) |
                            ((bool[x++]? 0x01 : 0x00) << 5) |
                            ((bool[x++]? 0x01 : 0x00) << 6) |
                            ((bool[x++]? 0x01 : 0x00) << 7);
                    break;
            }

            ////////////////////////
            // OLD
            ////////////////////////
			/* The code below was replaced with the switch statement
			 * above. It increases performance by only doing 1
			 * check against mod8 (switch statement) and only doing 1
			 * assignment operation for every possible value of mod8,
			 * rather than doing up to 8 assignment operations and an
			 * if check in between each one. The code is longer but
			 * faster.
			 *
			byte b = (byte)	0x00;
			b |= ((bool[x++]? 0x01 : 0x00) << 0);
			if (mod8 > 1) {
				b |= ((bool[x++]? 0x01 : 0x00) << 1);
				if (mod8 > 2) {
					b |= ((bool[x++]? 0x01 : 0x00) << 2);
					if (mod8 > 3) {
						b |= ((bool[x++]? 0x01 : 0x00) << 3);
						if (mod8 > 4) {
							b |= ((bool[x++]? 0x01 : 0x00) << 4);
							if (mod8 > 5) {
								b |= ((bool[x++]? 0x01 : 0x00) << 5);
								if (mod8 > 6) {
									b |= ((bool[x++]? 0x01 : 0x00) << 6);
									if (mod8 > 7) {
										b |= ((bool[x++]? 0x01 : 0x00) << 7);
									}
								}
							}
						}
					}
				}
			}
			*/
            boolBytes[boolByteIndex++] = b;
        }

        return boolBytes;
    }



    /**
     * Unpacks an integer from the DataInput indicating the number of booleans that are compressed. It then calculates
     * the number of bytes, reads them in, and decompresses and converts them into an array of booleans using the
     * toBooleanArray(byte[]); method. The array of booleans are trimmed to <code>numBools</code> elements. This is
     * necessary in situations where the number of booleans is not a multiple of 8.
     *
     * @return The boolean array decompressed from the bytes read in.
     * @throws IOException If an error occurred while reading.
     */
    protected static boolean[] readBooleanArray(int numBools,DataInput is) throws IOException {
        int length = (numBools/8)+((numBools%8 == 0)?0:1);
        byte[] boolBytes = new byte[length];
        is.readFully(boolBytes);


        boolean[] tmp = new boolean[boolBytes.length*8];
        int len = boolBytes.length;
        int boolIndex = 0;
        for (byte boolByte : boolBytes) {
            for (int y = 0; y < 8; y++) {
                tmp[boolIndex++] = (boolByte & (0x01 << y)) != 0x00;
            }
        }

        //Trim excess booleans
        boolean[] finalBoolArray = new boolean[numBools];
        System.arraycopy(tmp, 0, finalBoolArray, 0, numBools);

        //Return the trimmed, uncompressed boolean array
        return finalBoolArray;
    }





    /**
     * Header byte, is used at start of each record to indicate data type
     * WARNING !!! values bellow must be unique !!!!!
     *
     * @author Jan Kotek
     */
    protected interface Header {

        int ZERO_FAIL=0; //zero is invalid value, so it fails with uninitialized values
        int NULL = 1;
        int BOOLEAN_TRUE = 2;
        int BOOLEAN_FALSE = 3;

        int INT_M9 = 4;
        int INT_M8 = 5;
        int INT_M7 = 6;
        int INT_M6 = 7;
        int INT_M5 = 8;
        int INT_M4 = 9;
        int INT_M3 = 10;
        int INT_M2 = 11;
        int INT_M1 = 12;
        int INT_0 = 13;
        int INT_1 = 14;
        int INT_2 = 15;
        int INT_3 = 16;
        int INT_4 = 17;
        int INT_5 = 18;
        int INT_6 = 19;
        int INT_7 = 20;
        int INT_8 = 21;
        int INT_9 = 22;
        int INT_10 = 23;
        int INT_11 = 24;
        int INT_12 = 25;
        int INT_13 = 26;
        int INT_14 = 27;
        int INT_15 = 28;
        int INT_16 = 29;
        int INT_MIN_VALUE = 30;
        int INT_MAX_VALUE = 31;
        int INT_MF1 = 32;
        int INT_F1 = 33;
        int INT_MF2 = 34;
        int INT_F2 = 35;
        int INT_MF3 = 36;
        int INT_F3 = 37;
        int INT = 38;

        int LONG_M9 = 39;
        int LONG_M8 = 40;
        int LONG_M7 = 41;
        int LONG_M6 = 42;
        int LONG_M5 = 43;
        int LONG_M4 = 44;
        int LONG_M3 = 45;
        int LONG_M2 = 46;
        int LONG_M1 = 47;
        int LONG_0 = 48;
        int LONG_1 = 49;
        int LONG_2 = 50;
        int LONG_3 = 51;
        int LONG_4 = 52;
        int LONG_5 = 53;
        int LONG_6 = 54;
        int LONG_7 = 55;
        int LONG_8 = 56;
        int LONG_9 = 57;
        int LONG_10 = 58;
        int LONG_11 = 59;
        int LONG_12 = 60;
        int LONG_13 = 61;
        int LONG_14 = 62;
        int LONG_15 = 63;
        int LONG_16 = 64;
        int LONG_MIN_VALUE = 65;
        int LONG_MAX_VALUE = 66;

        int LONG_MF1 = 67;
        int LONG_F1 = 68;
        int LONG_MF2 = 69;
        int LONG_F2 = 70;
        int LONG_MF3 = 71;
        int LONG_F3 = 72;
        int LONG_MF4 = 73;
        int LONG_F4 = 74;
        int LONG_MF5 = 75;
        int LONG_F5 = 76;
        int LONG_MF6 = 77;
        int LONG_F6 = 78;
        int LONG_MF7 = 79;
        int LONG_F7 = 80;
        int LONG = 81;

        int BYTE_M1 = 82;
        int BYTE_0 = 83;
        int BYTE_1 = 84;
        int BYTE = 85;

        int CHAR_0 = 86;
        int CHAR_1 = 87;
        int CHAR_255 = 88;
        int CHAR = 89;

        int SHORT_M1 =90;
        int SHORT_0 = 91;
        int SHORT_1 = 92;
        int SHORT_255 = 93;
        int SHORT_M255 = 94;
        int SHORT = 95;

        int FLOAT_M1 = 96;
        int FLOAT_0 = 97;
        int FLOAT_1 = 98;
        int FLOAT_255 = 99;
        int FLOAT_SHORT = 100;
        int FLOAT = 101;

        int DOUBLE_M1 = 102;
        int DOUBLE_0 = 103;
        int DOUBLE_1 = 104;
        int DOUBLE_255 = 105;
        int DOUBLE_SHORT = 106;
        int DOUBLE_INT = 107;
        int DOUBLE = 108;

        int ARRAY_BYTE = 109;
        int ARRAY_BYTE_ALL_EQUAL = 110;

        int ARRAY_BOOLEAN = 111;
        int ARRAY_SHORT = 112;
        int ARRAY_CHAR = 113;
        int ARRAY_FLOAT = 114;
        int ARRAY_DOUBLE = 115;

        int ARRAY_INT_BYTE = 116;
        int ARRAY_INT_SHORT = 117;
        int ARRAY_INT_PACKED = 118;
        int ARRAY_INT = 119;

        int ARRAY_LONG_BYTE = 120;
        int ARRAY_LONG_SHORT = 121;
        int ARRAY_LONG_PACKED = 122;
        int ARRAY_LONG_INT = 123;
        int ARRAY_LONG = 124;

        int STRING_0 = 125;
        int STRING_1 = 126;
        int STRING_2 = 127;
        int STRING_3 = 128;
        int STRING_4 = 129;
        int STRING_5 = 130;
        int STRING_6 = 131;
        int STRING_7 = 132;
        int STRING_8 = 133;
        int STRING_9 = 134;
        int STRING_10 = 135;
        int STRING = 136;

        int BIGDECIMAL = 137;
        int BIGINTEGER = 138;


        int CLASS = 139;
        int DATE = 140;
        int FUN_HI = 141;
        int UUID = 142;

        //144 to 149 reserved for other non recursive objects

        int MAPDB = 150;
        int TUPLE2 = 151;
        int TUPLE3 = 152;
        int TUPLE4 = 153;
        int TUPLE5 = 154; //reserved for Tuple5 if we will ever implement it
        int TUPLE6 = 155; //reserved for Tuple6 if we will ever implement it
        int TUPLE7 = 156; //reserved for Tuple7 if we will ever implement it
        int TUPLE8 = 157; //reserved for Tuple8 if we will ever implement it


        int  ARRAY_OBJECT = 158;
        //special cases for BTree values which stores references
        int ARRAY_OBJECT_PACKED_LONG = 159;
        int ARRAYLIST_PACKED_LONG = 160;
        int ARRAY_OBJECT_ALL_NULL = 161;
        int ARRAY_OBJECT_NO_REFS = 162;

        int  ARRAYLIST = 163;
        int  TREEMAP = 164;
        int  HASHMAP = 165;
        int  LINKEDHASHMAP = 166;
        int  TREESET = 167;
        int  HASHSET = 168;
        int  LINKEDHASHSET = 169;
        int  LINKEDLIST = 170;
        int  PROPERTIES = 171;

        /**
         * Value used in Java Serialization header. For this header we throw an exception because data might be corrupted
         */
        int JAVA_SERIALIZATION = 172;

        /**
         * Use POJO Serializer to get class structure and set its fields
         */
        int POJO = 173;
        /**
         * used for reference to already serialized object in object graph
         */
        int OBJECT_STACK = 174;

        /**
         * reference to named object
         */
        int NAMED = 175;

        int MA_LONG = 176;
        int MA_INT = 177;
        int MA_BOOL = 178;
        int MA_STRING = 179;
        int MA_VAR = 180;

    }

    @Override
    public int fixedSize() {
        return -1;
    }


}
