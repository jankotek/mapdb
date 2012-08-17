package net.kotek.jdbm;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

import static net.kotek.jdbm.SerializationHeader.*;

/**
 * Serializer which uses 'header byte' to serialize/deserialize
 * most of classes from 'java.lang' and 'java.util' packages.
 *
 * @author Jan Kotek
 */
public class SerializerBase implements Serializer{

    /**
     * print statistics to STDOUT
     */
    static final boolean DEBUG = false;

    /**
     * Utility class similar to ArrayList, but with fast identity search.
     */
    static class FastArrayList<K> {

        private int size = 0;
        private K[] elementData = (K[]) new Object[8];

        K get(int index) {
            if (index >= size) throw new IndexOutOfBoundsException();
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
         * @param obj
         * @return index of object in list or -1 if not found
         */
        int identityIndexOf(Object obj) {
            for (int i = 0; i < size; i++) {
                if (obj == elementData[i])
                    return i;
            }
            return -1;
        }

    }




    @Override
    public void serialize(final DataOutput out, final Object obj) throws IOException {
        serialize(out, obj, null);
    }


    public void serialize(final DataOutput out, final Object obj, FastArrayList objectStack) throws IOException {

        /**try to find object on stack if it exists*/
        if (objectStack != null) {
            int indexInObjectStack = objectStack.identityIndexOf(obj);
            if (indexInObjectStack != -1) {
                //object was already serialized, just write reference to it and return
                out.write(OBJECT_STACK);
                JdbmUtil.packInt(out, indexInObjectStack);
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
            if (((Boolean) obj).booleanValue())
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
            serializeByteArrayInt(out, buf);
            return;
        } else if (clazz == BigDecimal.class) {
            out.write(BIGDECIMAL);
            BigDecimal d = (BigDecimal) obj;
            serializeByteArrayInt(out, d.unscaledValue().toByteArray());
            JdbmUtil.packInt(out, d.scale());
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
                out.write(SHORT_FULL);
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
            out.writeUTF(((Class) obj).getName());
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
            JdbmUtil.packInt(out,a.length);
            for(short s:a) out.writeShort(s);
            return;
        } else if (obj instanceof boolean[]) {
            out.write(BOOLEAN_ARRAY);
            boolean[] a = (boolean[]) obj;
            JdbmUtil.packInt(out,a.length);
            for(boolean s:a) out.writeBoolean(s); //TODO pack 8 booleans to single byte
            return;
        } else if (obj instanceof double[]) {
            out.write(DOUBLE_ARRAY);
            double[] a = (double[]) obj;
            JdbmUtil.packInt(out,a.length);
            for(double s:a) out.writeDouble(s);
            return;
        } else if (obj instanceof float[]) {
            out.write(FLOAT_ARRAY);
            float[] a = (float[]) obj;
            JdbmUtil.packInt(out,a.length);
            for(float s:a) out.writeFloat(s);
            return;
        } else if (obj instanceof char[]) {
            out.write(CHAR_ARRAY);
            char[] a = (char[]) obj;
            JdbmUtil.packInt(out,a.length);
            for(char s:a) out.writeChar(s);
            return;
        } else if (obj instanceof byte[]) {
            byte[] b = (byte[]) obj;
            out.write(ARRAY_BYTE_INT);
            serializeByteArrayInt(out, b);
            return;
        } else if (clazz == Date.class) {
            out.write(DATE);
            out.writeLong(((Date) obj).getTime());
            return;
        }



//        /** classes bellow need object stack, so initialize it if not alredy initialized*/
//        if (objectStack == null) {
//            objectStack = new FastArrayList();
//            objectStack.add(obj);
//        }


        if (obj instanceof Object[]) {
            Object[] b = (Object[]) obj;
            boolean packableLongs = b.length <= 255;
            if (packableLongs) {
                //check if it contains packable longs
                for (Object o : b) {
                    if (o != null && (o.getClass() != Long.class || (((Long) o).longValue() < 0 && ((Long) o).longValue() != Long.MAX_VALUE))) {
                        packableLongs = false;
                        break;
                    }
                }
            }

            if (packableLongs) {
                //packable Longs is special case,  it is often used in JDBM to reference fields
                out.write(ARRAY_OBJECT_PACKED_LONG);
                out.write(b.length);
                for (Object o : b) {
                    if (o == null)
                        JdbmUtil.packLong(out, 0);
                    else
                        JdbmUtil.packLong(out, ((Long) o).longValue() + 1);
                }

            } else {
                out.write(ARRAY_OBJECT);
                JdbmUtil.packInt(out, b.length);

//                // Write class id for components
//                Class<?> componentType = obj.getClass().getComponentType();
//                registerClass(componentType);
//                //write class header
//                int classId = getClassId(componentType);
//                JdbmUtil.packInt(out, classId);

                for (Object o : b)
                    serialize(out, o, objectStack);

            }

        } else if (clazz == ArrayList.class) {
            ArrayList l = (ArrayList) obj;
            boolean packableLongs = l.size() < 255;
            if (packableLongs) {
                //packable Longs is special case,  it is often used in JDBM to reference fields
                for (Object o : l) {
                    if (o != null && (o.getClass() != Long.class || (((Long) o).longValue() < 0 && ((Long) o).longValue() != Long.MAX_VALUE))) {
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
                        JdbmUtil.packLong(out, 0);
                    else
                        JdbmUtil.packLong(out, ((Long) o).longValue() + 1);
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
            JdbmUtil.packInt(out, l.size());
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
            JdbmUtil.packInt(out, l.size());
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
        } else {
            out.write(POJO);
            throw new InternalError("POJO serialization not supported yet");
            //writeObject(out, obj, objectStack);
        }

    }


    static void serializeString(DataOutput out, String obj) throws IOException {
        final int len = obj.length();
        JdbmUtil.packInt(out, len);
        for (int i = 0; i < len; i++) {
            int c = (int) obj.charAt(i); //TODO investigate if c could be negative here
            JdbmUtil.packInt(out, c);
        }

    }

    private void serializeMap(int header, DataOutput out, Object obj, FastArrayList objectStack) throws IOException {
        Map l = (Map) obj;
        out.write(header);
        JdbmUtil.packInt(out, l.size());
        for (Object o : l.keySet()) {
            serialize(out, o, objectStack);
            serialize(out, l.get(o), objectStack);
        }
    }

    private void serializeCollection(int header, DataOutput out, Object obj, FastArrayList objectStack) throws IOException {
        Collection l = (Collection) obj;
        out.write(header);
        JdbmUtil.packInt(out, l.size());

        for (Object o : l)
            serialize(out, o, objectStack);

    }

    private void serializeByteArrayInt(DataOutput out, byte[] b) throws IOException {
        JdbmUtil.packInt(out, b.length);
        out.write(b);
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
            JdbmUtil.packInt(da, obj.length);
            for (long l : obj)
                da.write((int) l);
        } else if (0 <= min && max <= Long.MAX_VALUE) {
            da.write(ARRAY_LONG_PACKED);
            JdbmUtil.packInt(da, obj.length);
            for (long l : obj)
                JdbmUtil.packLong(da, l);
        } else if (Short.MIN_VALUE <= min && max <= Short.MAX_VALUE) {
            da.write(ARRAY_LONG_S);
            JdbmUtil.packInt(da, obj.length);
            for (long l : obj)
                da.writeShort((short) l);
        } else if (Integer.MIN_VALUE <= min && max <= Integer.MAX_VALUE) {
            da.write(ARRAY_LONG_I);
            JdbmUtil.packInt(da, obj.length);
            for (long l : obj)
                da.writeInt((int) l);
        } else {
            da.write(ARRAY_LONG_L);
            JdbmUtil.packInt(da, obj.length);
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
            JdbmUtil.packInt(da, obj.length);
            for (int i : obj)
                da.write(i);
        } else if (0 <= min && max <= Integer.MAX_VALUE) {
            da.write(ARRAY_INT_PACKED);
            JdbmUtil.packInt(da, obj.length);
            for (int l : obj)
                JdbmUtil.packInt(da, l);
        } else if (fitsInShort) {
            da.write(ARRAY_INT_S);
            JdbmUtil.packInt(da, obj.length);
            for (int i : obj)
                da.writeShort(i);
        } else {
            da.write(ARRAY_INT_I);
            JdbmUtil.packInt(da, obj.length);
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
            JdbmUtil.packInt(da, -val);
        } else {
            da.write(INTEGER_PACK);
            JdbmUtil.packInt(da, val);
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
            JdbmUtil.packLong(da, -val);
        } else {
            da.write(LONG_PACK);
            JdbmUtil.packLong(da, val);
        }
    }



    static String deserializeString(DataInput buf) throws IOException {
        int len = JdbmUtil.unpackInt(buf);
        char[] b = new char[len];
        for (int i = 0; i < len; i++)
            b[i] = (char) JdbmUtil.unpackInt(buf);

        return new String(b);
    }


    @Override
    public Object deserialize(DataInput is, int capacity) throws IOException {
        return deserialize(is, null);
    }

    public Object deserialize(DataInput is, FastArrayList objectStack) throws IOException {

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
                ret = Integer.valueOf(-1);
                break;
            case INTEGER_0:
                ret = Integer.valueOf(0);
                break;
            case INTEGER_1:
                ret = Integer.valueOf(1);
                break;
            case INTEGER_2:
                ret = Integer.valueOf(2);
                break;
            case INTEGER_3:
                ret = Integer.valueOf(3);
                break;
            case INTEGER_4:
                ret = Integer.valueOf(4);
                break;
            case INTEGER_5:
                ret = Integer.valueOf(5);
                break;
            case INTEGER_6:
                ret = Integer.valueOf(6);
                break;
            case INTEGER_7:
                ret = Integer.valueOf(7);
                break;
            case INTEGER_8:
                ret = Integer.valueOf(8);
                break;
            case INTEGER_MINUS_MAX:
                ret = Integer.valueOf(Integer.MIN_VALUE);
                break;
            case INTEGER_255:
                ret = Integer.valueOf(is.readUnsignedByte());
                break;
            case INTEGER_PACK_NEG:
                ret = Integer.valueOf(-JdbmUtil.unpackInt(is));
                break;
            case INTEGER_PACK:
                ret = Integer.valueOf(JdbmUtil.unpackInt(is));
                break;
            case LONG_MINUS_1:
                ret = Long.valueOf(-1);
                break;
            case LONG_0:
                ret = Long.valueOf(0);
                break;
            case LONG_1:
                ret = Long.valueOf(1);
                break;
            case LONG_2:
                ret = Long.valueOf(2);
                break;
            case LONG_3:
                ret = Long.valueOf(3);
                break;
            case LONG_4:
                ret = Long.valueOf(4);
                break;
            case LONG_5:
                ret = Long.valueOf(5);
                break;
            case LONG_6:
                ret = Long.valueOf(6);
                break;
            case LONG_7:
                ret = Long.valueOf(7);
                break;
            case LONG_8:
                ret = Long.valueOf(8);
                break;
            case LONG_255:
                ret = Long.valueOf(is.readUnsignedByte());
                break;
            case LONG_PACK_NEG:
                ret = Long.valueOf(-JdbmUtil.unpackLong(is));
                break;
            case LONG_PACK:
                ret = Long.valueOf(JdbmUtil.unpackLong(is));
                break;
            case LONG_MINUS_MAX:
                ret = Long.valueOf(Long.MIN_VALUE);
                break;
            case SHORT_MINUS_1:
                ret = Short.valueOf((short) -1);
                break;
            case SHORT_0:
                ret = Short.valueOf((short) 0);
                break;
            case SHORT_1:
                ret = Short.valueOf((short) 1);
                break;
            case SHORT_255:
                ret = Short.valueOf((short) is.readUnsignedByte());
                break;
            case SHORT_FULL:
                ret = Short.valueOf(is.readShort());
                break;
            case BYTE_MINUS_1:
                ret = Byte.valueOf((byte) -1);
                break;
            case BYTE_0:
                ret = Byte.valueOf((byte) 0);
                break;
            case BYTE_1:
                ret = Byte.valueOf((byte) 1);
                break;
            case BYTE_FULL:
                ret = Byte.valueOf(is.readByte());
                break;
            case SHORT_ARRAY:
                int size = JdbmUtil.unpackInt(is);
                ret = new short[size];
                for(int i=0;i<size;i++) ((short[])ret)[i] = is.readShort();
                break;
            case BOOLEAN_ARRAY:
                size = JdbmUtil.unpackInt(is);
                ret = new boolean[size];
                for(int i=0;i<size;i++) ((boolean[])ret)[i] = is.readBoolean();
                break;
            case DOUBLE_ARRAY:
                size = JdbmUtil.unpackInt(is);
                ret = new double[size];
                for(int i=0;i<size;i++) ((double[])ret)[i] = is.readDouble();
                break;
            case FLOAT_ARRAY:
                size = JdbmUtil.unpackInt(is);
                ret = new float[size];
                for(int i=0;i<size;i++) ((float[])ret)[i] = is.readFloat();
                break;
            case CHAR_ARRAY:
                size = JdbmUtil.unpackInt(is);
                ret = new char[size];
                for(int i=0;i<size;i++) ((char[])ret)[i] = is.readChar();
                break;
            case CHAR:
                ret = Character.valueOf(is.readChar());
                break;
            case FLOAT_MINUS_1:
                ret = Float.valueOf(-1);
                break;
            case FLOAT_0:
                ret = Float.valueOf(0);
                break;
            case FLOAT_1:
                ret = Float.valueOf(1);
                break;
            case FLOAT_255:
                ret = Float.valueOf(is.readUnsignedByte());
                break;
            case FLOAT_SHORT:
                ret = Float.valueOf(is.readShort());
                break;
            case FLOAT_FULL:
                ret = Float.valueOf(is.readFloat());
                break;
            case DOUBLE_MINUS_1:
                ret = Double.valueOf(-1);
                break;
            case DOUBLE_0:
                ret = Double.valueOf(0);
                break;
            case DOUBLE_1:
                ret = Double.valueOf(1);
                break;
            case DOUBLE_255:
                ret = Double.valueOf(is.readUnsignedByte());
                break;
            case DOUBLE_SHORT:
                ret = Double.valueOf(is.readShort());
                break;
            case DOUBLE_FULL:
                ret = Double.valueOf(is.readDouble());
                break;
            case BIGINTEGER:
                ret = new BigInteger(deserializeArrayByteInt(is));
                break;
            case BIGDECIMAL:
                ret = new BigDecimal(new BigInteger(deserializeArrayByteInt(is)), JdbmUtil.unpackInt(is));
                break;
            case STRING:
                ret = deserializeString(is);
                break;
            case STRING_EMPTY:
                ret = JdbmUtil.EMPTY_STRING;
                break;

            case CLASS:
                ret = deserializeClass(is);
                break;
            case DATE:
                ret = new Date(is.readLong());
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
            case ARRAY_BYTE_INT:
                ret = deserializeArrayByteInt(is);
                break;
            case LOCALE :
                ret = new Locale(is.readUTF(),is.readUTF(),is.readUTF());
                break;
            case JAVA_SERIALIZATION:
                throw new InternalError("Wrong header, data were probably serialized with OutputStream, not with JDBM serialization");

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
                ret = objectStack.get(JdbmUtil.unpackInt(is));
                break;
            case ARRAYLIST:
                ret = deserializeArrayList(is, objectStack);
                break;
            case ARRAY_OBJECT:
                ret = deserializeArrayObject(is, objectStack);
                break;
            case ARRAY_OBJECT_PACKED_LONG:
                ret = deserializeArrayObjectPackedLong(is);
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

            default:
                throw new InternalError("Unknown serialization header: " + head);
        }

        if (head != OBJECT_STACK && objectStack.size() == oldObjectStackSize) {
            //check if object was not already added to stack as part of collection
            objectStack.add(ret);
        }


        return ret;
    }


    private Class deserializeClass(DataInput is) throws IOException {
        try {
            return Class.forName(is.readUTF());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }


    private byte[] deserializeArrayByteInt(DataInput is) throws IOException {
        int size = JdbmUtil.unpackInt(is);
        byte[] b = new byte[size];
        is.readFully(b);
        return b;
    }


    private long[] deserializeArrayLongL(DataInput is) throws IOException {
        int size = JdbmUtil.unpackInt(is);
        long[] ret = new long[size];
        for (int i = 0; i < size; i++)
            ret[i] = is.readLong();
        return ret;
    }


    private long[] deserializeArrayLongI(DataInput is) throws IOException {
        int size = JdbmUtil.unpackInt(is);
        long[] ret = new long[size];
        for (int i = 0; i < size; i++)
            ret[i] = is.readInt();
        return ret;
    }


    private long[] deserializeArrayLongS(DataInput is) throws IOException {
        int size = JdbmUtil.unpackInt(is);
        long[] ret = new long[size];
        for (int i = 0; i < size; i++)
            ret[i] = is.readShort();
        return ret;
    }


    private long[] deserializeArrayLongB(DataInput is) throws IOException {
        int size = JdbmUtil.unpackInt(is);
        long[] ret = new long[size];
        for (int i = 0; i < size; i++) {
            ret[i] = is.readUnsignedByte();
            if (ret[i] < 0)
                throw new EOFException();
        }
        return ret;
    }


    private int[] deserializeArrayIntIInt(DataInput is) throws IOException {
        int size = JdbmUtil.unpackInt(is);
        int[] ret = new int[size];
        for (int i = 0; i < size; i++)
            ret[i] = is.readInt();
        return ret;
    }


    private int[] deserializeArrayIntSInt(DataInput is) throws IOException {
        int size = JdbmUtil.unpackInt(is);
        int[] ret = new int[size];
        for (int i = 0; i < size; i++)
            ret[i] = is.readShort();
        return ret;
    }


    private int[] deserializeArrayIntBInt(DataInput is) throws IOException {
        int size = JdbmUtil.unpackInt(is);
        int[] ret = new int[size];
        for (int i = 0; i < size; i++) {
            ret[i] = is.readUnsignedByte();
            if (ret[i] < 0)
                throw new EOFException();
        }
        return ret;
    }


    private int[] deserializeArrayIntPack(DataInput is) throws IOException {
        int size = JdbmUtil.unpackInt(is);
        if (size < 0)
            throw new EOFException();

        int[] ret = new int[size];
        for (int i = 0; i < size; i++) {
            ret[i] = JdbmUtil.unpackInt(is);
        }
        return ret;
    }

    private long[] deserializeArrayLongPack(DataInput is) throws IOException {
        int size = JdbmUtil.unpackInt(is);
        if (size < 0)
            throw new EOFException();

        long[] ret = new long[size];
        for (int i = 0; i < size; i++) {
            ret[i] = JdbmUtil.unpackLong(is);
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


    private Object[] deserializeArrayObject(DataInput is, FastArrayList objectStack) throws IOException {
        int size = JdbmUtil.unpackInt(is);
//        // Read class id for components
//        int classId = JdbmUtil.unpackInt(is);
//        Class clazz = classId2class.get(classId);
//        Object[] s = (Object[]) Array.newInstance(clazz, size);
//        objectStack.add(s);
        Object[] s = new Object[size];
        for (int i = 0; i < size; i++)
            s[i] = deserialize(is, objectStack);
        return s;
    }

    private Object[] deserializeArrayObjectPackedLong(DataInput is) throws IOException {
        int size = is.readUnsignedByte();
        Object[] s = new Object[size];
        for (int i = 0; i < size; i++) {
            long l = JdbmUtil.unpackLong(is);
            if (l == 0)
                s[i] = null;
            else
                s[i] = Long.valueOf(l - 1);
        }
        return s;
    }


    private ArrayList<Object> deserializeArrayList(DataInput is, FastArrayList objectStack) throws IOException {
        int size = JdbmUtil.unpackInt(is);
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
            long l = JdbmUtil.unpackLong(is);
            if (l == 0)
                s.add(null);
            else
                s.add(Long.valueOf(l - 1));
        }
        return s;
    }


    private java.util.LinkedList deserializeLinkedList(DataInput is, FastArrayList objectStack) throws IOException {
        int size = JdbmUtil.unpackInt(is);
        java.util.LinkedList s = new java.util.LinkedList();
        objectStack.add(s);
        for (int i = 0; i < size; i++)
            s.add(deserialize(is, objectStack));
        return s;
    }


    private Vector<Object> deserializeVector(DataInput is, FastArrayList objectStack) throws IOException {
        int size = JdbmUtil.unpackInt(is);
        Vector<Object> s = new Vector<Object>(size);
        objectStack.add(s);
        for (int i = 0; i < size; i++)
            s.add(deserialize(is, objectStack));
        return s;
    }


    private HashSet<Object> deserializeHashSet(DataInput is, FastArrayList objectStack) throws IOException {
        int size = JdbmUtil.unpackInt(is);
        HashSet<Object> s = new HashSet<Object>(size);
        objectStack.add(s);
        for (int i = 0; i < size; i++)
            s.add(deserialize(is, objectStack));
        return s;
    }


    private LinkedHashSet<Object> deserializeLinkedHashSet(DataInput is, FastArrayList objectStack) throws IOException {
        int size = JdbmUtil.unpackInt(is);
        LinkedHashSet<Object> s = new LinkedHashSet<Object>(size);
        objectStack.add(s);
        for (int i = 0; i < size; i++)
            s.add(deserialize(is, objectStack));
        return s;
    }


    private TreeSet<Object> deserializeTreeSet(DataInput is, FastArrayList objectStack) throws IOException {
        int size = JdbmUtil.unpackInt(is);
        TreeSet<Object> s = new TreeSet<Object>();
        objectStack.add(s);
        Comparator comparator = (Comparator) deserialize(is, objectStack);
        if (comparator != null)
            s = new TreeSet<Object>(comparator);

        for (int i = 0; i < size; i++)
            s.add(deserialize(is, objectStack));
        return s;
    }


    private TreeMap<Object, Object> deserializeTreeMap(DataInput is, FastArrayList objectStack) throws IOException {
        int size = JdbmUtil.unpackInt(is);

        TreeMap<Object, Object> s = new TreeMap<Object, Object>();
        objectStack.add(s);
        Comparator comparator = (Comparator) deserialize(is, objectStack);
        if (comparator != null)
            s = new TreeMap<Object, Object>(comparator);
        for (int i = 0; i < size; i++)
            s.put(deserialize(is, objectStack), deserialize(is, objectStack));
        return s;
    }


    private HashMap<Object, Object> deserializeHashMap(DataInput is, FastArrayList objectStack) throws IOException {
        int size = JdbmUtil.unpackInt(is);

        HashMap<Object, Object> s = new HashMap<Object, Object>(size);
        objectStack.add(s);
        for (int i = 0; i < size; i++)
            s.put(deserialize(is, objectStack), deserialize(is, objectStack));
        return s;
    }

    private IdentityHashMap<Object, Object> deserializeIdentityHashMap(DataInput is, FastArrayList objectStack) throws IOException {
        int size = JdbmUtil.unpackInt(is);

        IdentityHashMap<Object, Object> s = new IdentityHashMap<Object, Object>(size);
        objectStack.add(s);
        for (int i = 0; i < size; i++)
            s.put(deserialize(is, objectStack), deserialize(is, objectStack));
        return s;
    }

    private LinkedHashMap<Object, Object> deserializeLinkedHashMap(DataInput is, FastArrayList objectStack) throws IOException {
        int size = JdbmUtil.unpackInt(is);

        LinkedHashMap<Object, Object> s = new LinkedHashMap<Object, Object>(size);
        objectStack.add(s);
        for (int i = 0; i < size; i++)
            s.put(deserialize(is, objectStack), deserialize(is, objectStack));
        return s;
    }


    private Hashtable<Object, Object> deserializeHashtable(DataInput is, FastArrayList objectStack) throws IOException {
        int size = JdbmUtil.unpackInt(is);

        Hashtable<Object, Object> s = new Hashtable<Object, Object>(size);
        objectStack.add(s);
        for (int i = 0; i < size; i++)
            s.put(deserialize(is, objectStack), deserialize(is, objectStack));
        return s;
    }


    private Properties deserializeProperties(DataInput is, FastArrayList objectStack) throws IOException {
        int size = JdbmUtil.unpackInt(is);

        Properties s = new Properties();
        objectStack.add(s);
        for (int i = 0; i < size; i++)
            s.put(deserialize(is, objectStack), deserialize(is, objectStack));
        return s;
    }




}
