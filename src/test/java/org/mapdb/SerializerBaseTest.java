/*******************************************************************************
 * Copyright 2010 Cees De Groot, Alex Boisvert, Jan Kotek
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.mapdb;

import junit.framework.TestCase;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleEntry;
import java.util.*;

import static org.junit.Assert.assertArrayEquals;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class SerializerBaseTest extends TestCase {

    SerializerBase ser = new SerializerBase();

    private byte[] serialize(Object i) throws IOException {
        DataOutput2 in = new DataOutput2();
        ser.serialize(in, i);
        return in.copyBytes();
    }

    private Object deserialize(byte[] buf) throws IOException {
        return ser.deserialize(new DataInput2(ByteBuffer.wrap(buf),0),-1);
    }

    public void testInt() throws IOException{
        int[] vals = {
                Integer.MIN_VALUE,
                -Short.MIN_VALUE * 2,
                -Short.MIN_VALUE + 1,
                -Short.MIN_VALUE,
                -10, -9, -8, -7, -6, -5, -4, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                127, 254, 255, 256, Short.MAX_VALUE, Short.MAX_VALUE + 1,
                Short.MAX_VALUE * 2, Integer.MAX_VALUE
        };
        for (int i : vals) {
            byte[] buf = serialize(i);
            Object l2 = deserialize(buf);
            assertTrue(l2.getClass() == Integer.class);
            assertEquals(l2, i);
        }
    }



    public void testShort() throws IOException{
        short[] vals = {
                (short) (-Short.MIN_VALUE + 1),
                (short) -Short.MIN_VALUE,
                -10, -9, -8, -7, -6, -5, -4, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                127, 254, 255, 256, Short.MAX_VALUE, Short.MAX_VALUE - 1,
                Short.MAX_VALUE
        };
        for (short i : vals) {
            byte[] buf = serialize(i);
            Object l2 = deserialize(buf);
            assertTrue(l2.getClass() == Short.class);
            assertEquals(l2, i);
        }
    }

    public void testDouble() throws IOException{
        double[] vals = {
                1f, 0f, -1f, Math.PI, 255, 256, Short.MAX_VALUE, Short.MAX_VALUE + 1, -100
        };
        for (double i : vals) {
            byte[] buf = serialize(i);
            Object l2 = deserialize(buf);
            assertTrue(l2.getClass() == Double.class);
            assertEquals(l2, i);
        }
    }


    public void testFloat() throws IOException{
        float[] vals = {
                1f, 0f, -1f, (float) Math.PI, 255, 256, Short.MAX_VALUE, Short.MAX_VALUE + 1, -100
        };
        for (float i : vals) {
            byte[] buf = serialize(i);
            Object l2 = deserialize(buf);
            assertTrue(l2.getClass() == Float.class);
            assertEquals(l2, i);
        }
    }

    public void testChar() throws IOException{
        char[] vals = {
                'a', ' '
        };
        for (char i : vals) {
            byte[] buf = serialize(i);
            Object l2 = deserialize(buf);
            assertEquals(l2.getClass(), Character.class);
            assertEquals(l2, i);
        }
    }


    public void testLong() throws IOException{
        long[] vals = {
                Long.MIN_VALUE,
                Integer.MIN_VALUE, (long)Integer.MIN_VALUE - 1, (long)Integer.MIN_VALUE + 1,
                -Short.MIN_VALUE * 2,
                -Short.MIN_VALUE + 1,
                -Short.MIN_VALUE,
                -10, -9, -8, -7, -6, -5, -4, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                127, 254, 255, 256, Short.MAX_VALUE, Short.MAX_VALUE + 1,
                Short.MAX_VALUE * 2, Integer.MAX_VALUE, (long)Integer.MAX_VALUE + 1, Long.MAX_VALUE
        };
        for (long i : vals) {
            byte[] buf = serialize(i);
            Object l2 = deserialize(buf);
            assertTrue(l2.getClass() == Long.class);
            assertEquals(l2, i);
        }
    }

    public void testBoolean1() throws IOException{
        byte[] buf = serialize(true);
        Object l2 = deserialize(buf);
        assertTrue(l2.getClass() == Boolean.class);
        assertEquals(l2, true);

        byte[] buf2 = serialize(false);
        Object l22 = deserialize(buf2);
        assertTrue(l22.getClass() == Boolean.class);
        assertEquals(l22, false);

    }

    public void testString() throws IOException{
        byte[] buf = serialize("Abcd");
        String l2 = (String) deserialize(buf);
        assertEquals(l2, "Abcd");
    }

    public void testBigString() throws IOException{
        String bigString = "";
        for (int i = 0; i < 1e4; i++)
            bigString += i % 10;
        byte[] buf = serialize(bigString);
        String l2 = (String) deserialize(buf);
        assertEquals(l2, bigString);
    }


    public void testNoArgumentConstructorInJavaSerialization() throws ClassNotFoundException, IOException {
        SimpleEntry a = new SimpleEntry(1, "11");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        new ObjectOutputStream(out).writeObject(a);
        ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(out.toByteArray()));
        SimpleEntry a2 = (SimpleEntry) in.readObject();
        assertEquals(a, a2);
    }


    public void testArrayList() throws ClassNotFoundException, IOException {
        Collection c = new ArrayList();
        for (int i = 0; i < 200; i++)
            c.add(i);
        assertEquals(c, deserialize(serialize(c)));
        for (int i = 0; i < 2000; i++)
            c.add(i);
        assertEquals(c, deserialize(serialize(c)));
    }

    public void testLinkedList() throws ClassNotFoundException, IOException {
        Collection c = new java.util.LinkedList();
        for (int i = 0; i < 200; i++)
            c.add(i);
        assertEquals(c, deserialize(serialize(c)));
        for (int i = 0; i < 2000; i++)
            c.add(i);
        assertEquals(c, deserialize(serialize(c)));
    }

    public void testVector() throws ClassNotFoundException, IOException {
        Collection c = new Vector();
        for (int i = 0; i < 200; i++)
            c.add(i);
        assertEquals(c, deserialize(serialize(c)));
        for (int i = 0; i < 2000; i++)
            c.add(i);
        assertEquals(c, deserialize(serialize(c)));
    }


    public void testTreeSet() throws ClassNotFoundException, IOException {
        Collection c = new TreeSet();
        for (int i = 0; i < 200; i++)
            c.add(i);
        assertEquals(c, deserialize(serialize(c)));
        for (int i = 0; i < 2000; i++)
            c.add(i);
        assertEquals(c, deserialize(serialize(c)));
    }

    public void testHashSet() throws ClassNotFoundException, IOException {
        Collection c = new HashSet();
        for (int i = 0; i < 200; i++)
            c.add(i);
        assertEquals(c, deserialize(serialize(c)));
        for (int i = 0; i < 2000; i++)
            c.add(i);
        assertEquals(c, deserialize(serialize(c)));
    }

    public void testLinkedHashSet() throws ClassNotFoundException, IOException {
        Collection c = new LinkedHashSet();
        for (int i = 0; i < 200; i++)
            c.add(i);
        assertEquals(c, deserialize(serialize(c)));
        for (int i = 0; i < 2000; i++)
            c.add(i);
        assertEquals(c, deserialize(serialize(c)));
    }

    public void testHashMap() throws ClassNotFoundException, IOException {
        Map c = new HashMap();
        for (int i = 0; i < 200; i++)
            c.put(i, i + 10000);
        assertEquals(c, deserialize(serialize(c)));
        for (int i = 0; i < 2000; i++)
            c.put(i, i + 10000);
        assertEquals(c, deserialize(serialize(c)));
    }

    public void testTreeMap() throws ClassNotFoundException, IOException {
        Map c = new TreeMap();
        for (int i = 0; i < 200; i++)
            c.put(i, i + 10000);
        assertEquals(c, deserialize(serialize(c)));
        for (int i = 0; i < 2000; i++)
            c.put(i, i + 10000);
        assertEquals(c, deserialize(serialize(c)));
    }

    public void testLinkedHashMap() throws ClassNotFoundException, IOException {
        Map c = new LinkedHashMap();
        for (int i = 0; i < 200; i++)
            c.put(i, i + 10000);
        assertEquals(c, deserialize(serialize(c)));
        for (int i = 0; i < 2000; i++)
            c.put(i, i + 10000);
        assertEquals(c, deserialize(serialize(c)));
    }

    public void testHashtable() throws ClassNotFoundException, IOException {
        Map c = new Hashtable();
        for (int i = 0; i < 200; i++)
            c.put(i, i + 10000);
        assertEquals(c, deserialize(serialize(c)));
        for (int i = 0; i < 2000; i++)
            c.put(i, i + 10000);
        assertEquals(c, deserialize(serialize(c)));
    }

    public void testProperties() throws ClassNotFoundException, IOException {
        Properties c = new Properties();
        for (int i = 0; i < 200; i++)
            c.put(i, i + 10000);
        assertEquals(c, deserialize(serialize(c)));
        for (int i = 0; i < 2000; i++)
            c.put(i, i + 10000);
        assertEquals(c, deserialize(serialize(c)));
    }


    public void testClass() throws IOException{
        byte[] buf = serialize(String.class);
        Class l2 = (Class) deserialize(buf);
        assertEquals(l2, String.class);
    }

    public void testClass2() throws IOException{
        byte[] buf = serialize(long[].class);
        Class l2 = (Class) deserialize(buf);
        assertEquals(l2, long[].class);
    }


    public void testUnicodeString() throws ClassNotFoundException, IOException {
        String s = "Ciudad Bolíva";
        byte[] buf = serialize(s);
        assertTrue("text is not unicode", buf.length != s.length());
        Object l2 = deserialize(buf);
        assertEquals(l2, s);
    }

    public void testPackedLongCollection() throws ClassNotFoundException, IOException {
        ArrayList l1 = new ArrayList();
        l1.add(0L);
        l1.add(1L);
        l1.add(0L);
        assertEquals(l1, deserialize(serialize(l1)));
        l1.add(-1L);
        assertEquals(l1, deserialize(serialize(l1)));
    }

    public void testNegativeLongsArray() throws ClassNotFoundException, IOException {
       long[] l = new long[] { -12 };
       Object deserialize = deserialize(serialize(l));
       assertTrue(Arrays.equals(l, (long[]) deserialize));
     }


    public void testNegativeIntArray() throws ClassNotFoundException, IOException {
       int[] l = new int[] { -12 };
       Object deserialize = deserialize(serialize(l));
       assertTrue(Arrays.equals(l, (int[]) deserialize));
     }


    public void testNegativeShortArray() throws ClassNotFoundException, IOException {
       short[] l = new short[] { -12 };
       Object deserialize = deserialize(serialize(l));
        assertTrue(Arrays.equals(l, (short[]) deserialize));
     }

    public void testBooleanArray() throws ClassNotFoundException, IOException {
        boolean[] l = new boolean[] { true,false };
        Object deserialize = deserialize(serialize(l));
        assertTrue(Arrays.equals(l, (boolean[]) deserialize));
    }

    public void testDoubleArray() throws ClassNotFoundException, IOException {
        double[] l = new double[] { Math.PI, 1D };
        Object deserialize = deserialize(serialize(l));
        assertTrue(Arrays.equals(l, (double[]) deserialize));
    }

    public void testFloatArray() throws ClassNotFoundException, IOException {
        float[] l = new float[] { 1F, 1.234235F };
        Object deserialize = deserialize(serialize(l));
        assertTrue(Arrays.equals(l, (float[]) deserialize));
    }

    public void testByteArray() throws ClassNotFoundException, IOException {
        byte[] l = new byte[] { 1,34,-5 };
        Object deserialize = deserialize(serialize(l));
        assertTrue(Arrays.equals(l, (byte[]) deserialize));
    }

    public void testCharArray() throws ClassNotFoundException, IOException {
        char[] l = new char[] { '1','a','&' };
        Object deserialize = deserialize(serialize(l));
        assertTrue(Arrays.equals(l, (char[]) deserialize));
    }


    public void testDate() throws IOException{
        Date d = new Date(6546565565656L);
        assertEquals(d, deserialize(serialize(d)));
        d = new Date(System.currentTimeMillis());
        assertEquals(d, deserialize(serialize(d)));
    }

    public void testBigDecimal() throws IOException{
        BigDecimal d = new BigDecimal("445656.7889889895165654423236");
        assertEquals(d, deserialize(serialize(d)));
        d = new BigDecimal("-53534534534534445656.7889889895165654423236");
        assertEquals(d, deserialize(serialize(d)));
    }

    public void testBigInteger() throws IOException{
        BigInteger d = new BigInteger("4456567889889895165654423236");
        assertEquals(d, deserialize(serialize(d)));
        d = new BigInteger("-535345345345344456567889889895165654423236");
        assertEquals(d, deserialize(serialize(d)));
    }


    public void testLocale() throws Exception{
        assertEquals(Locale.FRANCE, deserialize(serialize(Locale.FRANCE)));
        assertEquals(Locale.CANADA_FRENCH, deserialize(serialize(Locale.CANADA_FRENCH)));
        assertEquals(Locale.SIMPLIFIED_CHINESE, deserialize(serialize(Locale.SIMPLIFIED_CHINESE)));

    }

    public void testUUID() throws IOException, ClassNotFoundException {
        //try a bunch of UUIDs.
        for(int i = 0; i < 1000;i++)
        {
            UUID uuid = UUID.randomUUID();
            assertEquals(uuid, deserialize(serialize(uuid)));
        }
    }

    public void testArray(){
        Object[] o = new Object[]{"A",Long.valueOf(1),Long.valueOf(2),Long.valueOf(3), Long.valueOf(3)};
        Object[] o2 = (Object[]) Utils.clone(o, Serializer.BASIC_SERIALIZER);
        assertArrayEquals(o,o2);
    }


    public void test_issue_38(){
        String[] s = new String[5];
        String[] s2 = (String[]) Utils.clone(s, Serializer.BASIC_SERIALIZER);
        assertArrayEquals(s, s2);
        assertTrue(s2.toString().contains("[Ljava.lang.String"));
    }

    public void test_multi_dim_array(){
        int[][] arr = new int[][]{{11,22,44},{1,2,34}};
        int[][] arr2= (int[][]) Utils.clone(arr, Serializer.BASIC_SERIALIZER);
        assertArrayEquals(arr,arr2);
    }

    public void test_multi_dim_large_array(){
        int[][] arr1 = new int[3000][];
        double[][] arr2 = new double[3000][];
        for(int i=0;i<3000;i++){
            arr1[i]= new int[]{i,i+1};
            arr2[i]= new double[]{i,i+1};
        }
        assertArrayEquals(arr1, (Object[]) Utils.clone(arr1, Serializer.BASIC_SERIALIZER));
        assertArrayEquals(arr2, (Object[]) Utils.clone(arr2, Serializer.BASIC_SERIALIZER));
    }


    public void test_multi_dim_array2(){
        Object[][] arr = new Object[][]{{11,22,44},{1,2,34}};
        Object[][] arr2= (Object[][]) Utils.clone(arr, Serializer.BASIC_SERIALIZER);
        assertArrayEquals(arr,arr2);
    }


    public void test_static_objects(){
        for(Object o:SerializerBase.knownSerializable.get){
            assertTrue(o==Utils.clone(o, Serializer.BASIC_SERIALIZER));
        }
    }

    public void test_tuple_key_serializer(){
        assertEquals(BTreeKeySerializer.TUPLE2, Utils.clone(BTreeKeySerializer.TUPLE2,SerializerBase.BASIC_SERIALIZER));
        assertEquals(BTreeKeySerializer.TUPLE3, Utils.clone(BTreeKeySerializer.TUPLE3,SerializerBase.BASIC_SERIALIZER));
        assertEquals(BTreeKeySerializer.TUPLE4, Utils.clone(BTreeKeySerializer.TUPLE4,SerializerBase.BASIC_SERIALIZER));
    }


    public void test_strings_var_sizes() throws IOException {
        for(int i=0;i<50;i++){
            String s = Utils.randomString(i);
            assertEquals(s, deserialize(serialize(s)));
        }
    }

}

