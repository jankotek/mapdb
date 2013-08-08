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
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.AbstractMap.SimpleEntry;
import java.util.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class SerializerBaseTest extends TestCase {


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
        for (Integer i : vals) {
            Object l2 = clone(i);
            assertEquals(i, l2);
            assertTrue(l2.getClass() == Integer.class);
        }
    }



    public void testShort() throws IOException{
        for (int i = Short.MIN_VALUE;i<=Short.MAX_VALUE;i++) {
            Short ii = (short)i;
            Object l2 = clone(ii);
            assertEquals(ii,l2);
            assertTrue(l2.getClass() == Short.class);
        }
    }

    public void testDouble() throws IOException{
        double[] vals = {
                1f, 0f, -1f, Math.PI, 255, 256, Short.MAX_VALUE, Short.MAX_VALUE + 1, -100
        };
        for (double i : vals) {
            Object l2 = clone(i);
            assertTrue(l2.getClass() == Double.class);
            assertEquals(l2, i);
        }
    }


    public void testFloat() throws IOException{
        float[] vals = {
                1f, 0f, -1f, (float) Math.PI, 255, 256, Short.MAX_VALUE, Short.MAX_VALUE + 1, -100
        };
        for (float i : vals) {
            Object l2 = clone(i);
            assertTrue(l2.getClass() == Float.class);
            assertEquals(l2, i);
        }
    }

    public void testChar() throws IOException{
        for (int ii = Character.MIN_VALUE;ii<=Character.MAX_VALUE;ii++) {
            Character i = (char)ii;
            Object l2 = clone(i);
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
            Object l2 = clone(i);
            assertTrue(l2.getClass() == Long.class);
            assertEquals(l2, i);
        }
    }

    public void testBoolean1() throws IOException{
        Object l2 = clone(true);
        assertTrue(l2.getClass() == Boolean.class);
        assertEquals(l2, true);

        Object l22 = clone(false);
        assertTrue(l22.getClass() == Boolean.class);
        assertEquals(l22, false);

    }

    public void testString() throws IOException{
        String l2 = (String) clone("Abcd");
        assertEquals(l2, "Abcd");
    }

    public void testBigString() throws IOException{
        String bigString = "";
        for (int i = 0; i < 1e4; i++)
            bigString += i % 10;
        String l2 = clone(bigString);
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
        assertEquals(c, clone((c)));
        for (int i = 0; i < 2000; i++)
            c.add(i);
        assertEquals(c, clone((c)));
    }

    public void testLinkedList() throws ClassNotFoundException, IOException {
        Collection c = new java.util.LinkedList();
        for (int i = 0; i < 200; i++)
            c.add(i);
        assertEquals(c, clone((c)));
        for (int i = 0; i < 2000; i++)
            c.add(i);
        assertEquals(c, clone((c)));
    }



    public void testTreeSet() throws ClassNotFoundException, IOException {
        Collection c = new TreeSet();
        for (int i = 0; i < 200; i++)
            c.add(i);
        assertEquals(c, clone((c)));
        for (int i = 0; i < 2000; i++)
            c.add(i);
        assertEquals(c, clone((c)));
    }

    public void testHashSet() throws ClassNotFoundException, IOException {
        Collection c = new HashSet();
        for (int i = 0; i < 200; i++)
            c.add(i);
        assertEquals(c, clone((c)));
        for (int i = 0; i < 2000; i++)
            c.add(i);
        assertEquals(c, clone((c)));
    }

    public void testLinkedHashSet() throws ClassNotFoundException, IOException {
        Collection c = new LinkedHashSet();
        for (int i = 0; i < 200; i++)
            c.add(i);
        assertEquals(c, clone((c)));
        for (int i = 0; i < 2000; i++)
            c.add(i);
        assertEquals(c, clone((c)));
    }

    public void testHashMap() throws ClassNotFoundException, IOException {
        Map c = new HashMap();
        for (int i = 0; i < 200; i++)
            c.put(i, i + 10000);
        assertEquals(c, clone((c)));
        for (int i = 0; i < 2000; i++)
            c.put(i, i + 10000);
        assertEquals(c, clone((c)));
    }

    public void testTreeMap() throws ClassNotFoundException, IOException {
        Map c = new TreeMap();
        for (int i = 0; i < 200; i++)
            c.put(i, i + 10000);
        assertEquals(c, clone((c)));
        for (int i = 0; i < 2000; i++)
            c.put(i, i + 10000);
        assertEquals(c, clone((c)));
    }

    public void testLinkedHashMap() throws ClassNotFoundException, IOException {
        Map c = new LinkedHashMap();
        for (int i = 0; i < 200; i++)
            c.put(i, i + 10000);
        assertEquals(c, clone((c)));
        for (int i = 0; i < 2000; i++)
            c.put(i, i + 10000);
        assertEquals(c, clone((c)));
    }


    public void testProperties() throws ClassNotFoundException, IOException {
        Properties c = new Properties();
        for (int i = 0; i < 200; i++)
            c.put(i, i + 10000);
        assertEquals(c, clone((c)));
        for (int i = 0; i < 2000; i++)
            c.put(i, i + 10000);
        assertEquals(c, clone((c)));
    }


    public void testClass() throws IOException{
        assertEquals(clone(String.class), String.class);
        assertEquals(clone(long[].class), long[].class);
    }


    public void testUnicodeString() throws ClassNotFoundException, IOException {
        String s = "Ciudad Bolíva";
        assertEquals(clone(s), s);
    }

    public void testPackedLongCollection() throws ClassNotFoundException, IOException {
        ArrayList l1 = new ArrayList();
        l1.add(0L);
        l1.add(1L);
        l1.add(0L);
        assertEquals(l1, clone((l1)));
        l1.add(-1L);
        assertEquals(l1, clone((l1)));
    }

    public void testNegativeLongsArray() throws ClassNotFoundException, IOException {
       long[] l = new long[] { -12 };
       Object deserialize = clone((l));
       assertTrue(Arrays.equals(l, (long[]) deserialize));
     }


    public void testNegativeIntArray() throws ClassNotFoundException, IOException {
       int[] l = new int[] { -12 };
       Object deserialize = clone((l));
       assertTrue(Arrays.equals(l, (int[]) deserialize));
     }


    public void testNegativeShortArray() throws ClassNotFoundException, IOException {
       short[] l = new short[] { -12 };
       Object deserialize = clone((l));
        assertTrue(Arrays.equals(l, (short[]) deserialize));
     }

    public void testBooleanArray() throws ClassNotFoundException, IOException {
        boolean[] l = new boolean[] { true,false };
        Object deserialize = clone((l));
        assertTrue(Arrays.equals(l, (boolean[]) deserialize));
    }

    public void testDoubleArray() throws ClassNotFoundException, IOException {
        double[] l = new double[] { Math.PI, 1D };
        Object deserialize = clone((l));
        assertTrue(Arrays.equals(l, (double[]) deserialize));
    }

    public void testFloatArray() throws ClassNotFoundException, IOException {
        float[] l = new float[] { 1F, 1.234235F };
        Object deserialize = clone((l));
        assertTrue(Arrays.equals(l, (float[]) deserialize));
    }

    public void testByteArray() throws ClassNotFoundException, IOException {
        byte[] l = new byte[] { 1,34,-5 };
        Object deserialize = clone((l));
        assertTrue(Arrays.equals(l, (byte[]) deserialize));
    }

    public void testCharArray() throws ClassNotFoundException, IOException {
        char[] l = new char[] { '1','a','&' };
        Object deserialize = clone((l));
        assertTrue(Arrays.equals(l, (char[]) deserialize));
    }


    public void testDate() throws IOException{
        Date d = new Date(6546565565656L);
        assertEquals(d, clone((d)));
        d = new Date(System.currentTimeMillis());
        assertEquals(d, clone((d)));
    }

    public void testBigDecimal() throws IOException{
        BigDecimal d = new BigDecimal("445656.7889889895165654423236");
        assertEquals(d, clone((d)));
        d = new BigDecimal("-53534534534534445656.7889889895165654423236");
        assertEquals(d, clone((d)));
    }

    public void testBigInteger() throws IOException{
        BigInteger d = new BigInteger("4456567889889895165654423236");
        assertEquals(d, clone((d)));
        d = new BigInteger("-535345345345344456567889889895165654423236");
        assertEquals(d, clone((d)));
    }


    public void testUUID() throws IOException, ClassNotFoundException {
        //try a bunch of UUIDs.
        for(int i = 0; i < 1000;i++)
        {
            UUID uuid = UUID.randomUUID();
            assertEquals(uuid, clone((uuid)));
        }
    }

    public void testArray() throws IOException {
        Object[] o = new Object[]{"A",Long.valueOf(1),Long.valueOf(2),Long.valueOf(3), Long.valueOf(3)};
        Object[] o2 = (Object[]) clone(o);
        assertArrayEquals(o,o2);
    }


    public void test_issue_38() throws IOException {
        String[] s = new String[5];
        String[] s2 = (String[]) clone(s);
        assertArrayEquals(s, s2);
        assertTrue(s2.toString().contains("[Ljava.lang.String"));
    }

    public void test_multi_dim_array() throws IOException {
        int[][] arr = new int[][]{{11,22,44},{1,2,34}};
        int[][] arr2= (int[][]) clone(arr);
        assertArrayEquals(arr,arr2);
    }

    public void test_multi_dim_large_array() throws IOException {
        int[][] arr1 = new int[3000][];
        double[][] arr2 = new double[3000][];
        for(int i=0;i<3000;i++){
            arr1[i]= new int[]{i,i+1};
            arr2[i]= new double[]{i,i+1};
        }
        assertArrayEquals(arr1, (Object[]) clone(arr1));
        assertArrayEquals(arr2, (Object[]) clone(arr2));
    }


    public void test_multi_dim_array2() throws IOException {
        Object[][] arr = new Object[][]{{11,22,44},{1,2,34}};
        Object[][] arr2= (Object[][]) clone(arr);
        assertArrayEquals(arr,arr2);
    }


    public void test_static_objects() throws IOException {
        for(Object o:SerializerBase.knownSerializable.get){
            assertTrue(o==clone(o));
        }
    }

    public void test_tuple_key_serializer() throws IOException {
        assertEquals(BTreeKeySerializer.TUPLE2, clone(BTreeKeySerializer.TUPLE2));
        assertEquals(BTreeKeySerializer.TUPLE3, clone(BTreeKeySerializer.TUPLE3));
        assertEquals(BTreeKeySerializer.TUPLE4, clone(BTreeKeySerializer.TUPLE4));
    }


    public void test_strings_var_sizes() throws IOException {
        for(int i=0;i<50;i++){
            String s = Utils.randomString(i);
            assertEquals(s, clone((s)));
        }
    }


    public void test_extended_chars() throws IOException {
        String s = "人口, 日本、人口, 日本の公式統計";
        assertEquals(s,clone((s)));
    }

    public void testBooleanArray2() throws IOException {
        for(int i=0;i<1000;i++){
            boolean[] b = new boolean[i];
            for(int j=0;j<i;j++) b[j] = Math.random()<0.5;

            boolean[] b2 = (boolean[]) clone((b));

            for(int j=0;j<i;j++) assertEquals(b[j], b2[j]);
        }
    }

    /** clone value using serialization */
    <E> E clone(E value) throws IOException {
        return Utils.clone(value,(Serializer<E>)Serializer.BASIC);
    }

    public static class SerializerBaseTestWithJUDataStreams extends SerializerBaseTest{
        @Override
        <E> E clone(E value) throws IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Serializer.BASIC.serialize(new DataOutputStream(out), value);

            return (E) Serializer.BASIC.deserialize(new DataInputStream(new ByteArrayInputStream(out.toByteArray())),-1);
        }
    }

    @SuppressWarnings({  "rawtypes" })
    public void testHeaderUnique() throws IllegalAccessException {
        Class c = SerializerBase.Header.class;
        Set<Integer> s = new TreeSet<Integer>();
        for (Field f : c.getDeclaredFields()) {
            f.setAccessible(true);
            int value = f.getInt(null);

            assertTrue("Value already used: " + value, !s.contains(value));
            s.add(value);
        }
        assertTrue(!s.isEmpty());
    }

    @SuppressWarnings({  "rawtypes" })
    public void testHeaderUniqueMapDB() throws IllegalAccessException {
        Class c = SerializerBase.HeaderMapDB.class;
        Set<Integer> s = new TreeSet<Integer>();
        for (Field f : c.getDeclaredFields()) {
            f.setAccessible(true);
            int value = f.getInt(null);

            assertTrue("Value already used: " + value, !s.contains(value));
            s.add(value);
        }
        assertTrue(!s.isEmpty());
    }


    public void test_All_Serializer_Fields_Serializable() throws IllegalAccessException, IOException {
        for(Field f:Serializer.class.getDeclaredFields()){
            Object a = f.get(null);
            assertTrue("field: "+f.getName(), SerializerBase.knownSerializable.get.contains(a));
            assertEquals("field: "+f.getName(),a,clone(a));
        }
    }

}