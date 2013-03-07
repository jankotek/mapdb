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

import java.util.Iterator;
import java.util.Random;
import java.util.TreeMap;

public class LongHashMapTest extends TestCase {

    public void testAll() {
        LongHashMap<String> t = new LongHashMap<String>(){
            @Override protected long hashSaltValue(){return 0;}
        };
        t.put(1, "aa");
        t.put(2, "bb");
        t.put(2, "bb");
        t.put(4, "cc");
        t.put(9, "FF");
        assertEquals(4, t.size());
        t.remove(1);
        assertEquals(3, t.size());
        assertEquals(t.get(1), null);
        assertEquals(t.get(2), "bb");
        assertEquals(t.get(3), null);
        assertEquals(t.get(4), "cc");
        assertEquals(t.get(5), null);
        assertEquals(t.get(-1), null);
        assertEquals(t.get(9), "FF");

        Iterator<String> vals = t.valuesIterator();
        assertTrue(vals.hasNext());
        assertEquals(vals.next(), "bb");
        assertTrue(vals.hasNext());
        assertEquals(vals.next(), "cc");
        assertTrue(vals.hasNext());
        assertEquals(vals.next(), "FF");

        assertFalse(vals.hasNext());

        t.clear();
        assertEquals(0, t.size());
        t.put(2, "bb");
        assertEquals(1, t.size());
        assertEquals(t.get(1), null);
        assertEquals(t.get(3), null);

    }

    public void testRandomCompare() {
        LongHashMap<String> v1 = new LongHashMap<String>();
        TreeMap<Long, String> v2 = new TreeMap<Long, String>();
        Random d = new Random();
        for (int i = 0; i < 1000; i++) {
            long key = d.nextInt() % 100;
            double random = d.nextDouble();
            if (random < 0.8) {
//				System.out.println("put "+key);
                v1.put(key, "" + key);
                v2.put(key, "" + key);
            } else {
//				System.out.println("remove "+key);
                v1.remove(key);
                v2.remove(key);
            }
            checkEquals(v1, v2);

        }
    }

    public void checkEquals(LongMap<String> v1, TreeMap<Long, String> v2) {
        assertEquals(v1.size(), v2.size());
        for (long k : v2.keySet()) {
            assertEquals(v1.get(k), v2.get(k));
        }

        int counter = 0;
        Iterator<String> it = v1.valuesIterator();
        while (it.hasNext()) {
            String v = it.next();
            long key = Long.parseLong(v);
            assertEquals(v1.get(key), v);
            assertEquals("" + key, v);
            counter++;
        }
        assertEquals(counter, v2.size());
    }


    public void test2() {
        LongHashMap<String> v1 = new LongHashMap<String>();
        v1.put(1611, "1611");
        v1.put(15500, "15500");
        v1.put(9446, "9446");
        System.out.println(v1.get(9446));
        System.out.println(v1.toString());
        assertEquals(3, v1.size());
        assertEquals(v1.get(9446), "9446");

    }

    public void testMapIter(){
        LongHashMap<String> v = new LongHashMap<String>();
        v.put(1L, "one");
        v.put(2L, "two");
        v.put(3L, "three");

        TreeMap<Long, String> v2 = new TreeMap<Long, String>();
        v2.put(1L, "one");
        v2.put(2L, "two");
        v2.put(3L, "three");

        TreeMap<Long, String> v3 = new TreeMap<Long, String>();
        LongMap.LongMapIterator<String> iter = v.longMapIterator();
        while(iter.moveToNext()){
            v3.put(iter.key(), iter.value());
        }

        assertEquals(v2,v3);
    }

    public void test_Issue6(){
            LongHashMap<String> t = new LongHashMap<String>();
            t.put(6447459, "aa");
            t.put(6382177, "bb");
            assertEquals("aa",t.get(6447459));
            assertEquals("bb",t.get(6382177));
            assertTrue(t.toString().contains("6382177 => bb"));
            assertTrue(t.toString().contains("6447459 => aa"));
    }

}
