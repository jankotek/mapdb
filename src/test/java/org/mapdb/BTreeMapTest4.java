/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.mapdb;

import java.io.Serializable;
import java.text.CollationKey;
import java.text.Collator;
import java.util.*;
import java.util.Map.Entry;


public class BTreeMapTest4 extends junit.framework.TestCase {

    protected <K,V> BTreeMap<K,V> newBTreeMap(Map map) {
        BTreeMap ret = DBMaker.newMemoryDB()
                .cacheDisable()
                .asyncWriteDisable().writeAheadLogDisable().make()
                .createTreeMap("test",6,false,false, null,null, null);
        ret.putAll(map);
        return ret;
    }

    protected <K,V> BTreeMap<K,V> newBTreeMap(Comparator comp) {
        return DBMaker.newMemoryDB()
                .cacheDisable()
                .asyncWriteDisable().writeAheadLogDisable().make()
                .createTreeMap("test",6,false,false, null,null, comp);
    }

    protected static <K,V> BTreeMap<K,V> newBTreeMap() {
        return DBMaker.newMemoryDB()
                .cacheDisable()
                .asyncWriteDisable().writeAheadLogDisable().make()
                .getTreeMap("test");
    }

    public static class ReversedComparator implements Comparator,Serializable {
        public int compare(Object o1, Object o2) {
            return -(((Comparable) o1).compareTo(o2));
        }

        public boolean equals(Object o1, Object o2) {
            return (((Comparable) o1).compareTo(o2)) == 0;
        }
    }

    // Regression for Harmony-1026
    public static class MockComparator<T extends Comparable<T>> implements
            Comparator<T>, Serializable {

        public int compare(T o1, T o2) {
            if (o1 == o2) {
                return 0;
            }
            if (null == o1 || null == o2) {
                return -1;
            }
            T c1 = o1;
            T c2 = o2;
            return c1.compareTo(c2);
        }
    }


    BTreeMap tm;

    Object objArray[] = new Object[1000];


    /**
     * @tests java.util.TreeMap#TreeMap(java.util.Comparator)
     */
    public void test_ConstructorLjava_util_Comparator() {
        // Test for method java.util.TreeMap(java.util.Comparator)
        Comparator comp = new ReversedComparator();
        BTreeMap reversedTreeMap = newBTreeMap(comp);
        assertEquals("TreeMap answered incorrect comparator", reversedTreeMap
                .comparator().getClass().toString(),comp.getClass().toString());
        reversedTreeMap.put(new Integer(1).toString(), new Integer(1));
        reversedTreeMap.put(new Integer(2).toString(), new Integer(2));
        assertTrue("TreeMap does not use comparator (firstKey was incorrect)",
                reversedTreeMap.firstKey().equals(new Integer(2).toString()));
        assertTrue("TreeMap does not use comparator (lastKey was incorrect)",
                reversedTreeMap.lastKey().equals(new Integer(1).toString()));

    }





    /**
     * @tests java.util.TreeMap#clear()
     */
    public void test_clear() {
        // Test for method void java.util.TreeMap.clear()
        tm.clear();
        assertEquals("Cleared map returned non-zero size", 0, tm.size());
    }


    /**
     * @tests java.util.TreeMap#comparator()
     */
    public void test_comparator() {
        // Test for method java.util.Comparator java.util.TreeMap.comparator()\
        Comparator comp = new ReversedComparator();
        BTreeMap reversedTreeMap = newBTreeMap(comp);
        assertTrue("TreeMap answered incorrect comparator", reversedTreeMap
                .comparator() == comp);
        reversedTreeMap.put(new Integer(1).toString(), new Integer(1));
        reversedTreeMap.put(new Integer(2).toString(), new Integer(2));
        assertTrue("TreeMap does not use comparator (firstKey was incorrect)",
                reversedTreeMap.firstKey().equals(new Integer(2).toString()));
        assertTrue("TreeMap does not use comparator (lastKey was incorrect)",
                reversedTreeMap.lastKey().equals(new Integer(1).toString()));
    }

    /**
     * @tests java.util.TreeMap#containsKey(java.lang.Object)
     */
    public void test_containsKeyLjava_lang_Object() {
        // Test for method boolean
        // java.util.TreeMap.containsKey(java.lang.Object)
        assertTrue("Returned false for valid key", tm.containsKey("95"));
        assertTrue("Returned true for invalid key", !tm.containsKey("XXXXX"));
    }

    /**
     * @tests java.util.TreeMap#containsValue(java.lang.Object)
     */
    public void test_containsValueLjava_lang_Object() {
        // Test for method boolean
        // java.util.TreeMap.containsValue(java.lang.Object)
        assertTrue("Returned false for valid value", tm
                .containsValue(objArray[986]));
        assertTrue("Returned true for invalid value", !tm
                .containsValue(new Object()));
    }

    /**
     * @tests java.util.TreeMap#entrySet()
     */
    public void test_entrySet() {
        // Test for method java.util.Set java.util.TreeMap.entrySet()
        Set anEntrySet = tm.entrySet();
        Iterator entrySetIterator = anEntrySet.iterator();
        assertTrue("EntrySet is incorrect size",
                anEntrySet.size() == objArray.length);
        Map.Entry entry;
        while (entrySetIterator.hasNext()) {
            entry = (Map.Entry) entrySetIterator.next();
            assertEquals("EntrySet does not contain correct mappings", tm
                    .get(entry.getKey()), entry.getValue());
        }
    }

    /**
     * @tests java.util.TreeMap#firstKey()
     */
    public void test_firstKey() {
        // Test for method java.lang.Object java.util.TreeMap.firstKey()
        assertEquals("Returned incorrect first key", "0", tm.firstKey());
    }

    /**
     * @tests java.util.TreeMap#get(java.lang.Object)
     */
    public void test_getLjava_lang_Object() {
        // Test for method java.lang.Object
        // java.util.TreeMap.get(java.lang.Object)
        Object o = Long.MIN_VALUE;
        tm.put("Hello", o);
        assertEquals("Failed to get mapping", tm.get("Hello"), o);
        
		// Test for the same key & same value
		tm = newBTreeMap();
		Object o2 = Long.MAX_VALUE;
		Integer key1 = 1;
		Integer key2 = 2;
		assertNull(tm.put(key1, o));
		assertNull(tm.put(key2, o));
		assertEquals(2, tm.values().size());
		assertEquals(2, tm.keySet().size());
		assertEquals(tm.get(key1), tm.get(key2));
		assertEquals(o, tm.put(key1, o2));
		assertEquals(o2, tm.get(key1));
    }


    // Regression for ill-behaved collator
    static class IllBehavedCollator extends Collator implements Serializable {
        @Override
        public int compare(String o1, String o2) {
            if (o1 == null) {
                return 0;
            }
            return o1.compareTo(o2);
        }

        @Override
        public CollationKey getCollationKey(String string) {
            return null;
        }

        @Override
        public int hashCode() {
            return 0;
        }
    }

    /**
	 * @tests java.util.TreeMap#headMap(java.lang.Object)
	 */
    public void test_headMapLjava_lang_Object() {
        // Test for method java.util.SortedMap
        // java.util.TreeMap.headMap(java.lang.Object)
        Map head = tm.headMap("100");
        assertEquals("Returned map of incorrect size", 3, head.size());
        assertTrue("Returned incorrect elements", head.containsKey("0")
                && head.containsValue(new Integer("1"))
                && head.containsKey("10"));

        // Regression for Harmony-1026
        BTreeMap<Integer, Double> map = newBTreeMap(
                new MockComparator());
        map.put(1, 2.1);
        map.put(2, 3.1);
        map.put(3, 4.5);
        map.put(7, 21.3);

        SortedMap<Integer, Double> smap = map.headMap(-1);
        assertEquals(0, smap.size());

        Set<Integer> keySet = smap.keySet();
        assertEquals(0, keySet.size());

        Set<Map.Entry<Integer, Double>> entrySet = smap.entrySet();
        assertEquals(0, entrySet.size());

        Collection<Double> valueCollection = smap.values();
        assertEquals(0, valueCollection.size());

//        // Regression for Harmony-1066
//        assertTrue(head instanceof Serializable);


        BTreeMap<String, String> treemap = newBTreeMap(new IllBehavedCollator());
//        assertEquals(0, treemap.headMap(null).size());
        
        treemap = newBTreeMap();
		SortedMap<String, String> headMap =  treemap.headMap("100");
		headMap.headMap("100");

	SortedMap<Integer,Integer> intMap,sub;
        int size = 16;
        intMap = newBTreeMap();
        for(int i=0; i<size; i++) {
            intMap.put(i,i);
        }
        sub = intMap.headMap(-1);
        assertEquals("size should be zero",sub.size(),0);
        assertTrue("submap should be empty",sub.isEmpty());
        try{
            sub.firstKey();
            fail("java.util.NoSuchElementException should be thrown");
        } catch(java.util.NoSuchElementException e) {
        }
		
		BTreeMap t = newBTreeMap();
		try {
            SortedMap th = t.headMap(null);
            fail("Should throw a NullPointerException");
        } catch( NullPointerException npe) {
            // expected
        }

        try{
            sub.lastKey();
            fail("java.util.NoSuchElementException should be thrown");
        } catch(java.util.NoSuchElementException e) {
        }

        size = 256;
        intMap = newBTreeMap();
        for(int i=0; i<size; i++) {
            intMap.put(i,i);
        }
        sub = intMap.headMap(-1);
        assertEquals("size should be zero",sub.size(),0);
        assertTrue("submap should be empty",sub.isEmpty());
        try{
            sub.firstKey();
            fail("java.util.NoSuchElementException should be thrown");
        } catch(java.util.NoSuchElementException e) {
        }
        
        try{
            sub.lastKey();
            fail("java.util.NoSuchElementException should be thrown");
        } catch(java.util.NoSuchElementException e) {
        }

    }

    /**
     * @tests java.util.TreeMap#keySet()
     */
    public void test_keySet() {
        // Test for method java.util.Set java.util.TreeMap.keySet()
        Set ks = tm.keySet();
        assertTrue("Returned set of incorrect size",
                ks.size() == objArray.length);
        for (int i = 0; i < tm.size(); i++) {
            assertTrue("Returned set is missing keys", ks.contains(new Integer(
                    i).toString()));
        }
    }

    /**
     * @tests java.util.TreeMap#lastKey()
     */
    public void test_lastKey() {
        // Test for method java.lang.Object java.util.TreeMap.lastKey()
        assertTrue("Returned incorrect last key", tm.lastKey().equals(
                objArray[objArray.length - 1].toString()));
        assertNotSame(objArray[objArray.length - 1].toString(), tm.lastKey());
		assertEquals(objArray[objArray.length - 2].toString(), tm
				.headMap("999").lastKey());
		assertEquals(objArray[objArray.length - 1].toString(), tm
				.tailMap("123").lastKey());
		assertEquals(objArray[objArray.length - 2].toString(), tm.subMap("99",
				"999").lastKey());
    }
	
	public void test_lastKey_after_subMap() {
		BTreeMap<String, String> tm = newBTreeMap();
		tm.put("001", "VAL001");
		tm.put("003", "VAL003");
		tm.put("002", "VAL002");
		SortedMap<String, String> sm = tm;
		String firstKey = (String) sm.firstKey();
		String lastKey="";
		for (int i = 1; i <= tm.size(); i++) {
			try{
				lastKey = (String) sm.lastKey();
			}
			catch(NoSuchElementException excep){
				fail("NoSuchElementException thrown when there are elements in the map");
			}
			sm = sm.subMap(firstKey, lastKey);
		}
	}

    /**
     * @tests java.util.TreeMap#put(java.lang.Object, java.lang.Object)
     */
    public void test_putLjava_lang_ObjectLjava_lang_Object() {
        // Test for method java.lang.Object
        // java.util.TreeMap.put(java.lang.Object, java.lang.Object)
        Object o = Long.MIN_VALUE;
        tm.put("Hello", o);
        assertEquals("Failed to put mapping", tm.get("Hello") , o);

        // regression for Harmony-780
        tm = newBTreeMap();
        assertNull(tm.put(new Object(), new Object()));
        try {
            tm.put(new Integer(1), new Object());
            fail("should throw ClassCastException");
        } catch (ClassCastException e) {
            // expected
        }

        tm = newBTreeMap();
        assertNull(tm.put(new Integer(1), new Object()));
        
        try {
			tm.put(new Object(), new Object());
			fail("Should throw a ClassCastException");
		} catch (ClassCastException e) {
			// expected
		}

//        // regression for Harmony-2474
//        // but RI6 changes its behavior
//        // so the test changes too
//        tm = newBTreeMap();
//        try {
//            tm.remove(o);
//            fail("should throw ClassCastException");
//        } catch (ClassCastException e) {
//            //expected
//        }
    }

    /**
     * @tests java.util.TreeMap#putAll(java.util.Map)
     */
    public void test_putAllLjava_util_Map() {
        // Test for method void java.util.TreeMap.putAll(java.util.Map)
        BTreeMap x = newBTreeMap();
        x.putAll(tm);
        assertTrue("Map incorrect size after put", x.size() == tm.size());
        for (Object element : objArray) {
            assertTrue("Failed to put all elements", x.get(element.toString())
                    .equals(element));
        }
    }

    /**
     * @tests java.util.TreeMap#remove(java.lang.Object)
     */
    public void test_removeLjava_lang_Object() {
        // Test for method java.lang.Object
        // java.util.TreeMap.remove(java.lang.Object)
        tm.remove("990");
        assertTrue("Failed to remove mapping", !tm.containsKey("990"));

    }

    /**
     * @tests java.util.TreeMap#size()
     */
    public void test_size() {
        // Test for method int java.util.TreeMap.size()
        assertEquals("Returned incorrect size", 1000, tm.size());
		assertEquals("Returned incorrect size", 447, tm.headMap("500").size());
		assertEquals("Returned incorrect size", 1000, tm.headMap("null").size());
		assertEquals("Returned incorrect size", 0, tm.headMap("").size());
		assertEquals("Returned incorrect size", 448, tm.headMap("500a").size());
		assertEquals("Returned incorrect size", 553, tm.tailMap("500").size());
		assertEquals("Returned incorrect size", 0, tm.tailMap("null").size());
		assertEquals("Returned incorrect size", 1000, tm.tailMap("").size());
		assertEquals("Returned incorrect size", 552, tm.tailMap("500a").size());
		assertEquals("Returned incorrect size", 111, tm.subMap("500", "600")
				.size());
		try {
			tm.subMap("null", "600");
			fail("Should throw an IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			// expected
		}
		assertEquals("Returned incorrect size", 1000, tm.subMap("", "null")
				.size()); 
    }

    /**
	 * @tests java.util.TreeMap#subMap(java.lang.Object, java.lang.Object)
	 */
    public void test_subMapLjava_lang_ObjectLjava_lang_Object() {
        // Test for method java.util.SortedMap
        // java.util.TreeMap.subMap(java.lang.Object, java.lang.Object)
        SortedMap subMap = tm.subMap(objArray[100].toString(), objArray[109]
                .toString());
        assertEquals("subMap is of incorrect size", 9, subMap.size());
        for (int counter = 100; counter < 109; counter++) {
            assertTrue("SubMap contains incorrect elements", subMap.get(
                    objArray[counter].toString()).equals(objArray[counter]));
        }

        try {
            tm.subMap(objArray[9].toString(), objArray[1].toString());
            fail("end key less than start key should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        // Regression test for typo in lastKey method
        SortedMap<String, String> map = newBTreeMap();
        map.put("1", "one"); //$NON-NLS-1$ //$NON-NLS-2$
        map.put("2", "two"); //$NON-NLS-1$ //$NON-NLS-2$
        map.put("3", "three"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("3", map.lastKey());
        SortedMap<String, String> sub = map.subMap("1", "3"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("2", sub.lastKey()); //$NON-NLS-1$
        
        BTreeMap t = newBTreeMap();
        try {
			SortedMap th = t.subMap(null,new Object());
			fail("Should throw a NullPointerException");
        } catch( NullPointerException npe) {
        	// expected
        }
    }
    
    
    /**
     * @tests java.util.TreeMap#subMap(java.lang.Object, java.lang.Object)
     */
    public void test_subMap_Iterator() {
        BTreeMap<String, String> map = newBTreeMap();

        String[] keys = { "1", "2", "3" };
        String[] values = { "one", "two", "three" };
        for (int i = 0; i < keys.length; i++) {
            map.put(keys[i], values[i]);
        }

        assertEquals(3, map.size());

        Map subMap = map.subMap("", "test");
        assertEquals(3, subMap.size());

        Set entrySet = subMap.entrySet();
        Iterator iter = entrySet.iterator();
        int size = 0;
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = (Map.Entry<String, String>) iter
                    .next();
            assertTrue(map.containsKey(entry.getKey()));
            assertTrue(map.containsValue(entry.getValue()));
            size++;
        }
        assertEquals(map.size(), size);

        Set<String> keySet = subMap.keySet();
        iter = keySet.iterator();
        size = 0;
        while (iter.hasNext()) {
            String key = (String) iter.next();
            assertTrue(map.containsKey(key));
            size++;
        }
        assertEquals(map.size(), size);
    }


    /**
     * @tests java.util.TreeMap#tailMap(java.lang.Object)
     */
    public void test_tailMapLjava_lang_Object() {
        // Test for method java.util.SortedMap
        // java.util.TreeMap.tailMap(java.lang.Object)
        Map tail = tm.tailMap(objArray[900].toString());
        assertTrue("Returned map of incorrect size : " + tail.size(), tail
                .size() == (objArray.length - 900) + 9);
        for (int i = 900; i < objArray.length; i++) {
            assertTrue("Map contains incorrect entries", tail
                    .containsValue(objArray[i]));
        }

//        // Regression for Harmony-1066
//        assertTrue(tail instanceof Serializable);

	SortedMap<Integer,Integer> intMap,sub;
        int size = 16;
        intMap = newBTreeMap();
        for(int i=0; i<size; i++) {
            intMap.put(i,i);
        }
        sub = intMap.tailMap(size);
        assertEquals("size should be zero",sub.size(),0);
        assertTrue("submap should be empty",sub.isEmpty());
        ;
        try{
            sub.firstKey();
            fail("java.util.NoSuchElementException should be thrown");
        } catch(java.util.NoSuchElementException e) {
        }
        
        BTreeMap t = newBTreeMap();
        try {
            SortedMap th = t.tailMap(null);
            fail("Should throw a NullPointerException");
        } catch( NullPointerException npe) {
            // expected
        }


        try{
            sub.lastKey();
            fail("java.util.NoSuchElementException should be thrown");
        } catch(java.util.NoSuchElementException e) {
        }

        size = 256;
        intMap = newBTreeMap();
        for(int i=0; i<size; i++) {
            intMap.put(i,i);
        }
        sub = intMap.tailMap(size);
        assertEquals("size should be zero",sub.size(),0);
        assertTrue("submap should be empty",sub.isEmpty());
        try{
            sub.firstKey();
            fail("java.util.NoSuchElementException should be thrown");
        } catch(java.util.NoSuchElementException e) {
        }
        
        try{
            sub.lastKey();
            fail("java.util.NoSuchElementException should be thrown");
        } catch(java.util.NoSuchElementException e) {
        }

    }

    /**
     * @tests java.util.TreeMap#values()
     */
    public void test_values() {
        // Test for method java.util.Collection java.util.TreeMap.values()
        Collection vals = tm.values();
        vals.iterator();
        assertTrue("Returned collection of incorrect size",
                vals.size() == objArray.length);
        for (Object element : objArray) {
            assertTrue("Collection contains incorrect elements", vals
                    .contains(element));
        }
        assertEquals(1000, vals.size());
        int j = 0;
        for (Iterator iter = vals.iterator(); iter.hasNext();) {
            Object element = (Object) iter.next();
            j++;
        }
        assertEquals(1000, j);
        
//        vals = tm.descendingMap().values();
//        vals.iterator();
        assertTrue("Returned collection of incorrect size",
                vals.size() == objArray.length);
        for (Object element : objArray) {
            assertTrue("Collection contains incorrect elements", vals
                    .contains(element));
        }
        assertEquals(1000, vals.size());
        j = 0;
        for (Iterator iter = vals.iterator(); iter.hasNext();) {
            Object element = (Object) iter.next();
            j++;
        }
        assertEquals(1000, j);
        
        BTreeMap myTreeMap = newBTreeMap();
        for (int i = 0; i < 100; i++) {
            myTreeMap.put(objArray[i], objArray[i]);
        }
        Collection col = myTreeMap.values();

        // contains
        assertTrue("UnmodifiableCollectionTest - should contain 0", col
                .contains(new Integer(0)));
        assertTrue("UnmodifiableCollectionTest - should contain 50", col
                .contains(new Integer(50)));
        assertTrue("UnmodifiableCollectionTest - should not contain 100", !col
                .contains(new Integer(100)));

        // containsAll
        HashSet<Integer> hs = new HashSet<Integer>();
        hs.add(new Integer(0));
        hs.add(new Integer(25));
        hs.add(new Integer(99));
        assertTrue(
                "UnmodifiableCollectionTest - should contain set of 0, 25, and 99",
                col.containsAll(hs));
        hs.add(new Integer(100));
        assertTrue(
                "UnmodifiableCollectionTest - should not contain set of 0, 25, 99 and 100",
                !col.containsAll(hs));

        // isEmpty
        assertTrue("UnmodifiableCollectionTest - should not be empty", !col
                .isEmpty());

        // iterator
        Iterator<Integer> it = col.iterator();
        SortedSet<Integer> ss = new TreeSet<Integer>();
        while (it.hasNext()) {
            ss.add(it.next());
        }
        it = ss.iterator();
        for (int counter = 0; it.hasNext(); counter++) {
            int nextValue = it.next().intValue();
            assertTrue(
                    "UnmodifiableCollectionTest - Iterator returned wrong value.  Wanted: "
                            + counter + " got: " + nextValue,
                    nextValue == counter);
        }

        // size
        assertTrue(
                "UnmodifiableCollectionTest - returned wrong size.  Wanted 100, got: "
                        + col.size(), col.size() == 100);

        // toArray
        Object[] objArray;
        objArray = col.toArray();
        for (int counter = 0; it.hasNext(); counter++) {
            assertTrue(
                    "UnmodifiableCollectionTest - toArray returned incorrect array",
                    objArray[counter] == it.next());
        }

        // toArray (Object[])
        objArray = new Object[100];
        col.toArray(objArray);
        for (int counter = 0; it.hasNext(); counter++) {
            assertTrue(
                    "UnmodifiableCollectionTest - toArray(Object) filled array incorrectly",
                    objArray[counter] == it.next());
        }
        col.remove(new Integer(0));
        assertTrue(
                "Removing from the values collection should remove from the original map",
                !myTreeMap.containsValue(new Integer(0)));
        assertEquals(99, col.size());
        j = 0;
        for (Iterator iter = col.iterator(); iter.hasNext();) {
            Object element = (Object) iter.next();
            j++;
        }
        assertEquals(99, j);
        
    }
    
    /**
     * @tests java.util.TreeMap the values() method in sub maps
     */
    public void test_subMap_values_size() {
        BTreeMap myTreeMap = newBTreeMap();
        for (int i = 0; i < 1000; i++) {
            myTreeMap.put(i, objArray[i]);
        }
        // Test for method values() in subMaps
        Collection vals = myTreeMap.subMap(200, 400).values();
        assertTrue("Returned collection of incorrect size", vals.size() == 200);
        for (int i = 200; i < 400; i++) {
            assertTrue("Collection contains incorrect elements" + i, vals
                    .contains(objArray[i]));
        }
        assertEquals(200,vals.toArray().length);
        vals.remove(objArray[300]);
        assertTrue(
                "Removing from the values collection should remove from the original map",
                !myTreeMap.containsValue(objArray[300]));
        assertTrue("Returned collection of incorrect size", vals.size() == 199);
        assertEquals(199,vals.toArray().length);

        myTreeMap.put(300, objArray[300]);
        // Test for method values() in subMaps
        vals = myTreeMap.headMap(400).values();
        assertEquals("Returned collection of incorrect size", vals.size(), 400);
        for (int i = 0; i < 400; i++) {
            assertTrue("Collection contains incorrect elements "+i, vals
                    .contains(objArray[i]));
        }
        assertEquals(400,vals.toArray().length);
        vals.remove(objArray[300]);
        assertTrue(
                "Removing from the values collection should remove from the original map",
                !myTreeMap.containsValue(objArray[300]));
        assertTrue("Returned collection of incorrect size", vals.size() == 399);
        assertEquals(399,vals.toArray().length);
        
        myTreeMap.put(300, objArray[300]);
        // Test for method values() in subMaps
        vals = myTreeMap.tailMap(400).values();
        assertEquals("Returned collection of incorrect size", vals.size(), 600);
        for (int i = 400; i < 1000; i++) {
            assertTrue("Collection contains incorrect elements "+i, vals
                    .contains(objArray[i]));
        }
        assertEquals(600,vals.toArray().length);
        vals.remove(objArray[600]);
        assertTrue(
                "Removing from the values collection should remove from the original map",
                !myTreeMap.containsValue(objArray[600]));
        assertTrue("Returned collection of incorrect size", vals.size() == 599);
        assertEquals(599,vals.toArray().length);
        
        
        myTreeMap.put(600, objArray[600]);
        // Test for method values() in subMaps
        vals = myTreeMap.tailMap(401).values();
        assertEquals("Returned collection of incorrect size", vals.size(), 599);
        for (int i = 401; i < 1000; i++) {
            assertTrue("Collection contains incorrect elements "+i, vals
                    .contains(objArray[i]));
        }
        assertEquals(599,vals.toArray().length);
        vals.remove(objArray[600]);
        assertTrue(
                "Removing from the values collection should remove from the original map",
                !myTreeMap.containsValue(objArray[600]));
        assertTrue("Returned collection of incorrect size", vals.size() == 598);
        assertEquals(598,vals.toArray().length);
        
        myTreeMap.put(600, objArray[600]);
        // Test for method values() in subMaps
        vals = myTreeMap.headMap(401).values();
        assertEquals("Returned collection of incorrect size", vals.size(), 401);
        for (int i = 0; i <= 400; i++) {
            assertTrue("Collection contains incorrect elements "+i, vals
                    .contains(objArray[i]));
        }
        assertEquals(401,vals.toArray().length);
        vals.remove(objArray[300]);
        assertTrue(
                "Removing from the values collection should remove from the original map",
                !myTreeMap.containsValue(objArray[300]));
        assertTrue("Returned collection of incorrect size", vals.size() == 400);
        assertEquals(400,vals.toArray().length);
        
    }
    
    /**
     * @tests java.util.TreeMap#subMap()
     */
    public void test_subMap_Iterator2() {
        BTreeMap<String, String> map = newBTreeMap();

        String[] keys = { "1", "2", "3" };
        String[] values = { "one", "two", "three" };
        for (int i = 0; i < keys.length; i++) {
            map.put(keys[i], values[i]);
        }

        assertEquals(3, map.size());

        Map subMap = map.subMap("", "test");
        assertEquals(3, subMap.size());

        Set entrySet = subMap.entrySet();
        Iterator iter = entrySet.iterator();
        int size = 0;
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = (Map.Entry<String, String>) iter
                    .next();
            assertTrue(map.containsKey(entry.getKey()));
            assertTrue(map.containsValue(entry.getValue()));
            size++;
        }
        assertEquals(map.size(), size);

        Set<String> keySet = subMap.keySet();
        iter = keySet.iterator();
        size = 0;
        while (iter.hasNext()) {
            String key = (String) iter.next();
            assertTrue(map.containsKey(key));
            size++;
        }
        assertEquals(map.size(), size);
    }


    /**
     * @tests {@link java.util.TreeMap#firstEntry()}
     */
    public void test_firstEntry() throws Exception {
        Integer testint = new Integer(-1);
        Integer testint10000 = new Integer(-10000);
        Integer testint9999 = new Integer(-9999);
        assertEquals(objArray[0].toString(), tm.firstEntry().getKey());
        assertEquals(objArray[0], tm.firstEntry().getValue());
        tm.put(testint.toString(), testint);
        assertEquals(testint.toString(), tm.firstEntry().getKey());
        assertEquals(testint, tm.firstEntry().getValue());
        tm.put(testint10000.toString(), testint10000);
        assertEquals(testint.toString(), tm.firstEntry().getKey());
        assertEquals(testint, tm.firstEntry().getValue());
        tm.put(testint9999.toString(), testint9999);
        assertEquals(testint.toString(), tm.firstEntry().getKey());
        Entry entry = tm.firstEntry();
        assertEquals(testint, entry.getValue());
        assertEntry(entry);
        tm.clear();
        assertNull(tm.firstEntry());
    }

    /**
     * @tests {@link java.util.TreeMap#lastEntry()
     */
    public void test_lastEntry() throws Exception {
        Integer testint10000 = new Integer(10000);
        Integer testint9999 = new Integer(9999);
        assertEquals(objArray[999].toString(), tm.lastEntry().getKey());
        assertEquals(objArray[999], tm.lastEntry().getValue());
        tm.put(testint10000.toString(), testint10000);
        assertEquals(objArray[999].toString(), tm.lastEntry().getKey());
        assertEquals(objArray[999], tm.lastEntry().getValue());
        tm.put(testint9999.toString(), testint9999);
        assertEquals(testint9999.toString(), tm.lastEntry().getKey());
        Entry entry = tm.lastEntry();
        assertEquals(testint9999, entry.getValue());
        assertEntry(entry);
        tm.clear();
        assertNull(tm.lastEntry());
    }

    /**
     * @tests {@link java.util.TreeMap#pollFirstEntry()
     */
    public void test_pollFirstEntry() throws Exception {
        Integer testint = new Integer(-1);
        Integer testint10000 = new Integer(-10000);
        Integer testint9999 = new Integer(-9999);
        assertEquals(objArray[0].toString(), tm.pollFirstEntry().getKey());
        assertEquals(objArray[1], tm.pollFirstEntry().getValue());
        assertEquals(objArray[10], tm.pollFirstEntry().getValue());
        tm.put(testint.toString(), testint);
        tm.put(testint10000.toString(), testint10000);
        assertEquals(testint.toString(), tm.pollFirstEntry().getKey());
        assertEquals(testint10000, tm.pollFirstEntry().getValue());
        tm.put(testint9999.toString(), testint9999);
        assertEquals(testint9999.toString(), tm.pollFirstEntry().getKey());
        Entry entry = tm.pollFirstEntry();
        assertEntry(entry);
        assertEquals(objArray[100], entry.getValue());
        tm.clear();
        assertNull(tm.pollFirstEntry());
    }

    /**
     * @tests {@link java.util.TreeMap#pollLastEntry()
     */
    public void test_pollLastEntry() throws Exception {
        Integer testint10000 = new Integer(10000);
        Integer testint9999 = new Integer(9999);
        assertEquals(objArray[999].toString(), tm.pollLastEntry().getKey());
        assertEquals(objArray[998], tm.pollLastEntry().getValue());
        assertEquals(objArray[997], tm.pollLastEntry().getValue());
        tm.put(testint10000.toString(), testint10000);
        assertEquals(objArray[996], tm.pollLastEntry().getValue());
        tm.put(testint9999.toString(), testint9999);
        assertEquals(testint9999.toString(), tm.pollLastEntry().getKey());
        Entry entry = tm.pollLastEntry();
        assertEquals(objArray[995], entry.getValue());
        assertEntry(entry);
        tm.clear();
        assertNull(tm.pollLastEntry());
    }

    public void testLastFirstEntryOnEmpty(){
        tm.clear();
        assertNull(tm.firstEntry());
        assertNull(tm.lastEntry());
    }

    /**
     * @tests {@link java.util.TreeMap#lowerEntry(Object)
     */
    public void test_lowerEntry() throws Exception {
        Integer testint10000 = new Integer(10000);
        Integer testint9999 = new Integer(9999);
        assertEquals(objArray[999], tm.lowerEntry(testint9999.toString())
                .getValue());
        assertEquals(objArray[100], tm.lowerEntry(testint10000.toString())
                .getValue());
        tm.put(testint10000.toString(), testint10000);
        tm.put(testint9999.toString(), testint9999);
        assertEquals(objArray[999], tm.lowerEntry(testint9999.toString())
                .getValue());
        Entry entry = tm.lowerEntry(testint10000.toString());
        assertEquals(objArray[100], entry.getValue());
        assertEntry(entry);
        try {
            tm.lowerEntry(testint10000);
            fail("should throw ClassCastException");
        } catch (ClassCastException e) {
            // expected
        }
        try {
            tm.lowerEntry(null);
            fail("should throw NullPointerException");
        } catch (NullPointerException e) {
            // expected
        }
        tm.clear();
        assertNull(tm.lowerEntry(testint9999.toString()));
//        assertNull(tm.lowerEntry(null));
    }

    /**
     * @tests {@link java.util.TreeMap#lowerKey(Object)
     */
    public void test_lowerKey() throws Exception {
        Integer testint10000 = new Integer(10000);
        Integer testint9999 = new Integer(9999);
        assertEquals(objArray[999].toString(), tm.lowerKey(testint9999
                .toString()));
        assertEquals(objArray[100].toString(), tm.lowerKey(testint10000
                .toString()));
        tm.put(testint10000.toString(), testint10000);
        tm.put(testint9999.toString(), testint9999);
        assertEquals(objArray[999].toString(), tm.lowerKey(testint9999
                .toString()));
        assertEquals(objArray[100].toString(), tm.lowerKey(testint10000
                .toString()));
        try {
            tm.lowerKey(testint10000);
            fail("should throw ClassCastException");
        } catch (ClassCastException e) {
            // expected
        }
        try {
            tm.lowerKey(null);
            fail("should throw NullPointerException");
        } catch (NullPointerException e) {
            // expected
        }
        tm.clear();
        assertNull(tm.lowerKey(testint9999.toString()));
//        assertNull(tm.lowerKey(null));
    }

    /**
     * @tests {@link java.util.TreeMap#floorEntry(Object)
     */
    public void test_floorEntry() throws Exception {
        Integer testint10000 = new Integer(10000);
        Integer testint9999 = new Integer(9999);
        assertEquals(objArray[999], tm.floorEntry(testint9999.toString())
                .getValue());
        assertEquals(objArray[100], tm.floorEntry(testint10000.toString())
                .getValue());
        tm.put(testint10000.toString(), testint10000);
        tm.put(testint9999.toString(), testint9999);
        assertEquals(testint9999, tm.floorEntry(testint9999.toString())
                .getValue());
        Entry entry = tm.floorEntry(testint10000.toString());
        assertEquals(testint10000, entry.getValue());
        assertEntry(entry);
        try {
            tm.floorEntry(testint10000);
            fail("should throw ClassCastException");
        } catch (ClassCastException e) {
            // expected
        }
        try {
            tm.floorEntry(null);
            fail("should throw NullPointerException");
        } catch (NullPointerException e) {
            // expected
        }
        tm.clear();
        assertNull(tm.floorEntry(testint9999.toString()));
    }

    /**
     * @tests {@link java.util.TreeMap#floorKey(Object)
     */
    public void test_floorKey() throws Exception {
        Integer testint10000 = new Integer(10000);
        Integer testint9999 = new Integer(9999);
        assertEquals(objArray[999].toString(), tm.floorKey(testint9999
                .toString()));
        assertEquals(objArray[100].toString(), tm.floorKey(testint10000
                .toString()));
        tm.put(testint10000.toString(), testint10000);
        tm.put(testint9999.toString(), testint9999);
        assertEquals(testint9999.toString(), tm
                .floorKey(testint9999.toString()));
        assertEquals(testint10000.toString(), tm.floorKey(testint10000
                .toString()));
        try {
            tm.floorKey(testint10000);
            fail("should throw ClassCastException");
        } catch (ClassCastException e) {
            // expected
        }
        try {
            tm.floorKey(null);
            fail("should throw NullPointerException");
        } catch (NullPointerException e) {
            // expected
        }
        tm.clear();
        assertNull(tm.floorKey(testint9999.toString()));
//        assertNull(tm.floorKey(null));
    }

    /**
     * @tests {@link java.util.TreeMap#ceilingEntry(Object)
     */
    public void test_ceilingEntry() throws Exception {
        Integer testint100 = new Integer(100);
        Integer testint = new Integer(-1);
        assertEquals(objArray[0], tm.ceilingEntry(testint.toString())
                .getValue());
        assertEquals(objArray[100], tm.ceilingEntry(testint100.toString())
                .getValue());
        tm.put(testint.toString(), testint);
        tm.put(testint100.toString(), testint);
        assertEquals(testint, tm.ceilingEntry(testint.toString()).getValue());
        Entry entry = tm.ceilingEntry(testint100.toString());
        assertEquals(testint, entry.getValue());
        assertEntry(entry);
        try {
            tm.ceilingEntry(testint100);
            fail("should throw ClassCastException");
        } catch (ClassCastException e) {
            // expected
        }
        try {
            tm.ceilingEntry(null);
            fail("should throw NullPointerException");
        } catch (NullPointerException e) {
            // expected
        }
//        tm.clear();
//        assertNull(tm.ceilingEntry(testint.toString()));
//        assertNull(tm.ceilingEntry(null));
    }

    /**
     * @tests {@link java.util.TreeMap#ceilingKey(Object)
     */
    public void test_ceilingKey() throws Exception {
        Integer testint100 = new Integer(100);
        Integer testint = new Integer(-1);
        assertEquals(objArray[0].toString(), tm.ceilingKey(testint.toString()));
        assertEquals(objArray[100].toString(), tm.ceilingKey(testint100
                .toString()));
        tm.put(testint.toString(), testint);
        tm.put(testint100.toString(), testint);
        assertEquals(testint.toString(), tm.ceilingKey(testint.toString()));
        assertEquals(testint100.toString(), tm
                .ceilingKey(testint100.toString()));
        try {
            tm.ceilingKey(testint100);
            fail("should throw ClassCastException");
        } catch (ClassCastException e) {
            // expected
        }
        try {
            tm.ceilingKey(null);
            fail("should throw NullPointerException");
        } catch (NullPointerException e) {
            // expected
        }
//        tm.clear();
//        assertNull(tm.ceilingKey(testint.toString()));
//        assertNull(tm.ceilingKey(null));
    }

    /**
     * @tests {@link java.util.TreeMap#higherEntry(Object)
     */
    public void test_higherEntry() throws Exception {
        Integer testint9999 = new Integer(9999);
        Integer testint10000 = new Integer(10000);
        Integer testint100 = new Integer(100);
        Integer testint = new Integer(-1);
        assertEquals(objArray[0], tm.higherEntry(testint.toString()).getValue());
        assertEquals(objArray[101], tm.higherEntry(testint100.toString())
                .getValue());
        assertEquals(objArray[101], tm.higherEntry(testint10000.toString())
                .getValue());
        tm.put(testint9999.toString(), testint);
        tm.put(testint100.toString(), testint);
        tm.put(testint10000.toString(), testint);
        assertEquals(objArray[0], tm.higherEntry(testint.toString()).getValue());
        assertEquals(testint, tm.higherEntry(testint100.toString()).getValue());
        Entry entry = tm.higherEntry(testint10000.toString());
        assertEquals(objArray[101], entry.getValue());
        assertEntry(entry);
        assertNull(tm.higherEntry(testint9999.toString()));
        try {
            tm.higherEntry(testint100);
            fail("should throw ClassCastException");
        } catch (ClassCastException e) {
            // expected
        }
        try {
            tm.higherEntry(null);
            fail("should throw NullPointerException");
        } catch (NullPointerException e) {
            // expected
        }
//        tm.clear();
//        assertNull(tm.higherEntry(testint.toString()));
//        assertNull(tm.higherEntry(null));
    }

    /**
     * @tests {@link java.util.TreeMap#higherKey(Object)
     */
    public void test_higherKey() throws Exception {
        Integer testint9999 = new Integer(9999);
        Integer testint10000 = new Integer(10000);
        Integer testint100 = new Integer(100);
        Integer testint = new Integer(-1);
        assertEquals(objArray[0].toString(), tm.higherKey(testint.toString()));
        assertEquals(objArray[101].toString(), tm.higherKey(testint100
                .toString()));
        assertEquals(objArray[101].toString(), tm.higherKey(testint10000
                .toString()));
        tm.put(testint9999.toString(), testint);
        tm.put(testint100.toString(), testint);
        tm.put(testint10000.toString(), testint);
        assertEquals(objArray[0].toString(), tm.higherKey(testint.toString()));
        assertEquals(testint10000.toString(), tm.higherKey(testint100
                .toString()));
        assertEquals(objArray[101].toString(), tm.higherKey(testint10000
                .toString()));
        assertNull(tm.higherKey(testint9999.toString()));
        try {
            tm.higherKey(testint100);
            fail("should throw ClassCastException");
        } catch (ClassCastException e) {
            // expected
        }
        try {
            tm.higherKey(null);
            fail("should throw NullPointerException");
        } catch (NullPointerException e) {
            // expected
        }
//        tm.clear();
//        assertNull(tm.higherKey(testint.toString()));
//        assertNull(tm.higherKey(null));
    }

    public void test_navigableKeySet() throws Exception {
        Integer testint9999 = new Integer(9999);
        Integer testint10000 = new Integer(10000);
        Integer testint100 = new Integer(100);
        Integer testint0 = new Integer(0);
        NavigableSet set = tm.navigableKeySet();
        assertFalse(set.contains(testint9999.toString()));
        tm.put(testint9999.toString(), testint9999);
        assertTrue(set.contains(testint9999.toString()));
        tm.remove(testint9999.toString());
        assertFalse(set.contains(testint9999.toString()));
        try {
            set.add(new Object());
            fail("should throw UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }
        try {
            set.add(null);
            fail("should throw UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }
        try {
            set.addAll(null);
            fail("should throw UnsupportedOperationException");
        } catch (NullPointerException e) {
            // expected
        }
        Collection collection = new LinkedList();
        set.addAll(collection);
        try {
            collection.add(new Object());
            set.addAll(collection);
            fail("should throw UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }
        set.remove(testint100.toString());
        assertFalse(tm.containsKey(testint100.toString()));
        assertTrue(tm.containsKey(testint0.toString()));
        Iterator iter = set.iterator();
        iter.next();
        iter.remove();
        assertFalse(tm.containsKey(testint0.toString()));
        collection.add(new Integer(200).toString());
        set.retainAll(collection);
        assertEquals(1, tm.size());
        set.removeAll(collection);
        assertEquals(0, tm.size());
        tm.put(testint10000.toString(), testint10000);
        assertEquals(1, tm.size());
        set.clear();
        assertEquals(0, tm.size());
    }

    private void assertEntry(Entry entry) {
        try {
            entry.setValue(new Object());
            fail("should throw UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }
        assertEquals((entry.getKey() == null ? 0 : entry.getKey().hashCode())
                ^ (entry.getValue() == null ? 0 : entry.getValue().hashCode()),
                entry.hashCode());
        assertEquals(entry.toString(), entry.getKey() + "=" + entry.getValue());
    }

    /**
     * @tests java.util.TreeMap#subMap(java.lang.Object,boolean,
     *        java.lang.Object,boolean)
     */
    public void test_subMapLjava_lang_ObjectZLjava_lang_ObjectZ() {
        // normal case
        SortedMap subMap = tm.subMap(objArray[100].toString(), true,
                objArray[109].toString(), true);
        assertEquals("subMap is of incorrect size", 10, subMap.size());
        subMap = tm.subMap(objArray[100].toString(), true, objArray[109]
                .toString(), false);
        assertEquals("subMap is of incorrect size", 9, subMap.size());
        for (int counter = 100; counter < 109; counter++) {
            assertTrue("SubMap contains incorrect elements", subMap.get(
                    objArray[counter].toString()).equals(objArray[counter]));
        }
        subMap = tm.subMap(objArray[100].toString(), false, objArray[109]
                .toString(), true);
        assertEquals("subMap is of incorrect size", 9, subMap.size());
        assertNull(subMap.get(objArray[100].toString()));

        // Exceptions
        try {
            tm.subMap(objArray[9].toString(), true, objArray[1].toString(),
                    true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            tm.subMap(objArray[9].toString(), false, objArray[1].toString(),
                    false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            tm.subMap(null, true, null, true);
            fail("should throw NullPointerException");
        } catch (NullPointerException e) {
            // expected
        }
        try {
            tm.subMap(null, false, objArray[100], true);
            fail("should throw NullPointerException");
        } catch (NullPointerException e) {
            // expected
        }
        try {
            tm.subMap(new LinkedList(), false, objArray[100], true);
            fail("should throw ClassCastException");
        } catch (ClassCastException e) {
            // expected
        }

        // use integer elements to test
        BTreeMap treeMapInt = newBTreeMap();
        assertEquals(0, treeMapInt.subMap(new Integer(-1), true,
                new Integer(100), true).size());
        for (int i = 0; i < 100; i++) {
            treeMapInt.put(new Integer(i), new Integer(i).toString());
        }
        SortedMap<Integer, String> result = treeMapInt.subMap(new Integer(-1),
                true, new Integer(100), true);
        assertEquals(100, result.size());
        result.put(new Integer(-1), new Integer(-1).toString());
        assertEquals(101, result.size());
        assertEquals(101, treeMapInt.size());
        result = treeMapInt
                .subMap(new Integer(50), true, new Integer(60), true);
        assertEquals(11, result.size());
        try {
            result.put(new Integer(-2), new Integer(-2).toString());
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }
        assertEquals(11, result.size());
        treeMapInt.remove(new Integer(50));
        assertEquals(100, treeMapInt.size());
        assertEquals(10, result.size());
        result.remove(new Integer(60));
        assertEquals(99, treeMapInt.size());
        assertEquals(9, result.size());
        SortedMap<Integer, String> result2 = null;
        try {
            result2 = result.subMap(new Integer(-2), new Integer(100));
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }
        result2 = result.subMap(new Integer(50), new Integer(60));
        assertEquals(9, result2.size());

        // sub map of sub map
        NavigableMap<Integer, Object> mapIntObj = newBTreeMap();
        for (int i = 0; i < 10; ++i) {
            mapIntObj.put(i, new Object());
        }
        mapIntObj = mapIntObj.subMap(5, false, 9, true);
        assertEquals(4, mapIntObj.size());
        mapIntObj = mapIntObj.subMap(5, false, 9, true);
        assertEquals(4, mapIntObj.size());
        mapIntObj = mapIntObj.subMap(5, false, 6, false);
        assertEquals(0, mapIntObj.size());
        
        // a special comparator dealing with null key
        tm = newBTreeMap(new SpecialNullableComparator());
        tm.put(new String("1st"), 1);
        tm.put(new String("2nd"), 2);
        tm.put(new String("3rd"), 3);
        String nullKey = "0";
        tm.put(nullKey, -1);
        SortedMap s = tm.subMap(nullKey, "3rd");
        assertEquals(3, s.size());
        assertTrue(s.containsValue(-1));
        assertTrue(s.containsValue(1));
        assertTrue(s.containsValue(2));
        assertTrue(s.containsKey(nullKey));
        assertFalse(s.containsKey("3nd"));
        // RI fails here
        // assertTrue(s.containsKey("1st"));
        // assertTrue(s.containsKey("2nd"));
//        s = tm.descendingMap();
//        s = s.subMap("3rd", null);
//        // assertEquals(4, s.size());
////        assertTrue(s.containsValue(-1));
////        assertTrue(s.containsValue(1));
////        assertTrue(s.containsValue(2));
////        assertTrue(s.containsValue(3));
//        assertFalse(s.containsKey(null));
//        assertTrue(s.containsKey("1st"));
//        assertTrue(s.containsKey("2nd"));
//        assertTrue(s.containsKey("3rd"));
    }

    // a special comparator dealing with null key
    static public class SpecialNullableComparator implements Comparator,Serializable {
        public int compare(Object o1, Object o2) {
            if (o1 == null) {
                return -1;
            }
            return ((String) o1).compareTo((String) o2);
        }
    }


    

    /**
     * @tests java.util.TreeMap#headMap(java.lang.Object,boolea)
     */
    public void test_headMapLjava_lang_ObjectZL() {
        // normal case
        SortedMap subMap = tm.headMap(objArray[100].toString(), true);
        assertEquals("subMap is of incorrect size", 4, subMap.size());
        subMap = tm.headMap(objArray[109].toString(), true);
        assertEquals("subMap is of incorrect size", 13, subMap.size());
        for (int counter = 100; counter < 109; counter++) {
            assertTrue("SubMap contains incorrect elements", subMap.get(
                    objArray[counter].toString()).equals(objArray[counter]));
        }
        subMap = tm.headMap(objArray[100].toString(), false);
        assertEquals("subMap is of incorrect size", 3, subMap.size());
        assertNull(subMap.get(objArray[100].toString()));

        // Exceptions
        assertEquals(0, tm.headMap("", true).size());
        assertEquals(0, tm.headMap("", false).size());

        try {
            tm.headMap(null, true);
            fail("should throw NullPointerException");
        } catch (NullPointerException e) {
            // expected
        }
        try {
            tm.headMap(null, false);
            fail("should throw NullPointerException");
        } catch (NullPointerException e) {
            // expected
        }
//        try {
//            tm.headMap(new Object(), true);
//            fail("should throw ClassCastException");
//        } catch (ClassCastException e) {
//            // expected
//        }
//        try {
//            tm.headMap(new Object(), false);
//            fail("should throw ClassCastException");
//        } catch (ClassCastException e) {
//            // expected
//        }

        // use integer elements to test
        BTreeMap<Integer, String> treeMapInt = newBTreeMap();
        assertEquals(0, treeMapInt.headMap(new Integer(-1), true).size());
        for (int i = 0; i < 100; i++) {
            treeMapInt.put(new Integer(i), new Integer(i).toString());
        }
        SortedMap<Integer, String> result = treeMapInt
                .headMap(new Integer(101));
        assertEquals(100, result.size());
        try {
            result.put(new Integer(101), new Integer(101).toString());
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }
        assertEquals(100, result.size());
        assertEquals(100, treeMapInt.size());
        result = treeMapInt.headMap(new Integer(50), true);
        assertEquals(51, result.size());
        result.put(new Integer(-1), new Integer(-1).toString());
        assertEquals(52, result.size());

        treeMapInt.remove(new Integer(40));
        assertEquals(100, treeMapInt.size());
        assertEquals(51, result.size());
        result.remove(new Integer(30));
        assertEquals(99, treeMapInt.size());
        assertEquals(50, result.size());
        SortedMap<Integer, String> result2 = null;
        try {
            result.subMap(new Integer(-2), new Integer(100));
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            result.subMap(new Integer(1), new Integer(100));
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }
        result2 = result.subMap(new Integer(-2), new Integer(48));
        assertEquals(47,result2.size());
        
        result2 = result.subMap(new Integer(40), new Integer(50));
        assertEquals(9, result2.size());


        // head map of head map
        NavigableMap<Integer, Object> mapIntObj = newBTreeMap();
        for (int i = 0; i < 10; ++i) {
            mapIntObj.put(i, new Object());
        }
        mapIntObj = mapIntObj.headMap(5, false);
        assertEquals(5, mapIntObj.size());
        mapIntObj = mapIntObj.headMap(5, false);
        assertEquals(5, mapIntObj.size());
        mapIntObj = mapIntObj.tailMap(5, false);
        assertEquals(0, mapIntObj.size());
    }

    /**
     * @tests java.util.TreeMap#tailMap(java.lang.Object,boolea)
     */
    public void test_tailMapLjava_lang_ObjectZL() {
        // normal case
        SortedMap subMap = tm.tailMap(objArray[100].toString(), true);
        assertEquals("subMap is of incorrect size", 997, subMap.size());
        subMap = tm.tailMap(objArray[109].toString(), true);
        assertEquals("subMap is of incorrect size", 988, subMap.size());
        for (int counter = 119; counter > 110; counter--) {
            assertTrue("SubMap contains incorrect elements", subMap.get(
                    objArray[counter].toString()).equals(objArray[counter]));
        }
        subMap = tm.tailMap(objArray[100].toString(), false);
        assertEquals("subMap is of incorrect size", 996, subMap.size());
        assertNull(subMap.get(objArray[100].toString()));

        // Exceptions
        assertEquals(1000, tm.tailMap("", true).size());
        assertEquals(1000, tm.tailMap("", false).size());

        try {
            tm.tailMap(null, true);
            fail("should throw NullPointerException");
        } catch (NullPointerException e) {
            // expected
        }
        try {
            tm.tailMap(null, false);
            fail("should throw NullPointerException");
        } catch (NullPointerException e) {
            // expected
        }
//        try {
//            tm.tailMap(new Object(), true);
//            fail("should throw ClassCastException");
//        } catch (ClassCastException e) {
//            // expected
//        }
//        try {
//            tm.tailMap(new Object(), false);
//            fail("should throw ClassCastException");
//        } catch (ClassCastException e) {
//            // expected
//        }

        // use integer elements to test
        BTreeMap<Integer, String> treeMapInt = newBTreeMap();
        assertEquals(0, treeMapInt.tailMap(new Integer(-1), true).size());
        for (int i = 0; i < 100; i++) {
            treeMapInt.put(new Integer(i), new Integer(i).toString());
        }
        SortedMap<Integer, String> result = treeMapInt.tailMap(new Integer(1));
        assertEquals(99, result.size());
        try {
            result.put(new Integer(-1), new Integer(-1).toString());
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }
        assertEquals(99, result.size());
        assertEquals(100, treeMapInt.size());
        result = treeMapInt.tailMap(new Integer(50), true);
        assertEquals(50, result.size());
        result.put(new Integer(101), new Integer(101).toString());
        assertEquals(51, result.size());

        treeMapInt.remove(new Integer(60));
        assertEquals(100, treeMapInt.size());
        assertEquals(50, result.size());
        result.remove(new Integer(70));
        assertEquals(99, treeMapInt.size());
        assertEquals(49, result.size());
        SortedMap<Integer, String> result2 = null;
        try {
            result2 = result.subMap(new Integer(-2), new Integer(100));
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }
        result2 = result.subMap(new Integer(60), new Integer(70));
        assertEquals(9, result2.size());


        // tail map of tail map
        NavigableMap<Integer, Object> mapIntObj = newBTreeMap();
        for (int i = 0; i < 10; ++i) {
            mapIntObj.put(i, new Object());
        }
        mapIntObj = mapIntObj.tailMap(5, false);
        assertEquals(4, mapIntObj.size());
        mapIntObj = mapIntObj.tailMap(5, false);
        assertEquals(4, mapIntObj.size());
        mapIntObj = mapIntObj.headMap(5, false);
        assertEquals(0, mapIntObj.size());
    }

//
//    public void test_descendingMap_subMap() throws Exception {
//        BTreeMap<Integer, Object> tm = newBTreeMap();
//        for (int i = 0; i < 10; ++i) {
//            tm.put(i, new Object());
//        }
//        NavigableMap<Integer, Object> descMap = tm.descendingMap();
//        assertEquals(7, descMap.subMap(8, true, 1, false).size());
//        assertEquals(4, descMap.headMap(6, true).size());
//        assertEquals(2, descMap.tailMap(2, false).size());
//
//        // sub map of sub map of descendingMap
//        NavigableMap<Integer, Object> mapIntObj = newBTreeMap();
//        for (int i = 0; i < 10; ++i) {
//            mapIntObj.put(i, new Object());
//        }
//        mapIntObj = mapIntObj.descendingMap();
//        NavigableMap<Integer, Object> subMapIntObj = mapIntObj.subMap(9, true,
//                5, false);
//        assertEquals(4, subMapIntObj.size());
//        subMapIntObj = subMapIntObj.subMap(9, true, 5, false);
//        assertEquals(4, subMapIntObj.size());
//        subMapIntObj = subMapIntObj.subMap(6, false, 5, false);
//        assertEquals(0, subMapIntObj.size());
//
//        subMapIntObj = mapIntObj.headMap(5, false);
//        assertEquals(4, subMapIntObj.size());
//        subMapIntObj = subMapIntObj.headMap(5, false);
//        assertEquals(4, subMapIntObj.size());
//        subMapIntObj = subMapIntObj.tailMap(5, false);
//        assertEquals(0, subMapIntObj.size());
//
//        subMapIntObj = mapIntObj.tailMap(5, false);
//        assertEquals(5, subMapIntObj.size());
//        subMapIntObj = subMapIntObj.tailMap(5, false);
//        assertEquals(5, subMapIntObj.size());
//        subMapIntObj = subMapIntObj.headMap(5, false);
//        assertEquals(0, subMapIntObj.size());
//    }
//

    private void illegalFirstNullKeyMapTester(NavigableMap<String, String> map) {
        try {
            map.get(null);
            fail("Should throw NullPointerException");
        } catch (NullPointerException e) {
            // expected
        }
        try {
            map.put("NormalKey", "value");
            fail("Should throw NullPointerException");
        } catch (NullPointerException e) {
            // expected
        }
        Set<String> keySet = map.keySet();
        assertTrue(!keySet.isEmpty());
        assertEquals(1, keySet.size());
        for (String key : keySet) {
            assertEquals(key, null);
            try {
                map.get(key);
                fail("Should throw NullPointerException");
            } catch (NullPointerException e) {
                // ignore
            }
        }
        Set<Entry<String, String>> entrySet = map.entrySet();
        assertTrue(!entrySet.isEmpty());
        assertEquals(1, entrySet.size());
        for (Entry<String, String> entry : entrySet) {
            assertEquals(null, entry.getKey());
            assertEquals("NullValue", entry.getValue());
        }
        Collection<String> values = map.values();
        assertTrue(!values.isEmpty());
        assertEquals(1, values.size());
        for (String value : values) {
            assertEquals("NullValue", value);
        }

        try {
            map.headMap(null, true);
            fail("Should throw NullPointerException");
        } catch (NullPointerException e) {
            // ignore
        }
        try {
            map.headMap(null, false);
            fail("Should throw NullPointerException");
        } catch (NullPointerException e) {
            // ignore
        }

        try {
            map.subMap(null, false, null, false);
            fail("Should throw NullPointerException");
        } catch (NullPointerException e) {
            // ignore
        }
        try {
            map.subMap(null, true, null, true);
            fail("Should throw NullPointerException");
        } catch (NullPointerException e) {
            // ignore
        }
        try {
            map.tailMap(null, true);
            fail("Should throw NullPointerException");
        } catch (NullPointerException e) {
            // ignore
        }
        try {
            map.tailMap(null, false);
            fail("Should throw NullPointerException");
        } catch (NullPointerException e) {
            // ignore
        }
    }

    /**
     * Tests equals() method.
     * Tests that no ClassCastException will be thrown in all cases.
     * Regression test for HARMONY-1639.
     */
    public void test_equals() throws Exception {
        // comparing TreeMaps with different object types
        Map m1 = newBTreeMap();
        Map m2 = newBTreeMap();
        m1.put("key1", "val1");
        m1.put("key2", "val2");
        m2.put(new Integer(1), "val1");
        m2.put(new Integer(2), "val2");
        assertFalse("Maps should not be equal 1", m1.equals(m2));
        assertFalse("Maps should not be equal 2", m2.equals(m1));

        // comparing TreeMap with HashMap
        m1 = newBTreeMap();
        m2 = new HashMap();
        m1.put("key", "val");
        m2.put(new Object(), "val");
        assertFalse("Maps should not be equal 3", m1.equals(m2));
        assertFalse("Maps should not be equal 4", m2.equals(m1));

        // comparing TreeMaps with not-comparable objects inside
        m1 = newBTreeMap();
        m2 = newBTreeMap();
        m1.put(new Object(), "val1");
        m2.put(new Object(), "val1");
        assertFalse("Maps should not be equal 5", m1.equals(m2));
        assertFalse("Maps should not be equal 6", m2.equals(m1));
    }

    public void test_remove_from_iterator() throws Exception {
        Set set = tm.keySet();
        Iterator iter = set.iterator();
        iter.next();
        iter.remove();
        try{
            iter.remove();
            fail("should throw IllegalStateException");
        }catch (IllegalStateException e){
            // expected
        }
    }
    

    public void test_iterator_next_(){
        Map m = tm.subMap("0", "1");
        Iterator it = m.entrySet().iterator();
        assertEquals("0=0",it.next().toString());
        while(it.hasNext()){}
        try {
          it.next();
          fail("should throw java.util.NoSuchElementException");
        }catch (Exception e){
          assertTrue(e instanceof java.util.NoSuchElementException);
        }
     }
    
    public void test_empty_subMap() throws Exception {
        BTreeMap<Float, List<Integer>> tm = newBTreeMap();
        SortedMap<Float, List<Integer>> sm = tm.tailMap(1.1f);
        assertTrue(sm.values().size() == 0);
    }
    
        public static BTreeMap treeMap = newBTreeMap();

        public void test_values_1(){
            treeMap.put("firstKey", "firstValue");
            treeMap.put("secondKey", "secondValue");
            treeMap.put("thirdKey", "thirdValue");
            Object firstKey = treeMap.firstKey();
            SortedMap subMap = ((SortedMap)treeMap).subMap(firstKey, firstKey);
            Iterator iter = subMap.values().iterator();
        }    
    
    /**
     * Sets up the fixture, for example, open a network connection. This method
     * is called before a test is executed.
     */
    @Override
    protected void setUp() {
        tm = newBTreeMap();
        for (int i = 0; i < objArray.length; i++) {
            Object x = objArray[i] = new Integer(i);
            tm.put(x.toString(), x);
        }
    }
}

