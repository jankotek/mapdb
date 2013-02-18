package org.mapdb;

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

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.Map.Entry;

import junit.framework.TestCase;


// 
public class TreeMapExtendTest extends TestCase {


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

    TreeMap tm;

    TreeMap tm_comparator;

    SortedMap subMap_default;

    SortedMap subMap_startExcluded_endExcluded;

    SortedMap subMap_startExcluded_endIncluded;

    SortedMap subMap_startIncluded_endExcluded;

    SortedMap subMap_startIncluded_endIncluded;

    SortedMap subMap_default_beforeStart_100;

    SortedMap subMap_default_afterEnd_109;

    NavigableMap navigableMap_startExcluded_endExcluded;

    NavigableMap navigableMap_startExcluded_endIncluded;

    NavigableMap navigableMap_startIncluded_endExcluded;

    NavigableMap navigableMap_startIncluded_endIncluded;

    SortedMap subMap_default_comparator;

    SortedMap subMap_startExcluded_endExcluded_comparator;

    SortedMap subMap_startExcluded_endIncluded_comparator;

    SortedMap subMap_startIncluded_endExcluded_comparator;

    SortedMap subMap_startIncluded_endIncluded_comparator;

    Object objArray[] = new Object[1000];

    public void test_TreeMap_Constructor_Default() {
        TreeMap treeMap = new TreeMap();
        assertTrue(treeMap.isEmpty());
        assertNull(treeMap.comparator());
        assertEquals(0, treeMap.size());

        try {
            treeMap.firstKey();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }
        assertNull(treeMap.firstEntry());

        try {
            treeMap.lastKey();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }
        assertNull(treeMap.lastEntry());

        try {
            treeMap.ceilingKey(1);
        } catch (NoSuchElementException e) {
            // Expected
        }
        assertNull(treeMap.ceilingEntry(1));

        try {
            treeMap.floorKey(1);
        } catch (NoSuchElementException e) {
            // Expected
        }
        assertNull(treeMap.floorEntry(1));
        assertNull(treeMap.lowerKey(1));
        assertNull(treeMap.lowerEntry(1));
        assertNull(treeMap.higherKey(1));
        assertNull(treeMap.higherEntry(1));
        assertFalse(treeMap.containsKey(1));
        assertFalse(treeMap.containsValue(1));
        assertNull(treeMap.get(1));

        assertNull(treeMap.pollFirstEntry());
        assertNull(treeMap.pollLastEntry());
        assertEquals(0, treeMap.values().size());
    }

    public void test_TreeMap_Constructor_Comparator() {
        MockComparator mockComparator = new MockComparator();
        TreeMap treeMap = new TreeMap(mockComparator);

        assertEquals(mockComparator, treeMap.comparator());
    }

    public void test_TreeMap_Constructor_Map() {
        TreeMap treeMap = new TreeMap(tm);
        assertEquals(tm.size(), treeMap.size());
        assertEquals(tm.firstKey(), treeMap.firstKey());
        assertEquals(tm.firstEntry(), treeMap.firstEntry());
        assertEquals(tm.lastKey(), treeMap.lastKey());
        assertEquals(tm.lastEntry(), treeMap.lastEntry());
        assertEquals(tm.keySet(), treeMap.keySet());

        String key = new Integer(100).toString();
        assertEquals(tm.ceilingKey(key), treeMap.ceilingKey(key));
        assertEquals(tm.ceilingEntry(key), treeMap.ceilingEntry(key));
        assertEquals(tm.floorKey(key), treeMap.floorKey(key));
        assertEquals(tm.floorEntry(key), treeMap.floorEntry(key));
        assertEquals(tm.lowerKey(key), treeMap.lowerKey(key));
        assertEquals(tm.lowerEntry(key), treeMap.lowerEntry(key));
        assertEquals(tm.higherKey(key), treeMap.higherKey(key));
        assertEquals(tm.higherEntry(key), treeMap.higherEntry(key));
        assertEquals(tm.entrySet(), treeMap.entrySet());
    }

    public void test_TreeMap_Constructor_SortedMap() {
        TreeMap treeMap = new TreeMap(subMap_default);
        assertEquals(subMap_default.size(), treeMap.size());
        assertEquals(subMap_default.firstKey(), treeMap.firstKey());
        assertEquals(subMap_default.lastKey(), treeMap.lastKey());
        assertEquals(subMap_default.keySet(), treeMap.keySet());
        assertEquals(subMap_default.entrySet(), treeMap.entrySet());
    }

    public void test_TreeMap_clear() {
        tm.clear();
        assertEquals(0, tm.size());
    }

    public void test_TreeMap_clone() {
        TreeMap cloneTreeMap = (TreeMap) tm.clone();
        assertEquals(tm, cloneTreeMap);
    }

    public void test_SubMap_Constructor() {
    }

    public void test_SubMap_clear() {
        subMap_default.clear();
        assertEquals(0, subMap_default.size());
    }

    public void test_SubMap_comparator() {
        assertEquals(tm.comparator(), subMap_default.comparator());
    }

    public void test_SubMap_containsKey() {
        String key = null;
        for (int counter = 101; counter < 109; counter++) {
            key = objArray[counter].toString();
            assertTrue("SubMap contains incorrect elements", subMap_default
                    .containsKey(key));
            assertTrue("SubMap contains incorrect elements",
                    subMap_startExcluded_endExcluded.containsKey(key));
            assertTrue("SubMap contains incorrect elements",
                    subMap_startExcluded_endIncluded.containsKey(key));
            assertTrue("SubMap contains incorrect elements",
                    subMap_startIncluded_endExcluded.containsKey(key));
            assertTrue("SubMap contains incorrect elements",
                    subMap_startIncluded_endIncluded.containsKey(key));
        }

        // Check boundary
        key = objArray[100].toString();
        assertTrue("SubMap contains incorrect elements", subMap_default
                .containsKey(key));
        assertFalse("SubMap contains incorrect elements",
                subMap_startExcluded_endExcluded.containsKey(key));
        assertFalse("SubMap contains incorrect elements",
                subMap_startExcluded_endIncluded.containsKey(key));
        assertTrue("SubMap contains incorrect elements",
                subMap_startIncluded_endExcluded.containsKey(key));
        assertTrue("SubMap contains incorrect elements",
                subMap_startIncluded_endIncluded.containsKey(key));

        key = objArray[109].toString();
        assertFalse("SubMap contains incorrect elements", subMap_default
                .containsKey(key));
        assertFalse("SubMap contains incorrect elements",
                subMap_startExcluded_endExcluded.containsKey(key));
        assertTrue("SubMap contains incorrect elements",
                subMap_startExcluded_endIncluded.containsKey(key));
        assertFalse("SubMap contains incorrect elements",
                subMap_startIncluded_endExcluded.containsKey(key));
        assertTrue("SubMap contains incorrect elements",
                subMap_startIncluded_endIncluded.containsKey(key));

        // With Comparator
        for (int counter = 101; counter < 109; counter++) {
            key = objArray[counter].toString();
            assertTrue("SubMap contains incorrect elements",
                    subMap_default_comparator.containsKey(key));
            assertTrue("SubMap contains incorrect elements",
                    subMap_startExcluded_endExcluded_comparator
                            .containsKey(key));
            assertTrue("SubMap contains incorrect elements",
                    subMap_startExcluded_endIncluded_comparator
                            .containsKey(key));
            assertTrue("SubMap contains incorrect elements",
                    subMap_startIncluded_endExcluded_comparator
                            .containsKey(key));
            assertTrue("SubMap contains incorrect elements",
                    subMap_startIncluded_endIncluded_comparator
                            .containsKey(key));
        }

        // Check boundary
        key = objArray[100].toString();
        assertTrue("SubMap contains incorrect elements",
                subMap_default_comparator.containsKey(key));
        assertFalse("SubMap contains incorrect elements",
                subMap_startExcluded_endExcluded_comparator.containsKey(key));
        assertFalse("SubMap contains incorrect elements",
                subMap_startExcluded_endIncluded_comparator.containsKey(key));
        assertTrue("SubMap contains incorrect elements",
                subMap_startIncluded_endExcluded_comparator.containsKey(key));
        assertTrue("SubMap contains incorrect elements",
                subMap_startIncluded_endIncluded_comparator.containsKey(key));

        key = objArray[109].toString();
        assertFalse("SubMap contains incorrect elements",
                subMap_default_comparator.containsKey(key));
        assertFalse("SubMap contains incorrect elements",
                subMap_startExcluded_endExcluded_comparator.containsKey(key));
        assertTrue("SubMap contains incorrect elements",
                subMap_startExcluded_endIncluded_comparator.containsKey(key));
        assertFalse("SubMap contains incorrect elements",
                subMap_startIncluded_endExcluded_comparator.containsKey(key));
        assertTrue("SubMap contains incorrect elements",
                subMap_startIncluded_endIncluded_comparator.containsKey(key));
    }

    public void test_SubMap_containsValue() {
        Object value = null;
        for (int counter = 101; counter < 109; counter++) {
            value = objArray[counter];
            assertTrue("SubMap contains incorrect elements", subMap_default
                    .containsValue(value));
            assertTrue("SubMap contains incorrect elements",
                    subMap_startExcluded_endExcluded.containsValue(value));
            assertTrue("SubMap contains incorrect elements",
                    subMap_startExcluded_endIncluded.containsValue(value));
            assertTrue("SubMap contains incorrect elements",
                    subMap_startIncluded_endExcluded.containsValue(value));
            assertTrue("SubMap contains incorrect elements",
                    subMap_startIncluded_endIncluded.containsValue(value));
        }

        // Check boundary
        value = objArray[100];
        assertTrue("SubMap contains incorrect elements", subMap_default
                .containsValue(value));
        assertFalse("SubMap contains incorrect elements",
                subMap_startExcluded_endExcluded.containsValue(value));
        assertFalse("SubMap contains incorrect elements",
                subMap_startExcluded_endIncluded.containsValue(value));
        assertTrue("SubMap contains incorrect elements",
                subMap_startIncluded_endExcluded.containsValue(value));
        assertTrue("SubMap contains incorrect elements",
                subMap_startIncluded_endIncluded.containsValue(value));

        value = objArray[109];
        assertFalse("SubMap contains incorrect elements", subMap_default
                .containsValue(value));
        assertFalse("SubMap contains incorrect elements",
                subMap_startExcluded_endExcluded.containsValue(value));
        assertTrue("SubMap contains incorrect elements",
                subMap_startExcluded_endIncluded.containsValue(value));
        assertFalse("SubMap contains incorrect elements",
                subMap_startIncluded_endExcluded.containsValue(value));
        assertTrue("SubMap contains incorrect elements",
                subMap_startIncluded_endIncluded.containsValue(value));

        assertFalse(subMap_default.containsValue(null));

        TreeMap tm_null = new TreeMap();
        tm_null.put("0", 1);
        tm_null.put("1", null);
        tm_null.put("2", 2);
        SortedMap subMap = tm_null.subMap("0", "2");
        assertTrue(subMap.containsValue(null));

        subMap.remove("1");
        assertFalse(subMap.containsValue(null));
    }

    public void test_SubMap_entrySet() {
        Set entrySet = subMap_default.entrySet();
        assertFalse(entrySet.isEmpty());
        assertEquals(9, entrySet.size());

        entrySet = subMap_startExcluded_endExcluded.entrySet();
        assertFalse(entrySet.isEmpty());
        assertEquals(8, entrySet.size());

        entrySet = subMap_startExcluded_endIncluded.entrySet();
        assertFalse(entrySet.isEmpty());
        assertEquals(9, entrySet.size());

        entrySet = subMap_startIncluded_endExcluded.entrySet();
        assertFalse(entrySet.isEmpty());
        assertEquals(9, entrySet.size());

        entrySet = subMap_startIncluded_endIncluded.entrySet();
        assertFalse(entrySet.isEmpty());
        assertEquals(10, entrySet.size());
    }

    public void test_SubMap_firstKey() {
        String firstKey1 = new Integer(100).toString();
        String firstKey2 = new Integer(101).toString();
        assertEquals(firstKey1, subMap_default.firstKey());
        assertEquals(firstKey2, subMap_startExcluded_endExcluded.firstKey());
        assertEquals(firstKey2, subMap_startExcluded_endIncluded.firstKey());
        assertEquals(firstKey1, subMap_startIncluded_endExcluded.firstKey());
        assertEquals(firstKey1, subMap_startIncluded_endIncluded.firstKey());

        try {
            subMap_default.subMap(firstKey1, firstKey1).firstKey();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        try {
            subMap_startExcluded_endExcluded.subMap(firstKey2, firstKey2)
                    .firstKey();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        try {
            subMap_startExcluded_endIncluded.subMap(firstKey2, firstKey2)
                    .firstKey();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        try {
            subMap_startIncluded_endExcluded.subMap(firstKey1, firstKey1)
                    .firstKey();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        try {
            subMap_startIncluded_endIncluded.subMap(firstKey1, firstKey1)
                    .firstKey();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        // With Comparator
        assertEquals(firstKey1, subMap_default_comparator.firstKey());
        assertEquals(firstKey2, subMap_startExcluded_endExcluded_comparator
                .firstKey());
        assertEquals(firstKey2, subMap_startExcluded_endIncluded_comparator
                .firstKey());
        assertEquals(firstKey1, subMap_startIncluded_endExcluded_comparator
                .firstKey());
        assertEquals(firstKey1, subMap_startIncluded_endIncluded_comparator
                .firstKey());

        try {
            subMap_default_comparator.subMap(firstKey1, firstKey1).firstKey();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        try {
            subMap_startExcluded_endExcluded_comparator.subMap(firstKey2,
                    firstKey2).firstKey();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        try {
            subMap_startExcluded_endIncluded_comparator.subMap(firstKey2,
                    firstKey2).firstKey();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        try {
            subMap_startIncluded_endExcluded_comparator.subMap(firstKey1,
                    firstKey1).firstKey();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        try {
            subMap_startIncluded_endIncluded_comparator.subMap(firstKey1,
                    firstKey1).firstKey();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

    }

    public void test_SubMap_lastKey() {
        String lastKey1 = new Integer(108).toString();
        String lastKey2 = new Integer(109).toString();
        assertEquals(lastKey1, subMap_default.lastKey());
        assertEquals(lastKey1, subMap_startExcluded_endExcluded.lastKey());
        assertEquals(lastKey2, subMap_startExcluded_endIncluded.lastKey());
        assertEquals(lastKey1, subMap_startIncluded_endExcluded.lastKey());
        assertEquals(lastKey2, subMap_startIncluded_endIncluded.lastKey());

        try {
            subMap_default.subMap(lastKey1, lastKey1).lastKey();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        try {
            subMap_startExcluded_endExcluded.subMap(lastKey1, lastKey1)
                    .lastKey();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        try {
            subMap_startExcluded_endIncluded.subMap(lastKey2, lastKey2)
                    .lastKey();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        try {
            subMap_startIncluded_endExcluded.subMap(lastKey1, lastKey1)
                    .lastKey();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        try {
            subMap_startIncluded_endIncluded.subMap(lastKey2, lastKey2)
                    .lastKey();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        // With Comparator
        assertEquals(lastKey1, subMap_default_comparator.lastKey());
        assertEquals(lastKey1, subMap_startExcluded_endExcluded_comparator
                .lastKey());
        assertEquals(lastKey2, subMap_startExcluded_endIncluded_comparator
                .lastKey());
        assertEquals(lastKey1, subMap_startIncluded_endExcluded_comparator
                .lastKey());
        assertEquals(lastKey2, subMap_startIncluded_endIncluded_comparator
                .lastKey());

        try {
            subMap_default_comparator.subMap(lastKey1, lastKey1).lastKey();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        try {
            subMap_startExcluded_endExcluded_comparator.subMap(lastKey1,
                    lastKey1).lastKey();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        try {
            subMap_startExcluded_endIncluded_comparator.subMap(lastKey2,
                    lastKey2).lastKey();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        try {
            subMap_startIncluded_endExcluded_comparator.subMap(lastKey1,
                    lastKey1).lastKey();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        try {
            subMap_startIncluded_endIncluded_comparator.subMap(lastKey2,
                    lastKey2).lastKey();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }
    }

    public void test_SubMap_get() {
        // left boundary
        Integer value = new Integer(100);
        assertEquals(value, subMap_default.get(value.toString()));
        assertEquals(null, subMap_startExcluded_endExcluded.get(value
                .toString()));
        assertEquals(null, subMap_startExcluded_endIncluded.get(value
                .toString()));
        assertEquals(value, subMap_startIncluded_endExcluded.get(value
                .toString()));
        assertEquals(value, subMap_startIncluded_endIncluded.get(value
                .toString()));

        // normal value
        value = new Integer(105);
        assertEquals(value, subMap_default.get(value.toString()));
        assertEquals(value, subMap_startExcluded_endExcluded.get(value
                .toString()));
        assertEquals(value, subMap_startExcluded_endIncluded.get(value
                .toString()));
        assertEquals(value, subMap_startIncluded_endExcluded.get(value
                .toString()));
        assertEquals(value, subMap_startIncluded_endIncluded.get(value
                .toString()));

        // right boundary
        value = new Integer(109);
        assertEquals(null, subMap_default.get(value.toString()));
        assertEquals(null, subMap_startExcluded_endExcluded.get(value
                .toString()));
        assertEquals(value, subMap_startExcluded_endIncluded.get(value
                .toString()));
        assertEquals(null, subMap_startIncluded_endExcluded.get(value
                .toString()));
        assertEquals(value, subMap_startIncluded_endIncluded.get(value
                .toString()));

        // With Comparator to test inInRange
        // left boundary
        value = new Integer(100);
        assertEquals(value, subMap_default_comparator.get(value.toString()));

        // normal value
        value = new Integer(105);
        assertEquals(value, subMap_default_comparator.get(value.toString()));

        // right boundary
        value = new Integer(109);
        assertEquals(null, subMap_default_comparator.get(value.toString()));
    }

    public void test_SubMap_headMap() {
        String endKey = new Integer(99).toString();
        try {
            subMap_default.headMap(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startExcluded_endExcluded.headMap(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startExcluded_endIncluded.headMap(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startIncluded_endExcluded.headMap(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startIncluded_endIncluded.headMap(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        SortedMap headMap = null;
        endKey = new Integer(100).toString();
        headMap = subMap_default.headMap(endKey);
        assertEquals(0, headMap.size());

        headMap = subMap_startExcluded_endExcluded.headMap(endKey);
        assertEquals(0, headMap.size());

        headMap = subMap_startExcluded_endIncluded.headMap(endKey);
        assertEquals(0, headMap.size());

        headMap = subMap_startIncluded_endExcluded.headMap(endKey);
        assertEquals(0, headMap.size());

        headMap = subMap_startIncluded_endIncluded.headMap(endKey);
        assertEquals(0, headMap.size());

        for (int i = 0, j = 101; i < 8; i++) {
            endKey = new Integer(i + j).toString();
            headMap = subMap_default.headMap(endKey);
            assertEquals(i + 1, headMap.size());

            headMap = subMap_startExcluded_endExcluded.headMap(endKey);
            assertEquals(i, headMap.size());

            headMap = subMap_startExcluded_endIncluded.headMap(endKey);
            assertEquals(i, headMap.size());

            headMap = subMap_startIncluded_endExcluded.headMap(endKey);
            assertEquals(i + 1, headMap.size());

            headMap = subMap_startIncluded_endIncluded.headMap(endKey);
            assertEquals(i + 1, headMap.size());
        }

        endKey = new Integer(109).toString();
        headMap = subMap_default.headMap(endKey);
        assertEquals(9, headMap.size());

        headMap = subMap_startExcluded_endExcluded.headMap(endKey);
        assertEquals(8, headMap.size());

        headMap = subMap_startExcluded_endIncluded.headMap(endKey);
        assertEquals(8, headMap.size());

        headMap = subMap_startIncluded_endExcluded.headMap(endKey);
        assertEquals(9, headMap.size());

        headMap = subMap_startIncluded_endIncluded.headMap(endKey);
        assertEquals(9, headMap.size());

        endKey = new Integer(110).toString();
        try {
            subMap_default.headMap(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startExcluded_endExcluded.headMap(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startExcluded_endIncluded.headMap(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startIncluded_endExcluded.headMap(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startIncluded_endIncluded.headMap(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        // With Comparator
        endKey = new Integer(99).toString();
        try {
            subMap_default_comparator.headMap(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startExcluded_endExcluded_comparator.headMap(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startExcluded_endIncluded_comparator.headMap(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startIncluded_endExcluded_comparator.headMap(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startIncluded_endIncluded_comparator.headMap(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        headMap = null;
        endKey = new Integer(100).toString();
        headMap = subMap_default_comparator.headMap(endKey);
        assertEquals(0, headMap.size());

        headMap = subMap_startExcluded_endExcluded_comparator.headMap(endKey);
        assertEquals(0, headMap.size());

        headMap = subMap_startExcluded_endIncluded_comparator.headMap(endKey);
        assertEquals(0, headMap.size());

        headMap = subMap_startIncluded_endExcluded_comparator.headMap(endKey);
        assertEquals(0, headMap.size());

        headMap = subMap_startIncluded_endIncluded_comparator.headMap(endKey);
        assertEquals(0, headMap.size());

        for (int i = 0, j = 101; i < 8; i++) {
            endKey = new Integer(i + j).toString();
            headMap = subMap_default_comparator.headMap(endKey);
            assertEquals(i + 1, headMap.size());

            headMap = subMap_startExcluded_endExcluded_comparator
                    .headMap(endKey);
            assertEquals(i, headMap.size());

            headMap = subMap_startExcluded_endIncluded_comparator
                    .headMap(endKey);
            assertEquals(i, headMap.size());

            headMap = subMap_startIncluded_endExcluded_comparator
                    .headMap(endKey);
            assertEquals(i + 1, headMap.size());

            headMap = subMap_startIncluded_endIncluded_comparator
                    .headMap(endKey);
            assertEquals(i + 1, headMap.size());
        }

        endKey = new Integer(108).toString();
        headMap = subMap_default_comparator.headMap(endKey);
        assertEquals(8, headMap.size());

        headMap = subMap_startExcluded_endExcluded_comparator.headMap(endKey);
        assertEquals(7, headMap.size());

        headMap = subMap_startExcluded_endIncluded_comparator.headMap(endKey);
        assertEquals(7, headMap.size());

        headMap = subMap_startIncluded_endExcluded_comparator.headMap(endKey);
        assertEquals(8, headMap.size());

        headMap = subMap_startIncluded_endIncluded_comparator.headMap(endKey);
        assertEquals(8, headMap.size());

        endKey = new Integer(110).toString();
        try {
            subMap_default_comparator.headMap(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startExcluded_endExcluded_comparator.headMap(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startExcluded_endIncluded_comparator.headMap(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startIncluded_endExcluded_comparator.headMap(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startIncluded_endIncluded_comparator.headMap(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
    }

    public void test_SubMap_isEmpty() {
        assertFalse(subMap_default.isEmpty());
        assertFalse(subMap_startExcluded_endExcluded.isEmpty());
        assertFalse(subMap_startExcluded_endIncluded.isEmpty());
        assertFalse(subMap_startIncluded_endExcluded.isEmpty());
        assertFalse(subMap_startIncluded_endIncluded.isEmpty());

        Object startKey = new Integer(100);
        Object endKey = startKey;
        SortedMap subMap = tm.subMap(startKey.toString(), endKey.toString());
        assertTrue(subMap.isEmpty());
        subMap = subMap_default.subMap(startKey.toString(), endKey.toString());
        assertTrue(subMap.isEmpty());
        subMap = subMap_startIncluded_endExcluded.subMap(startKey.toString(),
                endKey.toString());
        assertTrue(subMap.isEmpty());
        subMap = subMap_startIncluded_endIncluded.subMap(startKey.toString(),
                endKey.toString());
        assertTrue(subMap.isEmpty());

        for (int i = 0, j = 101; i < 8; i++) {
            startKey = i + j;
            endKey = startKey;

            subMap = subMap_default.subMap(startKey.toString(), endKey
                    .toString());
            assertTrue(subMap.isEmpty());

            subMap = subMap_startExcluded_endExcluded.subMap(startKey
                    .toString(), endKey.toString());
            assertTrue(subMap.isEmpty());

            subMap = subMap_startExcluded_endIncluded.subMap(startKey
                    .toString(), endKey.toString());
            assertTrue(subMap.isEmpty());

            subMap = subMap_startIncluded_endExcluded.subMap(startKey
                    .toString(), endKey.toString());
            assertTrue(subMap.isEmpty());

            subMap = subMap_startIncluded_endIncluded.subMap(startKey
                    .toString(), endKey.toString());
            assertTrue(subMap.isEmpty());
        }

        for (int i = 0, j = 101; i < 5; i++) {
            startKey = i + j;
            endKey = i + j + 4;

            subMap = subMap_default.subMap(startKey.toString(), endKey
                    .toString());
            assertFalse(subMap.isEmpty());

            subMap = subMap_startExcluded_endExcluded.subMap(startKey
                    .toString(), endKey.toString());
            assertFalse(subMap.isEmpty());

            subMap = subMap_startExcluded_endIncluded.subMap(startKey
                    .toString(), endKey.toString());
            assertFalse(subMap.isEmpty());

            subMap = subMap_startIncluded_endExcluded.subMap(startKey
                    .toString(), endKey.toString());
            assertFalse(subMap.isEmpty());

            subMap = subMap_startIncluded_endIncluded.subMap(startKey
                    .toString(), endKey.toString());
            assertFalse(subMap.isEmpty());
        }

        startKey = new Integer(109).toString();
        endKey = startKey;
        subMap = tm.subMap(startKey.toString(), endKey.toString());
        assertTrue(subMap.isEmpty());
        subMap = subMap_startExcluded_endIncluded.subMap(startKey, endKey);
        assertTrue(subMap.isEmpty());
        subMap = subMap_startIncluded_endIncluded.subMap(startKey, endKey);
        assertTrue(subMap.isEmpty());

    }

    public void test_SubMap_keySet() {
        Set keySet = subMap_default.keySet();
        assertFalse(keySet.isEmpty());
        assertEquals(9, keySet.size());

        keySet = subMap_startExcluded_endExcluded.entrySet();
        assertFalse(keySet.isEmpty());
        assertEquals(8, keySet.size());

        keySet = subMap_startExcluded_endIncluded.entrySet();
        assertFalse(keySet.isEmpty());
        assertEquals(9, keySet.size());

        keySet = subMap_startIncluded_endExcluded.entrySet();
        assertFalse(keySet.isEmpty());
        assertEquals(9, keySet.size());

        keySet = subMap_startIncluded_endIncluded.entrySet();
        assertFalse(keySet.isEmpty());
        assertEquals(10, keySet.size());
    }

    public void test_SubMap_put() {
        Integer value = new Integer(100);
        int addValue = 5;

        subMap_default.put(value.toString(), value + addValue);
        assertEquals(value + addValue, subMap_default.get(value.toString()));

        try {
            subMap_startExcluded_endExcluded.put(value.toString(), value
                    + addValue);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startExcluded_endIncluded.put(value.toString(), value
                    + addValue);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        subMap_startIncluded_endExcluded
                .put(value.toString(), value + addValue);
        assertEquals(value + addValue, subMap_startIncluded_endExcluded
                .get(value.toString()));

        subMap_startIncluded_endIncluded
                .put(value.toString(), value + addValue);
        assertEquals(value + addValue, subMap_startIncluded_endIncluded
                .get(value.toString()));

        value = new Integer(109);
        try {
            subMap_default.put(value.toString(), value + addValue);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startExcluded_endExcluded.put(value.toString(), value
                    + addValue);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        subMap_startExcluded_endIncluded
                .put(value.toString(), value + addValue);
        assertEquals(value + addValue, subMap_startExcluded_endIncluded
                .get(value.toString()));

        try {
            subMap_startIncluded_endExcluded.put(value.toString(), value
                    + addValue);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        subMap_startIncluded_endIncluded
                .put(value.toString(), value + addValue);
        assertEquals(value + addValue, subMap_startIncluded_endIncluded
                .get(value.toString()));
    }

    public void test_SubMap_remove() {
        Integer value = new Integer(100);

        subMap_default.remove(value.toString());
        assertNull(subMap_default.get(value.toString()));

        subMap_startExcluded_endExcluded.remove(value.toString());
        assertNull(subMap_startExcluded_endExcluded.get(value.toString()));

        subMap_startExcluded_endIncluded.remove(value.toString());
        assertNull(subMap_startExcluded_endIncluded.get(value.toString()));

        subMap_startIncluded_endExcluded.remove(value.toString());
        assertNull(subMap_startIncluded_endExcluded.get(value.toString()));

        subMap_startIncluded_endIncluded.remove(value.toString());
        assertNull(subMap_startIncluded_endIncluded.get(value.toString()));

        value = new Integer(109);
        subMap_default.remove(value.toString());
        assertNull(subMap_default.get(value.toString()));

        subMap_startExcluded_endExcluded.remove(value.toString());
        assertNull(subMap_startExcluded_endExcluded.get(value.toString()));

        subMap_startExcluded_endIncluded.remove(value.toString());
        assertNull(subMap_startExcluded_endIncluded.get(value.toString()));

        subMap_startIncluded_endExcluded.remove(value.toString());
        assertNull(subMap_startIncluded_endExcluded.get(value.toString()));

        subMap_startIncluded_endIncluded.remove(value.toString());
        assertNull(subMap_startIncluded_endIncluded.get(value.toString()));
    }

    public void test_SubMap_subMap_NoComparator() {
        String startKey = new Integer[100].toString();
        String endKey = new Integer[100].toString();
        try {
            subMap_default.subMap(startKey, endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startExcluded_endExcluded.subMap(startKey, endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startExcluded_endIncluded.subMap(startKey, endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startIncluded_endExcluded.subMap(startKey, endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startIncluded_endIncluded.subMap(startKey, endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        SortedMap subSubMap = null;
        for (int i = 101; i < 109; i++) {
            startKey = new Integer(i).toString();
            endKey = startKey;

            subSubMap = subMap_default.subMap(startKey, endKey);
            assertEquals(0, subSubMap.size());

            subSubMap = subMap_startExcluded_endExcluded.subMap(startKey,
                    endKey);
            assertEquals(0, subSubMap.size());

            subSubMap = subMap_startExcluded_endIncluded.subMap(startKey,
                    endKey);
            assertEquals(0, subSubMap.size());

            subSubMap = subMap_startIncluded_endExcluded.subMap(startKey,
                    endKey);
            assertEquals(0, subSubMap.size());

            subSubMap = subMap_startIncluded_endIncluded.subMap(startKey,
                    endKey);
            assertEquals(0, subSubMap.size());
        }

        for (int i = 101, j = 5; i < 105; i++) {
            startKey = new Integer(i).toString();
            endKey = new Integer(i + j).toString();

            subSubMap = subMap_default.subMap(startKey, endKey);
            assertEquals(j, subSubMap.size());

            subSubMap = subMap_startExcluded_endExcluded.subMap(startKey,
                    endKey);
            assertEquals(j, subSubMap.size());

            subSubMap = subMap_startExcluded_endIncluded.subMap(startKey,
                    endKey);
            assertEquals(j, subSubMap.size());

            subSubMap = subMap_startIncluded_endExcluded.subMap(startKey,
                    endKey);
            assertEquals(j, subSubMap.size());

            subSubMap = subMap_startIncluded_endIncluded.subMap(startKey,
                    endKey);
            assertEquals(j, subSubMap.size());
        }

        startKey = new Integer(108).toString();
        endKey = new Integer(109).toString();

        subSubMap = subMap_default.subMap(startKey, endKey);
        assertEquals(1, subSubMap.size());

        subSubMap = subMap_startExcluded_endExcluded.subMap(startKey, endKey);
        assertEquals(1, subSubMap.size());

        subSubMap = subMap_startExcluded_endIncluded.subMap(startKey, endKey);
        assertEquals(1, subSubMap.size());

        subSubMap = subMap_startIncluded_endExcluded.subMap(startKey, endKey);
        assertEquals(1, subSubMap.size());

        subSubMap = subMap_startIncluded_endIncluded.subMap(startKey, endKey);
        assertEquals(1, subSubMap.size());

        startKey = new Integer(109).toString();
        endKey = new Integer(109).toString();

        try {
            subMap_default.subMap(startKey, endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startExcluded_endExcluded.subMap(startKey, endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        subSubMap = subMap_startExcluded_endIncluded.subMap(startKey, endKey);
        assertEquals(0, subSubMap.size());

        try {
            subMap_startIncluded_endExcluded.subMap(startKey, endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        subSubMap = subMap_startIncluded_endIncluded.subMap(startKey, endKey);
        assertEquals(0, subSubMap.size());
    }

    public void test_SubMap_subMap_Comparator() {
        String startKey = new Integer[100].toString();
        String endKey = new Integer[100].toString();
        try {
            subMap_default_comparator.subMap(startKey, endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startExcluded_endExcluded_comparator
                    .subMap(startKey, endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startExcluded_endIncluded_comparator
                    .subMap(startKey, endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startIncluded_endExcluded_comparator
                    .subMap(startKey, endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startIncluded_endIncluded_comparator
                    .subMap(startKey, endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        SortedMap subSubMap = null;
        for (int i = 101; i < 109; i++) {
            startKey = new Integer(i).toString();
            endKey = startKey;

            subSubMap = subMap_default_comparator.subMap(startKey, endKey);
            assertEquals(0, subSubMap.size());

            subSubMap = subMap_startExcluded_endExcluded_comparator.subMap(
                    startKey, endKey);
            assertEquals(0, subSubMap.size());

            subSubMap = subMap_startExcluded_endIncluded_comparator.subMap(
                    startKey, endKey);
            assertEquals(0, subSubMap.size());

            subSubMap = subMap_startIncluded_endExcluded_comparator.subMap(
                    startKey, endKey);
            assertEquals(0, subSubMap.size());

            subSubMap = subMap_startIncluded_endIncluded_comparator.subMap(
                    startKey, endKey);
            assertEquals(0, subSubMap.size());
        }

        for (int i = 101, j = 5; i < 105; i++) {
            startKey = new Integer(i).toString();
            endKey = new Integer(i + j).toString();

            subSubMap = subMap_default_comparator.subMap(startKey, endKey);
            assertEquals(j, subSubMap.size());

            subSubMap = subMap_startExcluded_endExcluded_comparator.subMap(
                    startKey, endKey);
            assertEquals(j, subSubMap.size());

            subSubMap = subMap_startExcluded_endIncluded_comparator.subMap(
                    startKey, endKey);
            assertEquals(j, subSubMap.size());

            subSubMap = subMap_startIncluded_endExcluded_comparator.subMap(
                    startKey, endKey);
            assertEquals(j, subSubMap.size());

            subSubMap = subMap_startIncluded_endIncluded_comparator.subMap(
                    startKey, endKey);
            assertEquals(j, subSubMap.size());
        }

        startKey = new Integer(108).toString();
        endKey = new Integer(109).toString();

        subSubMap = subMap_default_comparator.subMap(startKey, endKey);
        assertEquals(1, subSubMap.size());

        subSubMap = subMap_startExcluded_endExcluded_comparator.subMap(
                startKey, endKey);
        assertEquals(1, subSubMap.size());

        subSubMap = subMap_startExcluded_endIncluded_comparator.subMap(
                startKey, endKey);
        assertEquals(1, subSubMap.size());

        subSubMap = subMap_startIncluded_endExcluded_comparator.subMap(
                startKey, endKey);
        assertEquals(1, subSubMap.size());

        subSubMap = subMap_startIncluded_endIncluded_comparator.subMap(
                startKey, endKey);
        assertEquals(1, subSubMap.size());

        startKey = new Integer(109).toString();
        endKey = new Integer(109).toString();

        try {
            subMap_default_comparator.subMap(startKey, endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startExcluded_endExcluded_comparator
                    .subMap(startKey, endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        subSubMap = subMap_startExcluded_endIncluded_comparator.subMap(
                startKey, endKey);
        assertEquals(0, subSubMap.size());

        try {
            subMap_startIncluded_endExcluded_comparator
                    .subMap(startKey, endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        subSubMap = subMap_startIncluded_endIncluded_comparator.subMap(
                startKey, endKey);
        assertEquals(0, subSubMap.size());
    }

    public void test_SubMap_tailMap() {
        String startKey = new Integer(99).toString();
        try {
            subMap_default.tailMap(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startExcluded_endExcluded.tailMap(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startExcluded_endIncluded.tailMap(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startIncluded_endExcluded.tailMap(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startIncluded_endIncluded.tailMap(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        SortedMap tailMap = null;

        startKey = new Integer(100).toString();
        tailMap = subMap_default.tailMap(startKey);
        assertEquals(9, tailMap.size());

        try {
            subMap_startExcluded_endExcluded.tailMap(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startExcluded_endIncluded.tailMap(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        tailMap = subMap_startIncluded_endExcluded.tailMap(startKey);
        assertEquals(9, tailMap.size());

        tailMap = subMap_startIncluded_endIncluded.tailMap(startKey);
        assertEquals(10, tailMap.size());

        for (int i = 0, j = 101, end = 8; i < end; i++) {
            startKey = new Integer(i + j).toString();
            tailMap = subMap_default.tailMap(startKey);
            assertEquals(end - i, tailMap.size());

            tailMap = subMap_startExcluded_endExcluded.tailMap(startKey);
            assertEquals(end - i, tailMap.size());

            tailMap = subMap_startExcluded_endIncluded.tailMap(startKey);
            assertEquals(end - i + 1, tailMap.size());

            tailMap = subMap_startIncluded_endExcluded.tailMap(startKey);
            assertEquals(end - i, tailMap.size());

            tailMap = subMap_startIncluded_endIncluded.tailMap(startKey);
            assertEquals(end - i + 1, tailMap.size());
        }

        startKey = new Integer(109).toString();
        try {
            subMap_default.tailMap(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            subMap_startExcluded_endExcluded.tailMap(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        tailMap = subMap_startExcluded_endIncluded.tailMap(startKey);
        assertEquals(1, tailMap.size());

        try {
            subMap_startIncluded_endExcluded.tailMap(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        tailMap = subMap_startIncluded_endIncluded.tailMap(startKey);
        assertEquals(1, tailMap.size());

        startKey = new Integer(110).toString();
        try {
            subMap_default.tailMap(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            subMap_startExcluded_endExcluded.tailMap(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            subMap_startExcluded_endIncluded.tailMap(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            subMap_startIncluded_endExcluded.tailMap(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            subMap_startIncluded_endIncluded.tailMap(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
    }

    public void test_SubMap_values() {
        Collection values = subMap_default.values();

        assertFalse(values.isEmpty());
        assertTrue(values.contains(100));
        for (int i = 101; i < 109; i++) {
            assertTrue(values.contains(i));
        }
        assertFalse(values.contains(109));

        values = subMap_startExcluded_endExcluded.values();
        assertFalse(values.isEmpty());
        assertFalse(values.contains(100));
        for (int i = 101; i < 109; i++) {
            assertTrue(values.contains(i));
        }
        assertFalse(values.contains(109));

        values = subMap_startExcluded_endIncluded.values();
        assertFalse(values.isEmpty());
        assertFalse(values.contains(100));
        for (int i = 101; i < 109; i++) {
            assertTrue(values.contains(i));
        }
        assertTrue(values.contains(109));

        values = subMap_startIncluded_endExcluded.values();
        assertFalse(values.isEmpty());
        assertTrue(values.contains(100));
        for (int i = 101; i < 109; i++) {
            assertTrue(values.contains(i));
        }
        assertFalse(values.contains(109));

        values = subMap_startIncluded_endIncluded.values();
        assertFalse(values.isEmpty());
        assertTrue(values.contains(100));
        for (int i = 100; i < 109; i++) {
            assertTrue(values.contains(i));
        }
        assertTrue(values.contains(109));
    }

    public void test_SubMap_size() {
        assertEquals(9, subMap_default.size());
        assertEquals(8, subMap_startExcluded_endExcluded.size());
        assertEquals(9, subMap_startExcluded_endIncluded.size());
        assertEquals(9, subMap_startIncluded_endExcluded.size());
        assertEquals(10, subMap_startIncluded_endIncluded.size());

        assertEquals(9, subMap_default_comparator.size());
        assertEquals(8, subMap_startExcluded_endExcluded_comparator.size());
        assertEquals(9, subMap_startExcluded_endIncluded_comparator.size());
        assertEquals(9, subMap_startIncluded_endExcluded_comparator.size());
        assertEquals(10, subMap_startIncluded_endIncluded_comparator.size());
    }

    public void test_SubMap_readObject() throws Exception {
        // SerializationTest.verifySelf(subMap_default);
        // SerializationTest.verifySelf(subMap_startExcluded_endExcluded);
        // SerializationTest.verifySelf(subMap_startExcluded_endIncluded);
        // SerializationTest.verifySelf(subMap_startIncluded_endExcluded);
        // SerializationTest.verifySelf(subMap_startIncluded_endIncluded);
    }

    public void test_AscendingSubMap_ceilingEntry() {
        String key = new Integer(99).toString();
        assertNull(navigableMap_startExcluded_endExcluded.ceilingEntry(key));
        assertNull(navigableMap_startExcluded_endIncluded.ceilingEntry(key));
        assertNull(navigableMap_startIncluded_endExcluded.ceilingEntry(key));
        assertNull(navigableMap_startIncluded_endIncluded.ceilingEntry(key));

        key = new Integer(100).toString();
        assertEquals(101, navigableMap_startExcluded_endExcluded.ceilingEntry(
                key).getValue());
        assertEquals(101, navigableMap_startExcluded_endIncluded.ceilingEntry(
                key).getValue());
        assertEquals(100, navigableMap_startIncluded_endExcluded.ceilingEntry(
                key).getValue());
        assertEquals(100, navigableMap_startIncluded_endIncluded.ceilingEntry(
                key).getValue());

        for (int i = 101; i < 109; i++) {
            key = new Integer(i).toString();
            assertEquals(i, navigableMap_startExcluded_endExcluded
                    .ceilingEntry(key).getValue());
            assertEquals(i, navigableMap_startExcluded_endIncluded
                    .ceilingEntry(key).getValue());
            assertEquals(i, navigableMap_startIncluded_endExcluded
                    .ceilingEntry(key).getValue());
            assertEquals(i, navigableMap_startIncluded_endIncluded
                    .ceilingEntry(key).getValue());

        }

        key = new Integer(109).toString();
        assertNull(navigableMap_startExcluded_endExcluded.ceilingEntry(key));
        assertEquals(109, navigableMap_startExcluded_endIncluded.ceilingEntry(
                key).getValue());
        assertNull(navigableMap_startIncluded_endExcluded.ceilingEntry(key));
        assertEquals(109, navigableMap_startIncluded_endIncluded.ceilingEntry(
                key).getValue());

        key = new Integer(110).toString();
        assertNull(navigableMap_startExcluded_endExcluded.ceilingEntry(key));
        assertNull(navigableMap_startExcluded_endIncluded.ceilingEntry(key));
        assertNull(navigableMap_startIncluded_endExcluded.ceilingEntry(key));
        assertNull(navigableMap_startIncluded_endIncluded.ceilingEntry(key));
    }

    public void test_AscendingSubMap_descendingMap() {
        NavigableMap descendingMap = navigableMap_startExcluded_endExcluded
                .descendingMap();
        assertEquals(navigableMap_startExcluded_endExcluded.size(),
                descendingMap.size());
        assertNotNull(descendingMap.comparator());

        assertEquals(navigableMap_startExcluded_endExcluded.firstKey(),
                descendingMap.lastKey());
        assertEquals(navigableMap_startExcluded_endExcluded.firstEntry(),
                descendingMap.lastEntry());

        assertEquals(navigableMap_startExcluded_endExcluded.lastKey(),
                descendingMap.firstKey());
        assertEquals(navigableMap_startExcluded_endExcluded.lastEntry(),
                descendingMap.firstEntry());

        descendingMap = navigableMap_startExcluded_endIncluded.descendingMap();
        assertEquals(navigableMap_startExcluded_endIncluded.size(),
                descendingMap.size());
        assertNotNull(descendingMap.comparator());

        assertEquals(navigableMap_startExcluded_endIncluded.firstKey(),
                descendingMap.lastKey());
        assertEquals(navigableMap_startExcluded_endIncluded.firstEntry(),
                descendingMap.lastEntry());

        assertEquals(navigableMap_startExcluded_endIncluded.lastKey(),
                descendingMap.firstKey());
        assertEquals(navigableMap_startExcluded_endIncluded.lastEntry(),
                descendingMap.firstEntry());

        descendingMap = navigableMap_startIncluded_endExcluded.descendingMap();
        assertEquals(navigableMap_startIncluded_endExcluded.size(),
                descendingMap.size());
        assertNotNull(descendingMap.comparator());

        assertEquals(navigableMap_startIncluded_endExcluded.firstKey(),
                descendingMap.lastKey());
        assertEquals(navigableMap_startIncluded_endExcluded.firstEntry(),
                descendingMap.lastEntry());

        assertEquals(navigableMap_startIncluded_endExcluded.lastKey(),
                descendingMap.firstKey());
        assertEquals(navigableMap_startIncluded_endExcluded.lastEntry(),
                descendingMap.firstEntry());

        descendingMap = navigableMap_startIncluded_endIncluded.descendingMap();
        assertEquals(navigableMap_startIncluded_endIncluded.size(),
                descendingMap.size());
        assertNotNull(descendingMap.comparator());

        assertEquals(navigableMap_startIncluded_endIncluded.firstKey(),
                descendingMap.lastKey());
        assertEquals(navigableMap_startIncluded_endIncluded.firstEntry(),
                descendingMap.lastEntry());

        assertEquals(navigableMap_startIncluded_endIncluded.lastKey(),
                descendingMap.firstKey());
        assertEquals(navigableMap_startIncluded_endIncluded.lastEntry(),
                descendingMap.firstEntry());
    }

    public void test_AscendingSubMap_floorEntry() {
        String key = new Integer(99).toString();
        assertEquals(108, navigableMap_startExcluded_endExcluded
                .floorEntry(key).getValue());
        assertEquals(109, navigableMap_startExcluded_endIncluded
                .floorEntry(key).getValue());
        assertEquals(108, navigableMap_startIncluded_endExcluded
                .floorEntry(key).getValue());
        assertEquals(109, navigableMap_startIncluded_endIncluded
                .floorEntry(key).getValue());

        key = new Integer(100).toString();
        assertNull(navigableMap_startExcluded_endExcluded.floorEntry(key));
        assertNull(navigableMap_startExcluded_endIncluded.floorEntry(key));
        assertEquals(100, navigableMap_startIncluded_endExcluded
                .floorEntry(key).getValue());
        assertEquals(100, navigableMap_startIncluded_endIncluded
                .floorEntry(key).getValue());

        for (int i = 101; i < 109; i++) {
            key = new Integer(i).toString();
            assertEquals(i, navigableMap_startExcluded_endExcluded.floorEntry(
                    key).getValue());
            assertEquals(i, navigableMap_startExcluded_endIncluded.floorEntry(
                    key).getValue());
            assertEquals(i, navigableMap_startIncluded_endExcluded.floorEntry(
                    key).getValue());
            assertEquals(i, navigableMap_startIncluded_endIncluded.floorEntry(
                    key).getValue());

        }

        key = new Integer(109).toString();
        assertEquals(108, navigableMap_startExcluded_endExcluded
                .floorEntry(key).getValue());
        assertEquals(109, navigableMap_startExcluded_endIncluded
                .floorEntry(key).getValue());
        assertEquals(108, navigableMap_startIncluded_endExcluded
                .floorEntry(key).getValue());
        assertEquals(109, navigableMap_startIncluded_endIncluded
                .floorEntry(key).getValue());

        key = new Integer(110).toString();
        assertEquals(108, navigableMap_startExcluded_endExcluded
                .floorEntry(key).getValue());
        assertEquals(109, navigableMap_startExcluded_endIncluded
                .floorEntry(key).getValue());
        assertEquals(108, navigableMap_startIncluded_endExcluded
                .floorEntry(key).getValue());
        assertEquals(109, navigableMap_startIncluded_endIncluded
                .floorEntry(key).getValue());
    }

    public void test_AscendingSubMap_pollFirstEntry() {
        assertEquals(101, navigableMap_startExcluded_endExcluded
                .pollFirstEntry().getValue());
        assertEquals(102, navigableMap_startExcluded_endIncluded
                .pollFirstEntry().getValue());
        assertEquals(100, navigableMap_startIncluded_endExcluded
                .pollFirstEntry().getValue());
        assertEquals(103, navigableMap_startIncluded_endIncluded
                .pollFirstEntry().getValue());
    }

    public void test_AscendingSubMap_pollLastEntry() {
        assertEquals(108, navigableMap_startExcluded_endExcluded
                .pollLastEntry().getValue());
        assertEquals(109, navigableMap_startExcluded_endIncluded
                .pollLastEntry().getValue());
        assertEquals(107, navigableMap_startIncluded_endExcluded
                .pollLastEntry().getValue());
        assertEquals(106, navigableMap_startIncluded_endIncluded
                .pollLastEntry().getValue());
    }

    public void test_AscendingSubMap_entrySet() {
        assertEquals(8, navigableMap_startExcluded_endExcluded.entrySet()
                .size());
        assertEquals(9, navigableMap_startExcluded_endIncluded.entrySet()
                .size());
        assertEquals(9, navigableMap_startIncluded_endExcluded.entrySet()
                .size());
        assertEquals(10, navigableMap_startIncluded_endIncluded.entrySet()
                .size());
    }

    public void test_AscendingSubMap_subMap() {
        Set entrySet;
        Entry startEntry, endEntry;
        int startIndex, endIndex;
        SortedMap subMap;
        Iterator subMapSetIterator;

        entrySet = navigableMap_startExcluded_endExcluded.entrySet();
        Iterator startIterator = entrySet.iterator();
        while (startIterator.hasNext()) {
            startEntry = (Entry) startIterator.next();
            startIndex = (Integer) startEntry.getValue();
            Iterator endIterator = entrySet.iterator();
            while (endIterator.hasNext()) {
                endEntry = (Entry) endIterator.next();
                endIndex = (Integer) endEntry.getValue();

                if (startIndex > endIndex) {
                    try {
                        navigableMap_startExcluded_endExcluded.subMap(
                                startEntry.getKey(), endEntry.getKey());
                        fail("should throw IllegalArgumentException");
                    } catch (IllegalArgumentException e) {
                        // Expected
                    }
                    try {
                        navigableMap_startExcluded_endExcluded.subMap(
                                startEntry.getKey(), false, endEntry.getKey(),
                                false);
                        fail("should throw IllegalArgumentException");
                    } catch (IllegalArgumentException e) {
                        // Expected
                    }
                    try {
                        navigableMap_startExcluded_endExcluded.subMap(
                                startEntry.getKey(), false, endEntry.getKey(),
                                true);
                        fail("should throw IllegalArgumentException");
                    } catch (IllegalArgumentException e) {
                        // Expected
                    }
                    try {
                        navigableMap_startExcluded_endExcluded.subMap(
                                startEntry.getKey(), true, endEntry.getKey(),
                                false);
                        fail("should throw IllegalArgumentException");
                    } catch (IllegalArgumentException e) {
                        // Expected
                    }
                    try {
                        navigableMap_startExcluded_endExcluded.subMap(
                                startEntry.getKey(), true, endEntry.getKey(),
                                true);
                        fail("should throw IllegalArgumentException");
                    } catch (IllegalArgumentException e) {
                        // Expected
                    }
                } else {
                    subMap = navigableMap_startExcluded_endExcluded.subMap(
                            startEntry.getKey(), endEntry.getKey());
                    subMapSetIterator = subMap.entrySet().iterator();
                    for (int index = startIndex; index < endIndex; index++) {
                        assertEquals(index, ((Entry) subMapSetIterator.next())
                                .getValue());
                    }

                    subMap = navigableMap_startExcluded_endExcluded.subMap(
                            startEntry.getKey(), false, endEntry.getKey(),
                            false);
                    subMapSetIterator = subMap.entrySet().iterator();
                    for (int index = startIndex + 1; index < endIndex; index++) {
                        assertEquals(index, ((Entry) subMapSetIterator.next())
                                .getValue());
                    }

                    subMap = navigableMap_startExcluded_endExcluded
                            .subMap(startEntry.getKey(), false, endEntry
                                    .getKey(), true);
                    subMapSetIterator = subMap.entrySet().iterator();
                    for (int index = startIndex + 1; index < endIndex; index++) {
                        assertEquals(index, ((Entry) subMapSetIterator.next())
                                .getValue());
                    }

                    subMap = navigableMap_startExcluded_endExcluded
                            .subMap(startEntry.getKey(), true, endEntry
                                    .getKey(), false);
                    subMapSetIterator = subMap.entrySet().iterator();
                    for (int index = startIndex; index < endIndex; index++) {
                        assertEquals(index, ((Entry) subMapSetIterator.next())
                                .getValue());
                    }

                    subMap = navigableMap_startExcluded_endExcluded.subMap(
                            startEntry.getKey(), true, endEntry.getKey(), true);
                    subMapSetIterator = subMap.entrySet().iterator();
                    for (int index = startIndex; index <= endIndex; index++) {
                        assertEquals(index, ((Entry) subMapSetIterator.next())
                                .getValue());
                    }
                }
            }
        }
    }

    public void test_DescendingSubMap_ceilingEntry() {
        NavigableMap decendingMap = tm.descendingMap();
        String key = new Integer(-1).toString();
        assertNull(decendingMap.ceilingEntry(key));
        for (int i = 0; i < objArray.length; i++) {
            key = objArray[i].toString();
            assertEquals(objArray[i], decendingMap.ceilingEntry(key).getValue());
        }
        key = new Integer(1000).toString();
        assertEquals(100, decendingMap.ceilingEntry(key).getValue());
        key = new Integer(1001).toString();
        assertEquals(100, decendingMap.ceilingEntry(key).getValue());

        decendingMap = navigableMap_startExcluded_endExcluded.descendingMap();
        key = new Integer(100).toString();
        assertNull(decendingMap.ceilingEntry(key));
        for (int i = 101; i < 109; i++) {
            key = new Integer(i).toString();
            assertEquals(i, decendingMap.ceilingEntry(key).getValue());
        }
        key = new Integer(109).toString();
        assertEquals(108, decendingMap.ceilingEntry(key).getValue());

        decendingMap = navigableMap_startExcluded_endIncluded.descendingMap();
        key = new Integer(100).toString();
        assertNull(decendingMap.ceilingEntry(key));
        for (int i = 101; i < 109; i++) {
            key = new Integer(i).toString();
            assertEquals(i, decendingMap.ceilingEntry(key).getValue());
        }
        key = new Integer(109).toString();
        assertEquals(109, decendingMap.ceilingEntry(key).getValue());

        decendingMap = navigableMap_startIncluded_endExcluded.descendingMap();
        key = new Integer(100).toString();
        assertEquals(100, decendingMap.ceilingEntry(key).getValue());
        for (int i = 101; i < 109; i++) {
            key = new Integer(i).toString();
            assertEquals(i, decendingMap.ceilingEntry(key).getValue());
        }
        key = new Integer(109).toString();
        assertEquals(108, decendingMap.ceilingEntry(key).getValue());

        decendingMap = navigableMap_startIncluded_endIncluded.descendingMap();
        key = new Integer(100).toString();
        assertEquals(100, decendingMap.ceilingEntry(key).getValue());
        for (int i = 101; i < 109; i++) {
            key = new Integer(i).toString();
            assertEquals(i, decendingMap.ceilingEntry(key).getValue());
        }
        key = new Integer(109).toString();
        assertEquals(109, decendingMap.ceilingEntry(key).getValue());

        // With Comparator
        decendingMap = ((NavigableMap) subMap_startExcluded_endExcluded_comparator)
                .descendingMap();
        key = new Integer(100).toString();
        assertNull(decendingMap.ceilingEntry(key));
        for (int i = 101; i < 109; i++) {
            key = new Integer(i).toString();
            assertEquals(i, decendingMap.ceilingEntry(key).getValue());
        }
        key = new Integer(109).toString();
        assertEquals(108, decendingMap.ceilingEntry(key).getValue());

        decendingMap = ((NavigableMap) subMap_startExcluded_endIncluded_comparator)
                .descendingMap();
        key = new Integer(100).toString();
        assertNull(decendingMap.ceilingEntry(key));
        for (int i = 101; i < 109; i++) {
            key = new Integer(i).toString();
            assertEquals(i, decendingMap.ceilingEntry(key).getValue());
        }
        key = new Integer(109).toString();
        assertEquals(109, decendingMap.ceilingEntry(key).getValue());

        decendingMap = ((NavigableMap) subMap_startIncluded_endExcluded_comparator)
                .descendingMap();
        key = new Integer(100).toString();
        assertEquals(100, decendingMap.ceilingEntry(key).getValue());
        for (int i = 101; i < 109; i++) {
            key = new Integer(i).toString();
            assertEquals(i, decendingMap.ceilingEntry(key).getValue());
        }
        key = new Integer(109).toString();
        assertEquals(108, decendingMap.ceilingEntry(key).getValue());

        decendingMap = ((NavigableMap) subMap_startIncluded_endIncluded_comparator)
                .descendingMap();
        key = new Integer(100).toString();
        assertEquals(100, decendingMap.ceilingEntry(key).getValue());
        for (int i = 101; i < 109; i++) {
            key = new Integer(i).toString();
            assertEquals(i, decendingMap.ceilingEntry(key).getValue());
        }
        key = new Integer(109).toString();
        assertEquals(109, decendingMap.ceilingEntry(key).getValue());
    }

    public void test_DescendingSubMap_descendingMap() {
        NavigableMap decendingMap = tm.descendingMap();
        NavigableMap decendingDecendingMap = decendingMap.descendingMap();
        assertEquals(decendingMap, decendingDecendingMap);

        NavigableMap decendingMapHeadMap = decendingMap.headMap(
                new Integer(990).toString(), false);
        NavigableMap decendingDecendingHeadMap = decendingMapHeadMap
                .descendingMap();
        assertNotNull(decendingMapHeadMap);
        assertNotNull(decendingDecendingHeadMap);
        assertEquals(decendingMapHeadMap, decendingDecendingHeadMap);

        NavigableMap decendingMapTailMap = decendingMap.tailMap(
                new Integer(990).toString(), false);
        NavigableMap decendingDecendingTailMap = decendingMapTailMap
                .descendingMap();
        assertNotNull(decendingMapTailMap);
        assertNotNull(decendingDecendingTailMap);
        // assertEquals(decendingMapTailMap,decendingDecendingTailMap);

        decendingMap = navigableMap_startExcluded_endExcluded.descendingMap();
        decendingDecendingMap = decendingMap.descendingMap();
        assertEquals(decendingMap, decendingDecendingMap);

        decendingMapHeadMap = decendingMap.headMap(new Integer(104).toString(),
                false);
        decendingDecendingHeadMap = decendingMapHeadMap.descendingMap();
        assertEquals(decendingMapHeadMap, decendingDecendingHeadMap);

        decendingMapTailMap = decendingMap.tailMap(new Integer(104).toString(),
                false);
        decendingDecendingTailMap = decendingMapTailMap.descendingMap();
        assertEquals(decendingMapTailMap, decendingDecendingTailMap);

        decendingMap = navigableMap_startExcluded_endIncluded.descendingMap();
        decendingDecendingMap = decendingMap.descendingMap();
        assertEquals(decendingMap, decendingDecendingMap);

        decendingMapHeadMap = decendingMap.headMap(new Integer(104).toString(),
                false);
        decendingDecendingHeadMap = decendingMapHeadMap.descendingMap();
        assertEquals(decendingMapHeadMap, decendingDecendingHeadMap);

        decendingMapTailMap = decendingMap.tailMap(new Integer(104).toString(),
                false);
        decendingDecendingTailMap = decendingMapTailMap.descendingMap();
        assertEquals(decendingMapTailMap, decendingDecendingTailMap);

        decendingMap = navigableMap_startIncluded_endExcluded.descendingMap();
        decendingDecendingMap = decendingMap.descendingMap();
        assertEquals(decendingMap, decendingDecendingMap);

        decendingMapHeadMap = decendingMap.headMap(new Integer(104).toString(),
                false);
        decendingDecendingHeadMap = decendingMapHeadMap.descendingMap();
        assertEquals(decendingMapHeadMap, decendingDecendingHeadMap);

        decendingMapTailMap = decendingMap.tailMap(new Integer(104).toString(),
                false);
        decendingDecendingTailMap = decendingMapTailMap.descendingMap();
        assertEquals(decendingMapTailMap, decendingDecendingTailMap);

        decendingMap = navigableMap_startIncluded_endIncluded.descendingMap();
        decendingDecendingMap = decendingMap.descendingMap();
        assertEquals(decendingMap, decendingDecendingMap);

        decendingMapHeadMap = decendingMap.headMap(new Integer(104).toString(),
                false);
        decendingDecendingHeadMap = decendingMapHeadMap.descendingMap();
        assertEquals(decendingMapHeadMap, decendingDecendingHeadMap);

        decendingMapTailMap = decendingMap.tailMap(new Integer(104).toString(),
                false);
        decendingDecendingTailMap = decendingMapTailMap.descendingMap();
        assertEquals(decendingMapTailMap, decendingDecendingTailMap);
    }

    public void test_DescendingSubMap_firstEntry() {
        NavigableMap decendingMap = tm.descendingMap();
        assertEquals(999, decendingMap.firstEntry().getValue());

        decendingMap = navigableMap_startExcluded_endExcluded.descendingMap();
        assertEquals(108, decendingMap.firstEntry().getValue());

        decendingMap = navigableMap_startExcluded_endIncluded.descendingMap();
        assertEquals(109, decendingMap.firstEntry().getValue());

        decendingMap = navigableMap_startIncluded_endExcluded.descendingMap();
        assertEquals(108, decendingMap.firstEntry().getValue());

        decendingMap = navigableMap_startIncluded_endIncluded.descendingMap();
        assertEquals(109, decendingMap.firstEntry().getValue());
    }

    public void test_DescendingSubMap_floorEntry() {
        NavigableMap decendingMap = tm.descendingMap();
        String key = new Integer(-1).toString();
        assertEquals(0, decendingMap.floorEntry(key).getValue());
        for (int i = 0; i < objArray.length; i++) {
            key = objArray[i].toString();
            assertEquals(objArray[i], decendingMap.floorEntry(key).getValue());
        }
        key = new Integer(1000).toString();
        assertEquals(101, decendingMap.floorEntry(key).getValue());
        key = new Integer(1001).toString();
        assertEquals(101, decendingMap.floorEntry(key).getValue());

        decendingMap = navigableMap_startExcluded_endExcluded.descendingMap();
        key = new Integer(100).toString();
        assertEquals(101, decendingMap.floorEntry(key).getValue());
        for (int i = 101; i < 109; i++) {
            key = new Integer(i).toString();
            assertEquals(i, decendingMap.floorEntry(key).getValue());
        }
        key = new Integer(109).toString();
        assertNull(decendingMap.floorEntry(key));

        decendingMap = navigableMap_startExcluded_endIncluded.descendingMap();
        key = new Integer(100).toString();
        assertEquals(101, decendingMap.floorEntry(key).getValue());
        for (int i = 101; i < 109; i++) {
            key = new Integer(i).toString();
            assertEquals(i, decendingMap.floorEntry(key).getValue());
        }
        key = new Integer(109).toString();
        assertEquals(109, decendingMap.floorEntry(key).getValue());

        decendingMap = navigableMap_startIncluded_endExcluded.descendingMap();
        key = new Integer(100).toString();
        assertEquals(100, decendingMap.floorEntry(key).getValue());
        for (int i = 101; i < 109; i++) {
            key = new Integer(i).toString();
            assertEquals(i, decendingMap.floorEntry(key).getValue());
        }
        key = new Integer(109).toString();
        assertNull(decendingMap.floorEntry(key));

        decendingMap = navigableMap_startIncluded_endIncluded.descendingMap();
        key = new Integer(100).toString();
        assertEquals(100, decendingMap.floorEntry(key).getValue());
        for (int i = 101; i < 109; i++) {
            key = new Integer(i).toString();
            assertEquals(i, decendingMap.floorEntry(key).getValue());
        }
        key = new Integer(109).toString();
        assertEquals(109, decendingMap.floorEntry(key).getValue());

        // With Comparator
        decendingMap = ((NavigableMap) subMap_startExcluded_endExcluded_comparator)
                .descendingMap();
        key = new Integer(100).toString();
        assertEquals(101, decendingMap.floorEntry(key).getValue());
        for (int i = 101; i < 109; i++) {
            key = new Integer(i).toString();
            assertEquals(i, decendingMap.floorEntry(key).getValue());
        }
        key = new Integer(109).toString();
        assertNull(decendingMap.floorEntry(key));

        decendingMap = ((NavigableMap) subMap_startExcluded_endIncluded_comparator)
                .descendingMap();
        key = new Integer(100).toString();
        assertEquals(101, decendingMap.floorEntry(key).getValue());
        for (int i = 101; i < 109; i++) {
            key = new Integer(i).toString();
            assertEquals(i, decendingMap.floorEntry(key).getValue());
        }
        key = new Integer(109).toString();
        assertEquals(109, decendingMap.floorEntry(key).getValue());

        decendingMap = ((NavigableMap) subMap_startIncluded_endExcluded_comparator)
                .descendingMap();
        key = new Integer(100).toString();
        assertEquals(100, decendingMap.floorEntry(key).getValue());
        for (int i = 101; i < 109; i++) {
            key = new Integer(i).toString();
            assertEquals(i, decendingMap.floorEntry(key).getValue());
        }
        key = new Integer(109).toString();
        assertNull(decendingMap.floorEntry(key));

        decendingMap = ((NavigableMap) subMap_startIncluded_endIncluded_comparator)
                .descendingMap();
        key = new Integer(100).toString();
        assertEquals(100, decendingMap.floorEntry(key).getValue());
        for (int i = 101; i < 109; i++) {
            key = new Integer(i).toString();
            assertEquals(i, decendingMap.floorEntry(key).getValue());
        }
        key = new Integer(109).toString();
        assertEquals(109, decendingMap.floorEntry(key).getValue());
    }

    public void test_DescendingSubMap_lastEntry() {
        NavigableMap decendingMap = tm.descendingMap();
        assertEquals(0, decendingMap.lastEntry().getValue());

        decendingMap = navigableMap_startExcluded_endExcluded.descendingMap();
        assertEquals(101, decendingMap.lastEntry().getValue());

        decendingMap = navigableMap_startExcluded_endIncluded.descendingMap();
        assertEquals(101, decendingMap.lastEntry().getValue());

        decendingMap = navigableMap_startIncluded_endExcluded.descendingMap();
        assertEquals(100, decendingMap.lastEntry().getValue());

        decendingMap = navigableMap_startIncluded_endIncluded.descendingMap();
        assertEquals(100, decendingMap.lastEntry().getValue());
    }

    public void test_DescendingSubMap_higherEntry() {
        NavigableMap decendingMap;
        NavigableMap decendingTailMap;
        Integer value;
        Entry entry;
        decendingMap = navigableMap_startExcluded_endExcluded.descendingMap();
        value = new Integer(101);
        assertNull(decendingMap.higherEntry(value.toString()));

        for (int i = 108; i > 101; i--) {
            value = new Integer(i);
            entry = decendingMap.higherEntry(value.toString());
            assertEquals(value - 1, entry.getValue());
        }

        value = new Integer(109);
        entry = decendingMap.higherEntry(value.toString());
        assertEquals(108, entry.getValue());

        decendingTailMap = decendingMap.tailMap(new Integer(104).toString(),
                false);
        value = new Integer(109);
        entry = decendingTailMap.higherEntry(value.toString());
        assertEquals(103, entry.getValue());

        decendingMap = navigableMap_startIncluded_endExcluded.descendingMap();
        value = new Integer(100);
        assertNull(decendingMap.higherEntry(value.toString()));

        for (int i = 108; i > 100; i--) {
            value = new Integer(i);
            entry = decendingMap.higherEntry(value.toString());
            assertEquals(value - 1, entry.getValue());
        }

        value = new Integer(109);
        entry = decendingMap.higherEntry(value.toString());
        assertEquals(108, entry.getValue());

        decendingTailMap = decendingMap.tailMap(new Integer(104).toString(),
                false);
        value = new Integer(109);
        entry = decendingTailMap.higherEntry(value.toString());
        assertEquals(103, entry.getValue());

        decendingMap = navigableMap_startExcluded_endIncluded.descendingMap();
        value = new Integer(101);
        assertNull(decendingMap.higherEntry(value.toString()));

        for (int i = 109; i > 101; i--) {
            value = new Integer(i);
            entry = decendingMap.higherEntry(value.toString());
            assertEquals(value - 1, entry.getValue());
        }

        value = new Integer(2);
        entry = decendingMap.higherEntry(value.toString());
        assertEquals(109, entry.getValue());

        decendingTailMap = decendingMap.tailMap(new Integer(104).toString(),
                false);
        value = new Integer(109);
        entry = decendingTailMap.higherEntry(value.toString());
        assertEquals(103, entry.getValue());

        decendingMap = navigableMap_startIncluded_endIncluded.descendingMap();
        value = new Integer(100);
        assertNull(decendingMap.higherEntry(value.toString()));

        for (int i = 109; i > 100; i--) {
            value = new Integer(i);
            entry = decendingMap.higherEntry(value.toString());
            assertEquals(value - 1, entry.getValue());
        }

        value = new Integer(2);
        entry = decendingMap.higherEntry(value.toString());
        assertEquals(109, entry.getValue());

        decendingTailMap = decendingMap.tailMap(new Integer(104).toString(),
                false);
        value = new Integer(109);
        entry = decendingTailMap.higherEntry(value.toString());
        assertEquals(103, entry.getValue());
    }

    public void test_DescendingSubMap_lowerEntry() {
        NavigableMap decendingMap;
        NavigableMap decendingHeadMap;
        Integer value;
        Entry entry;
        decendingMap = navigableMap_startExcluded_endExcluded.descendingMap();
        value = new Integer(99);
        assertNull(decendingMap.lowerEntry(value.toString()));
        for (int i = 100; i < 108; i++) {
            value = new Integer(i);
            entry = decendingMap.lowerEntry(value.toString());
            assertEquals(value + 1, entry.getValue());
        }
        value = new Integer(109);
        assertNull(decendingMap.lowerEntry(value.toString()));

        decendingHeadMap = decendingMap.headMap(new Integer(103).toString(),
                false);
        for (int i = 104; i < 106; i++) {
            value = new Integer(i);
            entry = decendingHeadMap.lowerEntry(value.toString());
            assertEquals(value + 1, entry.getValue());
        }
        value = new Integer(102);
        entry = decendingHeadMap.lowerEntry(value.toString());
        assertEquals(104, entry.getValue());

        value = new Integer(109);
        entry = decendingHeadMap.lowerEntry(value.toString());
        assertNull(entry);

        decendingMap = navigableMap_startExcluded_endIncluded.descendingMap();
        value = new Integer(99);
        assertNull(decendingMap.lowerEntry(value.toString()));
        for (int i = 100; i < 109; i++) {
            value = new Integer(i);
            entry = decendingMap.lowerEntry(value.toString());
            assertEquals(value + 1, entry.getValue());
        }
        value = new Integer(110);
        assertNull(decendingMap.lowerEntry(value.toString()));

        decendingHeadMap = decendingMap.headMap(new Integer(103).toString(),
                false);
        for (int i = 104; i < 109; i++) {
            value = new Integer(i);
            entry = decendingHeadMap.lowerEntry(value.toString());
            assertEquals(value + 1, entry.getValue());
        }
        value = new Integer(102);
        entry = decendingHeadMap.lowerEntry(value.toString());
        assertEquals(104, entry.getValue());

        value = new Integer(2);
        entry = decendingHeadMap.lowerEntry(value.toString());
        assertNull(entry);

        decendingMap = navigableMap_startIncluded_endExcluded.descendingMap();
        value = new Integer(99);
        assertNull(decendingMap.lowerEntry(value.toString()));
        for (int i = 100; i < 108; i++) {
            value = new Integer(i);
            entry = decendingMap.lowerEntry(value.toString());
            assertEquals(value + 1, entry.getValue());
        }
        value = new Integer(109);
        assertNull(decendingMap.lowerEntry(value.toString()));

        decendingHeadMap = decendingMap.headMap(new Integer(103).toString(),
                false);
        for (int i = 104; i < 107; i++) {
            value = new Integer(i);
            entry = decendingHeadMap.lowerEntry(value.toString());
            assertEquals(value + 1, entry.getValue());
        }
        value = new Integer(102);
        entry = decendingHeadMap.lowerEntry(value.toString());
        assertEquals(104, entry.getValue());

        value = new Integer(2);
        entry = decendingHeadMap.lowerEntry(value.toString());
        assertNull(entry);

        decendingMap = navigableMap_startIncluded_endIncluded.descendingMap();
        value = new Integer(99);
        assertNull(decendingMap.lowerEntry(value.toString()));
        for (int i = 100; i < 109; i++) {
            value = new Integer(i);
            entry = decendingMap.lowerEntry(value.toString());
            assertEquals(value + 1, entry.getValue());
        }
        value = new Integer(110);
        assertNull(decendingMap.lowerEntry(value.toString()));

        decendingHeadMap = decendingMap.headMap(new Integer(103).toString(),
                false);
        for (int i = 104; i < 109; i++) {
            value = new Integer(i);
            entry = decendingHeadMap.lowerEntry(value.toString());
            assertEquals(value + 1, entry.getValue());
        }
        value = new Integer(102);
        entry = decendingHeadMap.lowerEntry(value.toString());
        assertEquals(104, entry.getValue());

        value = new Integer(2);
        entry = decendingHeadMap.lowerEntry(value.toString());
        assertNull(entry);
    }

    public void test_DescendingSubMap_pollFirstEntry() {
        NavigableMap decendingMap = tm.descendingMap();
        assertEquals(999, decendingMap.pollFirstEntry().getValue());

        decendingMap = navigableMap_startExcluded_endExcluded.descendingMap();
        assertEquals(108, decendingMap.pollFirstEntry().getValue());

        decendingMap = navigableMap_startExcluded_endIncluded.descendingMap();
        assertEquals(109, decendingMap.pollFirstEntry().getValue());

        decendingMap = navigableMap_startIncluded_endExcluded.descendingMap();
        assertEquals(107, decendingMap.pollFirstEntry().getValue());

        decendingMap = navigableMap_startIncluded_endIncluded.descendingMap();
        assertEquals(106, decendingMap.pollFirstEntry().getValue());
    }

    public void test_DescendingSubMap_pollLastEntry() {
        NavigableMap decendingMap = tm.descendingMap();
        assertEquals(0, decendingMap.pollLastEntry().getValue());

        decendingMap = navigableMap_startExcluded_endExcluded.descendingMap();
        assertEquals(101, decendingMap.pollLastEntry().getValue());

        decendingMap = navigableMap_startExcluded_endIncluded.descendingMap();
        assertEquals(102, decendingMap.pollLastEntry().getValue());

        decendingMap = navigableMap_startIncluded_endExcluded.descendingMap();
        assertEquals(100, decendingMap.pollLastEntry().getValue());

        decendingMap = navigableMap_startIncluded_endIncluded.descendingMap();
        assertEquals(103, decendingMap.pollLastEntry().getValue());
    }

    public void test_DescendingSubMap_values() {
        NavigableMap decendingMap = tm.descendingMap();
        Collection values = decendingMap.values();
        assertFalse(values.isEmpty());
        assertFalse(values.contains(1000));
        for (int i = 999; i > 0; i--) {
            assertTrue(values.contains(i));
        }
        assertTrue(values.contains(0));

        String endKey = new Integer(99).toString();
        NavigableMap headMap = decendingMap.headMap(endKey, false);
        values = headMap.values();
        Iterator it = values.iterator();
        for (int i = 999; i > 990; i--) {
            assertTrue(values.contains(i));
            assertEquals(i, it.next());
        }

        String startKey = new Integer(11).toString();
        NavigableMap tailMap = decendingMap.tailMap(startKey, false);
        values = tailMap.values();
        it = values.iterator();
        for (int i = 109; i > 100; i--) {
            assertTrue(values.contains(i));
            assertEquals(i, it.next());
        }

        decendingMap = navigableMap_startExcluded_endExcluded.descendingMap();
        values = decendingMap.values();
        assertFalse(values.isEmpty());
        assertFalse(values.contains(109));
        for (int i = 108; i > 100; i--) {
            assertTrue(values.contains(i));
        }
        assertFalse(values.contains(100));

        decendingMap = navigableMap_startExcluded_endIncluded.descendingMap();
        values = decendingMap.values();
        assertFalse(values.isEmpty());
        assertFalse(values.contains(100));
        for (int i = 108; i > 100; i--) {
            assertTrue(values.contains(i));
        }
        assertTrue(values.contains(109));

        decendingMap = navigableMap_startIncluded_endExcluded.descendingMap();
        values = decendingMap.values();
        assertFalse(values.isEmpty());
        assertTrue(values.contains(100));
        for (int i = 108; i > 100; i--) {
            assertTrue(values.contains(i));
        }
        assertFalse(values.contains(109));

        decendingMap = navigableMap_startIncluded_endIncluded.descendingMap();
        values = decendingMap.values();
        assertFalse(values.isEmpty());
        assertTrue(values.contains(100));
        for (int i = 108; i > 100; i--) {
            assertTrue(values.contains(i));
        }
        assertTrue(values.contains(109));
    }

    public void test_DescendingSubMap_headMap() {
        NavigableMap decendingMap = tm.descendingMap();
        String endKey = new Integer(0).toString(), key;
        SortedMap subDecendingMap_Included = decendingMap.headMap(endKey, true);
        SortedMap subDecendingMap_Excluded = decendingMap
                .headMap(endKey, false);
        key = endKey;
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));
        for (int i = 1; i < 1000; i++) {
            key = new Integer(i).toString();
            assertTrue(subDecendingMap_Included.containsKey(key));
            assertTrue(subDecendingMap_Excluded.containsKey(key));
        }
        key = new Integer(1000).toString();
        assertFalse(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        decendingMap = navigableMap_startExcluded_endExcluded.descendingMap();
        endKey = new Integer(100).toString();
        try {
            decendingMap.headMap(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        subDecendingMap_Excluded = decendingMap.headMap(endKey, false);
        key = endKey;
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        endKey = new Integer(101).toString();
        subDecendingMap_Included = decendingMap.headMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.headMap(endKey, false);

        key = endKey;
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        for (int i = 102; i < 109; i++) {
            key = new Integer(i).toString();
            assertTrue(subDecendingMap_Included.containsKey(key));
            assertTrue(subDecendingMap_Excluded.containsKey(key));
        }
        key = new Integer(109).toString();
        assertFalse(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        decendingMap = navigableMap_startExcluded_endIncluded.descendingMap();
        endKey = new Integer(100).toString();
        try {
            decendingMap.headMap(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        subDecendingMap_Excluded = decendingMap.headMap(endKey, false);
        key = endKey;
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        endKey = new Integer(101).toString();
        subDecendingMap_Included = decendingMap.headMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.headMap(endKey, false);

        key = endKey;
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        for (int i = 102; i < 109; i++) {
            key = new Integer(i).toString();
            assertTrue(subDecendingMap_Included.containsKey(key));
            assertTrue(subDecendingMap_Excluded.containsKey(key));
        }
        key = new Integer(109).toString();
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertTrue(subDecendingMap_Excluded.containsKey(key));

        decendingMap = navigableMap_startIncluded_endExcluded.descendingMap();
        endKey = new Integer(100).toString();
        subDecendingMap_Included = decendingMap.headMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.headMap(endKey, false);
        key = endKey;
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        endKey = new Integer(101).toString();
        subDecendingMap_Included = decendingMap.headMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.headMap(endKey, false);

        key = endKey;
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        for (int i = 102; i < 109; i++) {
            key = new Integer(i).toString();
            assertTrue(subDecendingMap_Included.containsKey(key));
            assertTrue(subDecendingMap_Excluded.containsKey(key));
        }
        key = new Integer(109).toString();
        assertFalse(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        decendingMap = navigableMap_startIncluded_endIncluded.descendingMap();
        endKey = new Integer(100).toString();
        subDecendingMap_Included = decendingMap.headMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.headMap(endKey, false);
        key = endKey;
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        endKey = new Integer(101).toString();
        subDecendingMap_Included = decendingMap.headMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.headMap(endKey, false);

        key = endKey;
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        for (int i = 102; i < 109; i++) {
            key = new Integer(i).toString();
            assertTrue(subDecendingMap_Included.containsKey(key));
            assertTrue(subDecendingMap_Excluded.containsKey(key));
        }
        key = new Integer(109).toString();
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertTrue(subDecendingMap_Excluded.containsKey(key));

        // With Comparator

        decendingMap = ((NavigableMap) subMap_startExcluded_endExcluded_comparator)
                .descendingMap();
        endKey = new Integer(100).toString();
        try {
            decendingMap.headMap(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        subDecendingMap_Excluded = decendingMap.headMap(endKey, false);
        key = endKey;
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        endKey = new Integer(101).toString();
        subDecendingMap_Included = decendingMap.headMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.headMap(endKey, false);

        key = endKey;
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        for (int i = 102; i < 109; i++) {
            key = new Integer(i).toString();
            assertTrue(subDecendingMap_Included.containsKey(key));
            assertTrue(subDecendingMap_Excluded.containsKey(key));
        }
        key = new Integer(109).toString();
        assertFalse(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        decendingMap = ((NavigableMap) subMap_startExcluded_endIncluded_comparator)
                .descendingMap();
        endKey = new Integer(100).toString();
        try {
            decendingMap.headMap(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        subDecendingMap_Excluded = decendingMap.headMap(endKey, false);
        key = endKey;
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        endKey = new Integer(101).toString();
        subDecendingMap_Included = decendingMap.headMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.headMap(endKey, false);

        key = endKey;
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        for (int i = 102; i < 109; i++) {
            key = new Integer(i).toString();
            assertTrue(subDecendingMap_Included.containsKey(key));
            assertTrue(subDecendingMap_Excluded.containsKey(key));
        }
        key = new Integer(109).toString();
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertTrue(subDecendingMap_Excluded.containsKey(key));

        decendingMap = ((NavigableMap) subMap_startIncluded_endExcluded_comparator)
                .descendingMap();
        endKey = new Integer(100).toString();
        subDecendingMap_Included = decendingMap.headMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.headMap(endKey, false);
        key = endKey;
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        endKey = new Integer(101).toString();
        subDecendingMap_Included = decendingMap.headMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.headMap(endKey, false);

        key = endKey;
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        for (int i = 102; i < 109; i++) {
            key = new Integer(i).toString();
            assertTrue(subDecendingMap_Included.containsKey(key));
            assertTrue(subDecendingMap_Excluded.containsKey(key));
        }
        key = new Integer(109).toString();
        assertFalse(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        decendingMap = ((NavigableMap) subMap_startIncluded_endIncluded_comparator)
                .descendingMap();
        endKey = new Integer(100).toString();
        subDecendingMap_Included = decendingMap.headMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.headMap(endKey, false);
        key = endKey;
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        endKey = new Integer(101).toString();
        subDecendingMap_Included = decendingMap.headMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.headMap(endKey, false);

        key = endKey;
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        for (int i = 102; i < 109; i++) {
            key = new Integer(i).toString();
            assertTrue(subDecendingMap_Included.containsKey(key));
            assertTrue(subDecendingMap_Excluded.containsKey(key));
        }
        key = new Integer(109).toString();
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertTrue(subDecendingMap_Excluded.containsKey(key));
    }

    public void test_DescendingSubMap_subMap() {
        NavigableMap descendingMap = tm.descendingMap();
        String startKey = new Integer(109).toString();
        String endKey = new Integer(100).toString();
        try {
            descendingMap.subMap(endKey, false, startKey, false);
        } catch (IllegalArgumentException e) {
            // Expected
        }

        SortedMap subDescendingMap = descendingMap.subMap(startKey, false,
                endKey, false);
        String key = new Integer(100).toString();
        assertFalse(subDescendingMap.containsKey(key));
        for (int i = 101; i < 109; i++) {
            key = new Integer(i).toString();
            assertTrue(subDescendingMap.containsKey(key));
        }
        key = new Integer(109).toString();
        assertFalse(subDescendingMap.containsKey(key));

        subDescendingMap = descendingMap.subMap(startKey, false, endKey, true);
        key = new Integer(100).toString();
        assertTrue(subDescendingMap.containsKey(key));
        for (int i = 101; i < 109; i++) {
            key = new Integer(i).toString();
            assertTrue(subDescendingMap.containsKey(key));
        }
        key = new Integer(109).toString();
        assertFalse(subDescendingMap.containsKey(key));

        subDescendingMap = descendingMap.subMap(startKey, true, endKey, false);
        key = new Integer(100).toString();
        assertFalse(subDescendingMap.containsKey(key));
        for (int i = 101; i < 109; i++) {
            key = new Integer(i).toString();
            assertTrue(subDescendingMap.containsKey(key));
        }
        key = new Integer(109).toString();
        assertTrue(subDescendingMap.containsKey(key));

        subDescendingMap = descendingMap.subMap(startKey, true, endKey, true);
        key = new Integer(100).toString();
        assertTrue(subDescendingMap.containsKey(key));
        for (int i = 101; i < 109; i++) {
            key = new Integer(i).toString();
            assertTrue(subDescendingMap.containsKey(key));
        }
        key = new Integer(109).toString();
        assertTrue(subDescendingMap.containsKey(key));

        TreeMap<Integer, String> treeMap = new TreeMap<Integer, String>();
        for (int i = -10; i < 10; i++) {
            treeMap.put(i, String.valueOf(i));
        }
        descendingMap = treeMap.descendingMap();
        subDescendingMap = descendingMap.subMap(5, 0);
        assertEquals(5, subDescendingMap.size());
    }

    public void test_DescendingSubMap_tailMap() {
        // tm
        NavigableMap decendingMap = tm.descendingMap();
        String endKey = new Integer(1000).toString(), key;
        SortedMap subDecendingMap_Included = decendingMap.tailMap(endKey, true);
        SortedMap subDecendingMap_Excluded = decendingMap
                .tailMap(endKey, false);

        key = endKey;
        assertFalse(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));
        key = new Integer(100).toString();
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertTrue(subDecendingMap_Excluded.containsKey(key));

        key = new Integer(10).toString();
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertTrue(subDecendingMap_Excluded.containsKey(key));

        key = new Integer(1).toString();
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertTrue(subDecendingMap_Excluded.containsKey(key));

        key = new Integer(0).toString();
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertTrue(subDecendingMap_Excluded.containsKey(key));

        endKey = new Integer(999).toString();
        subDecendingMap_Included = decendingMap.tailMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        key = endKey;
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));
        for (int i = 998; i > 0; i--) {
            key = new Integer(i).toString();
            assertTrue(subDecendingMap_Included.containsKey(key));
            assertTrue(subDecendingMap_Excluded.containsKey(key));
        }
        key = new Integer(0).toString();
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertTrue(subDecendingMap_Excluded.containsKey(key));

        endKey = new Integer(0).toString();
        subDecendingMap_Included = decendingMap.tailMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        assertEquals(1, subDecendingMap_Included.size());
        key = endKey;
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertTrue(subDecendingMap_Excluded.isEmpty());

        // navigableMap_startExcluded_endExcluded
        decendingMap = navigableMap_startExcluded_endExcluded.descendingMap();
        endKey = new Integer(110).toString();
        try {
            decendingMap.tailMap(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            decendingMap.tailMap(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(109).toString();
        try {
            decendingMap.tailMap(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        key = endKey;
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        endKey = new Integer(108).toString();
        subDecendingMap_Included = decendingMap.tailMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        key = endKey;
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));
        for (int i = 107; i > 100; i--) {
            key = new Integer(i).toString();
            assertTrue(subDecendingMap_Included.containsKey(key));
            assertTrue(subDecendingMap_Excluded.containsKey(key));
        }
        key = new Integer(100).toString();
        assertFalse(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Included.containsKey(key));

        endKey = new Integer(101).toString();
        subDecendingMap_Included = decendingMap.tailMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        key = endKey;
        assertEquals(1, subDecendingMap_Included.size());
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertTrue(subDecendingMap_Excluded.isEmpty());

        endKey = new Integer(100).toString();
        try {
            decendingMap.tailMap(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        assertTrue(subDecendingMap_Excluded.isEmpty());

        endKey = new Integer(99).toString();
        try {
            decendingMap.tailMap(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            decendingMap.tailMap(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        // navigableMap_startExcluded_endIncluded
        decendingMap = navigableMap_startExcluded_endIncluded.descendingMap();
        endKey = new Integer(110).toString();
        try {
            decendingMap.tailMap(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            decendingMap.tailMap(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(109).toString();
        subDecendingMap_Included = decendingMap.tailMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        key = endKey;
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        endKey = new Integer(108).toString();
        subDecendingMap_Included = decendingMap.tailMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        key = endKey;
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));
        for (int i = 107; i > 100; i--) {
            key = new Integer(i).toString();
            assertTrue(subDecendingMap_Included.containsKey(key));
            assertTrue(subDecendingMap_Excluded.containsKey(key));
        }
        key = new Integer(100).toString();
        assertFalse(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Included.containsKey(key));

        endKey = new Integer(101).toString();
        subDecendingMap_Included = decendingMap.tailMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        key = endKey;
        assertEquals(1, subDecendingMap_Included.size());
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertTrue(subDecendingMap_Excluded.isEmpty());

        endKey = new Integer(100).toString();
        try {
            decendingMap.tailMap(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        assertTrue(subDecendingMap_Excluded.isEmpty());

        endKey = new Integer(99).toString();
        try {
            decendingMap.tailMap(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            decendingMap.tailMap(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        // navigableMap_startIncluded_endExcluded
        decendingMap = navigableMap_startIncluded_endExcluded.descendingMap();
        endKey = new Integer(110).toString();
        try {
            decendingMap.tailMap(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            decendingMap.tailMap(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(109).toString();
        try {
            decendingMap.tailMap(endKey, true);

        } catch (IllegalArgumentException e) {
            // Expected
        }
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        key = endKey;
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        endKey = new Integer(108).toString();
        subDecendingMap_Included = decendingMap.tailMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        key = endKey;
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));
        for (int i = 107; i > 100; i--) {
            key = new Integer(i).toString();
            assertTrue(subDecendingMap_Included.containsKey(key));
            assertTrue(subDecendingMap_Excluded.containsKey(key));
        }
        key = new Integer(100).toString();
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertTrue(subDecendingMap_Included.containsKey(key));

        endKey = new Integer(101).toString();
        subDecendingMap_Included = decendingMap.tailMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        key = endKey;
        assertEquals(2, subDecendingMap_Included.size());
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        endKey = new Integer(100).toString();
        subDecendingMap_Included = decendingMap.tailMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        key = endKey;
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        endKey = new Integer(99).toString();
        try {
            decendingMap.tailMap(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            decendingMap.tailMap(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        // navigableMap_startIncluded_endIncluded
        decendingMap = navigableMap_startIncluded_endIncluded.descendingMap();
        endKey = new Integer(110).toString();
        try {
            decendingMap.tailMap(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            decendingMap.tailMap(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(109).toString();
        try {
            decendingMap.tailMap(endKey, true);

        } catch (IllegalArgumentException e) {
            // Expected
        }
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        key = endKey;
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        endKey = new Integer(108).toString();
        subDecendingMap_Included = decendingMap.tailMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        key = endKey;
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));
        for (int i = 107; i > 100; i--) {
            key = new Integer(i).toString();
            assertTrue(subDecendingMap_Included.containsKey(key));
            assertTrue(subDecendingMap_Excluded.containsKey(key));
        }
        key = new Integer(100).toString();
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertTrue(subDecendingMap_Included.containsKey(key));

        endKey = new Integer(101).toString();
        subDecendingMap_Included = decendingMap.tailMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        key = endKey;
        assertEquals(2, subDecendingMap_Included.size());
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        endKey = new Integer(100).toString();
        subDecendingMap_Included = decendingMap.tailMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        key = endKey;
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        endKey = new Integer(99).toString();
        try {
            decendingMap.tailMap(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            decendingMap.tailMap(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        // With Comparator
        decendingMap = ((NavigableMap) subMap_startExcluded_endExcluded_comparator)
                .descendingMap();
        endKey = new Integer(110).toString();
        try {
            decendingMap.tailMap(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            decendingMap.tailMap(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(109).toString();
        try {
            decendingMap.tailMap(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        key = endKey;
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        endKey = new Integer(108).toString();
        subDecendingMap_Included = decendingMap.tailMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        key = endKey;
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));
        for (int i = 107; i > 100; i--) {
            key = new Integer(i).toString();
            assertTrue(subDecendingMap_Included.containsKey(key));
            assertTrue(subDecendingMap_Excluded.containsKey(key));
        }
        key = new Integer(100).toString();
        assertFalse(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Included.containsKey(key));

        endKey = new Integer(101).toString();
        subDecendingMap_Included = decendingMap.tailMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        key = endKey;
        assertEquals(1, subDecendingMap_Included.size());
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertTrue(subDecendingMap_Excluded.isEmpty());

        endKey = new Integer(100).toString();
        try {
            decendingMap.tailMap(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        assertTrue(subDecendingMap_Excluded.isEmpty());

        endKey = new Integer(99).toString();
        try {
            decendingMap.tailMap(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            decendingMap.tailMap(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        decendingMap = ((NavigableMap) subMap_startExcluded_endIncluded_comparator)
                .descendingMap();
        endKey = new Integer(110).toString();
        try {
            decendingMap.tailMap(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            decendingMap.tailMap(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(109).toString();
        subDecendingMap_Included = decendingMap.tailMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        key = endKey;
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        endKey = new Integer(108).toString();
        subDecendingMap_Included = decendingMap.tailMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        key = endKey;
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));
        for (int i = 107; i > 100; i--) {
            key = new Integer(i).toString();
            assertTrue(subDecendingMap_Included.containsKey(key));
            assertTrue(subDecendingMap_Excluded.containsKey(key));
        }
        key = new Integer(100).toString();
        assertFalse(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Included.containsKey(key));

        endKey = new Integer(101).toString();
        subDecendingMap_Included = decendingMap.tailMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        key = endKey;
        assertEquals(1, subDecendingMap_Included.size());
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertTrue(subDecendingMap_Excluded.isEmpty());

        endKey = new Integer(100).toString();
        try {
            decendingMap.tailMap(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        assertTrue(subDecendingMap_Excluded.isEmpty());

        endKey = new Integer(99).toString();
        try {
            decendingMap.tailMap(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            decendingMap.tailMap(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        // navigableMap_startIncluded_endExcluded
        decendingMap = ((NavigableMap) subMap_startIncluded_endExcluded)
                .descendingMap();
        endKey = new Integer(110).toString();
        try {
            decendingMap.tailMap(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            decendingMap.tailMap(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(109).toString();
        try {
            decendingMap.tailMap(endKey, true);

        } catch (IllegalArgumentException e) {
            // Expected
        }
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        key = endKey;
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        endKey = new Integer(108).toString();
        subDecendingMap_Included = decendingMap.tailMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        key = endKey;
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));
        for (int i = 107; i > 100; i--) {
            key = new Integer(i).toString();
            assertTrue(subDecendingMap_Included.containsKey(key));
            assertTrue(subDecendingMap_Excluded.containsKey(key));
        }
        key = new Integer(100).toString();
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertTrue(subDecendingMap_Included.containsKey(key));

        endKey = new Integer(101).toString();
        subDecendingMap_Included = decendingMap.tailMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        key = endKey;
        assertEquals(2, subDecendingMap_Included.size());
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        endKey = new Integer(100).toString();
        subDecendingMap_Included = decendingMap.tailMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        key = endKey;
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        endKey = new Integer(99).toString();
        try {
            decendingMap.tailMap(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            decendingMap.tailMap(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        decendingMap = ((NavigableMap) subMap_startIncluded_endIncluded)
                .descendingMap();
        endKey = new Integer(110).toString();
        try {
            decendingMap.tailMap(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            decendingMap.tailMap(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(109).toString();
        try {
            decendingMap.tailMap(endKey, true);

        } catch (IllegalArgumentException e) {
            // Expected
        }
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        key = endKey;
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        endKey = new Integer(108).toString();
        subDecendingMap_Included = decendingMap.tailMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        key = endKey;
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));
        for (int i = 107; i > 100; i--) {
            key = new Integer(i).toString();
            assertTrue(subDecendingMap_Included.containsKey(key));
            assertTrue(subDecendingMap_Excluded.containsKey(key));
        }
        key = new Integer(100).toString();
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertTrue(subDecendingMap_Included.containsKey(key));

        endKey = new Integer(101).toString();
        subDecendingMap_Included = decendingMap.tailMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        key = endKey;
        assertEquals(2, subDecendingMap_Included.size());
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        endKey = new Integer(100).toString();
        subDecendingMap_Included = decendingMap.tailMap(endKey, true);
        subDecendingMap_Excluded = decendingMap.tailMap(endKey, false);
        key = endKey;
        assertTrue(subDecendingMap_Included.containsKey(key));
        assertFalse(subDecendingMap_Excluded.containsKey(key));

        endKey = new Integer(99).toString();
        try {
            decendingMap.tailMap(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            decendingMap.tailMap(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
    }

    public void test_Entry_setValue() {
        TreeMap treeMap = new TreeMap();
        Integer value = null;
        for (int i = 0; i < 50; i++) {
            value = new Integer(i);
            treeMap.put(value, value);
        }
        Map checkedMap = Collections.checkedMap(treeMap, Integer.class,
                Integer.class);
        Set entrySet = checkedMap.entrySet();
        Iterator iterator = entrySet.iterator();
        Entry entry;
        value = new Integer(0);
        for (; iterator.hasNext(); value++) {
            entry = (Entry) iterator.next();
            assertEquals(value, entry.setValue(value + 1));
            assertEquals(value + 1, entry.getValue());
        }
    }

    public void test_DescendingSubMapEntrySet_comparator() {
        Set entrySet;
        NavigableSet descendingSet;
        Comparator comparator;
        Entry[] entryArray;
        Integer value1, value2;

        entrySet = navigableMap_startExcluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSet = ((NavigableSet) entrySet).descendingSet();
            assertNull(((NavigableSet) entrySet).comparator());
            comparator = descendingSet.comparator();
            assertNotNull(comparator);

            entryArray = (Entry[]) descendingSet
                    .toArray(new Entry[descendingSet.size()]);
            for (int i = 1; i < entryArray.length; i++) {
                value1 = (Integer) entryArray[i - 1].getValue();
                value2 = (Integer) entryArray[i].getValue();
                assertTrue(value1 > value2);
                assertTrue(comparator.compare(value1, value2) < 0);
            }
        }

        entrySet = navigableMap_startExcluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSet = ((NavigableSet) entrySet).descendingSet();
            assertNull(((NavigableSet) entrySet).comparator());
            comparator = descendingSet.comparator();
            assertNotNull(comparator);

            entryArray = (Entry[]) descendingSet
                    .toArray(new Entry[descendingSet.size()]);
            for (int i = 1; i < entryArray.length; i++) {
                value1 = (Integer) entryArray[i - 1].getValue();
                value2 = (Integer) entryArray[i].getValue();
                assertTrue(value1 > value2);
                assertTrue(comparator.compare(value1, value2) < 0);
            }
        }

        entrySet = navigableMap_startIncluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSet = ((NavigableSet) entrySet).descendingSet();
            assertNull(((NavigableSet) entrySet).comparator());
            comparator = descendingSet.comparator();
            assertNotNull(comparator);

            entryArray = (Entry[]) descendingSet
                    .toArray(new Entry[descendingSet.size()]);
            for (int i = 1; i < entryArray.length; i++) {
                value1 = (Integer) entryArray[i - 1].getValue();
                value2 = (Integer) entryArray[i].getValue();
                assertTrue(value1 > value2);
                assertTrue(comparator.compare(value1, value2) < 0);
            }
        }

        entrySet = navigableMap_startIncluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSet = ((NavigableSet) entrySet).descendingSet();
            assertNull(((NavigableSet) entrySet).comparator());
            comparator = descendingSet.comparator();
            assertNotNull(comparator);

            entryArray = (Entry[]) descendingSet
                    .toArray(new Entry[descendingSet.size()]);
            for (int i = 1; i < entryArray.length; i++) {
                value1 = (Integer) entryArray[i - 1].getValue();
                value2 = (Integer) entryArray[i].getValue();
                assertTrue(value1 > value2);
                assertTrue(comparator.compare(value1, value2) < 0);
            }
        }

        String endKey = new Integer(2).toString();
        entrySet = tm.headMap(endKey, true).entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSet = ((NavigableSet) entrySet).descendingSet();
            assertNotNull(descendingSet.comparator());
        }
    }

    public void test_DescendingSubMapEntrySet_descendingSet() {
        Set entrySet;
        NavigableSet ascendingSubMapEntrySet, descendingSet, descendingDescedingSet;
        Entry[] ascendingEntryArray, descendingDescendingArray;

        entrySet = navigableMap_startExcluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            descendingSet = ascendingSubMapEntrySet.descendingSet();
            descendingDescedingSet = descendingSet.descendingSet();
            ascendingEntryArray = (Entry[]) ascendingSubMapEntrySet
                    .toArray(new Entry[ascendingSubMapEntrySet.size()]);

            descendingDescendingArray = (Entry[]) descendingDescedingSet
                    .toArray(new Entry[descendingDescedingSet.size()]);

            assertEquals(ascendingEntryArray.length,
                    descendingDescendingArray.length);
            for (int i = 0; i < ascendingEntryArray.length; i++) {
                assertEquals(ascendingEntryArray[i],
                        descendingDescendingArray[i]);
            }
        }

        entrySet = navigableMap_startExcluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            descendingSet = ascendingSubMapEntrySet.descendingSet();
            descendingDescedingSet = descendingSet.descendingSet();
            ascendingEntryArray = (Entry[]) ascendingSubMapEntrySet
                    .toArray(new Entry[ascendingSubMapEntrySet.size()]);

            descendingDescendingArray = (Entry[]) descendingDescedingSet
                    .toArray(new Entry[descendingDescedingSet.size()]);

            assertEquals(ascendingEntryArray.length,
                    descendingDescendingArray.length);
            for (int i = 0; i < ascendingEntryArray.length; i++) {
                assertEquals(ascendingEntryArray[i],
                        descendingDescendingArray[i]);
            }
        }

        entrySet = navigableMap_startIncluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            descendingSet = ascendingSubMapEntrySet.descendingSet();
            descendingDescedingSet = descendingSet.descendingSet();
            ascendingEntryArray = (Entry[]) ascendingSubMapEntrySet
                    .toArray(new Entry[ascendingSubMapEntrySet.size()]);

            descendingDescendingArray = (Entry[]) descendingDescedingSet
                    .toArray(new Entry[descendingDescedingSet.size()]);

            assertEquals(ascendingEntryArray.length,
                    descendingDescendingArray.length);
            for (int i = 0; i < ascendingEntryArray.length; i++) {
                assertEquals(ascendingEntryArray[i],
                        descendingDescendingArray[i]);
            }
        }

        entrySet = navigableMap_startIncluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            descendingSet = ascendingSubMapEntrySet.descendingSet();
            descendingDescedingSet = descendingSet.descendingSet();
            ascendingEntryArray = (Entry[]) ascendingSubMapEntrySet
                    .toArray(new Entry[ascendingSubMapEntrySet.size()]);

            descendingDescendingArray = (Entry[]) descendingDescedingSet
                    .toArray(new Entry[descendingDescedingSet.size()]);

            assertEquals(ascendingEntryArray.length,
                    descendingDescendingArray.length);
            for (int i = 0; i < ascendingEntryArray.length; i++) {
                assertEquals(ascendingEntryArray[i],
                        descendingDescendingArray[i]);
            }
        }

        String endKey = new Integer(2).toString();
        entrySet = tm.headMap(endKey, true).entrySet();// 0...2
        if (entrySet instanceof NavigableSet) {
            // [2...0]
            descendingSet = ((NavigableSet) entrySet).descendingSet();
            // [0...2]
            descendingDescedingSet = descendingSet.descendingSet();
            Iterator iterator = descendingDescedingSet.iterator();
            assertEquals(0, ((Entry) iterator.next()).getValue());
        }

        String startKey = new Integer(2).toString();
        entrySet = tm.tailMap(startKey, true).entrySet();// 2...
        if (entrySet instanceof NavigableSet) {
            // [2...0]
            descendingSet = ((NavigableSet) entrySet).descendingSet();
            // [0...2]
            descendingDescedingSet = descendingSet.descendingSet();
            Iterator iterator = descendingDescedingSet.iterator();
            assertEquals(2, ((Entry) iterator.next()).getValue());
        }

    }

    public void test_DescendingSubMapEntrySet_first() {
        Set entrySet;
        NavigableSet ascendingSubMapEntrySet, descendingSet;
        Entry entry;

        entrySet = navigableMap_startExcluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            descendingSet = ascendingSubMapEntrySet.descendingSet();
            entry = (Entry) descendingSet.first();
            assertEquals(101, entry.getValue());
        }

        entrySet = navigableMap_startExcluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            descendingSet = ascendingSubMapEntrySet.descendingSet();
            entry = (Entry) descendingSet.first();
            assertEquals(101, entry.getValue());
        }

        entrySet = navigableMap_startIncluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            descendingSet = ascendingSubMapEntrySet.descendingSet();
            entry = (Entry) descendingSet.first();
            assertEquals(100, entry.getValue());
        }

        entrySet = navigableMap_startIncluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            descendingSet = ascendingSubMapEntrySet.descendingSet();
            entry = (Entry) descendingSet.first();
            assertEquals(100, entry.getValue());
        }
    }

    public void test_DescendingSubMapEntrySet_last() {
        Set entrySet;
        NavigableSet ascendingSubMapEntrySet, descendingSet;
        Entry entry;

        entrySet = navigableMap_startExcluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            descendingSet = ascendingSubMapEntrySet.descendingSet();
            entry = (Entry) descendingSet.last();
            assertEquals(108, entry.getValue());
        }

        entrySet = navigableMap_startExcluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            descendingSet = ascendingSubMapEntrySet.descendingSet();
            entry = (Entry) descendingSet.last();
            assertEquals(109, entry.getValue());
        }

        entrySet = navigableMap_startIncluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            descendingSet = ascendingSubMapEntrySet.descendingSet();
            entry = (Entry) descendingSet.last();
            assertEquals(108, entry.getValue());
        }

        entrySet = navigableMap_startIncluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            descendingSet = ascendingSubMapEntrySet.descendingSet();
            entry = (Entry) descendingSet.last();
            assertEquals(109, entry.getValue());
        }
    }

    public void test_DescendingSubMapEntrySet_pollFirst_startExcluded_endExcluded() {
        Set entrySet = navigableMap_startExcluded_endExcluded.entrySet();
        Entry entry;
        if (entrySet instanceof NavigableSet) {
            NavigableSet descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            assertEquals(8, descendingSubMapEntrySet.size());
            for (int i = 101; i < 109; i++) {
                entry = (Entry) descendingSubMapEntrySet.pollFirst();
                assertEquals(i, entry.getValue());
            }
            assertNull(descendingSubMapEntrySet.pollFirst());
        }
    }

    public void test_DescendingSubMapEntrySet_pollFirst_startExcluded_endIncluded() {
        Set entrySet = navigableMap_startExcluded_endIncluded.entrySet();
        Entry entry;
        if (entrySet instanceof NavigableSet) {
            NavigableSet descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            assertEquals(9, descendingSubMapEntrySet.size());
            for (int i = 101; i < 110; i++) {
                entry = (Entry) descendingSubMapEntrySet.pollFirst();
                assertEquals(i, entry.getValue());
            }
            assertNull(descendingSubMapEntrySet.pollFirst());
        }
    }

    public void test_DescendingSubMapEntrySet_pollFirst_startIncluded_endExcluded() {
        Set entrySet = navigableMap_startIncluded_endExcluded.entrySet();
        Entry entry;
        if (entrySet instanceof NavigableSet) {
            NavigableSet descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            assertEquals(9, descendingSubMapEntrySet.size());
            for (int i = 100; i < 109; i++) {
                entry = (Entry) descendingSubMapEntrySet.pollFirst();
                assertEquals(i, entry.getValue());
            }
            assertNull(descendingSubMapEntrySet.pollFirst());
        }
    }

    public void test_DescendingSubMapEntrySet_pollFirst_startIncluded_endIncluded() {
        Set entrySet = navigableMap_startIncluded_endIncluded.entrySet();
        Entry entry;
        if (entrySet instanceof NavigableSet) {
            NavigableSet descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            assertEquals(10, descendingSubMapEntrySet.size());
            for (int i = 100; i < 110; i++) {
                entry = (Entry) descendingSubMapEntrySet.pollFirst();
                assertEquals(i, entry.getValue());
            }
            assertNull(descendingSubMapEntrySet.pollFirst());
        }
    }

    public void test_DescendingSubMapEntrySet_pollFirst() {
        String key = new Integer(2).toString();
        Set entrySet = tm.headMap(key, true).entrySet();// [0...2]
        NavigableSet descendingEntrySet;
        Entry entry;

        if (entrySet instanceof NavigableSet) {
            // [2...0]
            descendingEntrySet = ((NavigableSet) entrySet).descendingSet();
            entry = (Entry) descendingEntrySet.pollFirst();
            assertEquals(0, entry.getValue());
        }

        entrySet = tm.tailMap(key, true).entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingEntrySet = ((NavigableSet) entrySet).descendingSet();
            entry = (Entry) descendingEntrySet.pollFirst();
            assertEquals(2, entry.getValue());
        }
    }

    public void test_DescendingSubMapEntrySet_pollLast_startExcluded_endExclued() {
        Set entrySet = navigableMap_startExcluded_endExcluded.entrySet();
        Entry entry;
        if (entrySet instanceof NavigableSet) {
            NavigableSet descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            assertEquals(8, descendingSubMapEntrySet.size());
            for (int i = 108; i > 100; i--) {
                entry = (Entry) descendingSubMapEntrySet.pollLast();
                assertEquals(i, entry.getValue());
            }
            assertNull(descendingSubMapEntrySet.pollFirst());
        }
    }

    public void test_DescendingSubMapEntrySet_pollLast_startExcluded_endInclued() {
        Set entrySet = navigableMap_startExcluded_endIncluded.entrySet();
        Entry entry;
        if (entrySet instanceof NavigableSet) {
            NavigableSet descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            assertEquals(9, descendingSubMapEntrySet.size());
            for (int i = 109; i > 100; i--) {
                entry = (Entry) descendingSubMapEntrySet.pollLast();
                assertEquals(i, entry.getValue());
            }
            assertNull(descendingSubMapEntrySet.pollFirst());
        }
    }

    public void test_DescendingSubMapEntrySet_pollLast_startIncluded_endExclued() {
        Set entrySet = navigableMap_startIncluded_endExcluded.entrySet();
        Entry entry;
        if (entrySet instanceof NavigableSet) {
            NavigableSet descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            assertEquals(9, descendingSubMapEntrySet.size());
            for (int i = 108; i > 99; i--) {
                entry = (Entry) descendingSubMapEntrySet.pollLast();
                assertEquals(i, entry.getValue());
            }
            assertNull(descendingSubMapEntrySet.pollFirst());
        }
    }

    public void test_DescendingSubMapEntrySet_pollLast_startIncluded_endInclued() {
        Set entrySet = navigableMap_startIncluded_endIncluded.entrySet();
        Entry entry;
        if (entrySet instanceof NavigableSet) {
            NavigableSet descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            assertEquals(10, descendingSubMapEntrySet.size());
            for (int i = 109; i > 99; i--) {
                entry = (Entry) descendingSubMapEntrySet.pollLast();
                assertEquals(i, entry.getValue());
            }
            assertNull(descendingSubMapEntrySet.pollFirst());
        }
    }

    public void test_DescendingSubMapEntrySet_pollLast() {
        String key = new Integer(2).toString();
        Set entrySet = tm.headMap(key, true).entrySet();// [0...2]
        NavigableSet descendingEntrySet;
        Entry entry;

        if (entrySet instanceof NavigableSet) {
            // [2...0]
            descendingEntrySet = ((NavigableSet) entrySet).descendingSet();
            entry = (Entry) descendingEntrySet.pollLast();
            assertEquals(2, entry.getValue());
        }

        entrySet = tm.tailMap(key, true).entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingEntrySet = ((NavigableSet) entrySet).descendingSet();
            entry = (Entry) descendingEntrySet.pollLast();
            assertEquals(999, entry.getValue());
        }
    }

    public void test_DescendingSubMapEntrySet_descendingIterator() {
        Set entrySet;
        NavigableSet descendingSet;
        Iterator iterator;

        entrySet = navigableMap_startExcluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSet = ((NavigableSet) entrySet).descendingSet();
            iterator = descendingSet.iterator();
            for (int value = 108; value > 100; value--) {
                assertTrue(iterator.hasNext());
                assertEquals(value, ((Entry) iterator.next()).getValue());
            }
            assertFalse(iterator.hasNext());
            try {
                iterator.next();
                fail("should throw NoSuchElementException");
            } catch (NoSuchElementException e) {
                // Expected
            }
        }

        entrySet = navigableMap_startExcluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSet = ((NavigableSet) entrySet).descendingSet();
            iterator = descendingSet.iterator();
            for (int value = 109; value > 100; value--) {
                assertTrue(iterator.hasNext());
                assertEquals(value, ((Entry) iterator.next()).getValue());
            }
            assertFalse(iterator.hasNext());
            try {
                iterator.next();
                fail("should throw NoSuchElementException");
            } catch (NoSuchElementException e) {
                // Expected
            }
        }

        entrySet = navigableMap_startIncluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSet = ((NavigableSet) entrySet).descendingSet();
            iterator = descendingSet.iterator();
            for (int value = 108; value > 99; value--) {
                assertTrue(iterator.hasNext());
                assertEquals(value, ((Entry) iterator.next()).getValue());
            }
            assertFalse(iterator.hasNext());
            try {
                iterator.next();
                fail("should throw NoSuchElementException");
            } catch (NoSuchElementException e) {
                // Expected
            }
        }

        entrySet = navigableMap_startIncluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSet = ((NavigableSet) entrySet).descendingSet();
            iterator = descendingSet.iterator();
            for (int value = 109; value > 99; value--) {
                assertTrue(iterator.hasNext());
                assertEquals(value, ((Entry) iterator.next()).getValue());
            }
            assertFalse(iterator.hasNext());
            try {
                iterator.next();
                fail("should throw NoSuchElementException");
            } catch (NoSuchElementException e) {
                // Expected
            }
        }

        String endKey = new Integer(2).toString();
        entrySet = tm.headMap(endKey, true).entrySet();// 0...2
        if (entrySet instanceof NavigableSet) {
            // [2...0]
            descendingSet = ((NavigableSet) entrySet).descendingSet();
            iterator = descendingSet.descendingIterator();
            assertEquals(0, ((Entry) iterator.next()).getValue());// 0...2
        }
    }

    public void test_DescendingSubMapEntrySet_headSet() {
        Set entrySet, headSet;
        NavigableSet descendingSubMapEntrySet;
        Iterator iterator, headSetIterator;
        Entry entry;
        int value;

        entrySet = navigableMap_startExcluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            iterator = descendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                headSet = descendingSubMapEntrySet.headSet(entry);
                headSetIterator = headSet.iterator();
                for (value = 108; headSetIterator.hasNext(); value--) {
                    assertEquals(value, ((Entry) headSetIterator.next())
                            .getValue());
                }
                try {
                    headSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                headSet = descendingSubMapEntrySet.headSet(entry, false);
                headSetIterator = headSet.iterator();
                for (value = 108; headSetIterator.hasNext(); value--) {
                    assertEquals(value, ((Entry) headSetIterator.next())
                            .getValue());
                }
                try {
                    headSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                headSet = descendingSubMapEntrySet.headSet(entry, true);
                headSetIterator = headSet.iterator();
                for (value = 108; headSetIterator.hasNext(); value--) {
                    assertEquals(value, ((Entry) headSetIterator.next())
                            .getValue());
                }
                try {
                    headSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }
            }
        }

        entrySet = navigableMap_startExcluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            iterator = descendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                headSet = descendingSubMapEntrySet.headSet(entry);
                headSetIterator = headSet.iterator();
                for (value = 109; headSetIterator.hasNext(); value--) {
                    assertEquals(value, ((Entry) headSetIterator.next())
                            .getValue());
                }
                try {
                    headSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                headSet = descendingSubMapEntrySet.headSet(entry, false);
                headSetIterator = headSet.iterator();
                for (value = 109; headSetIterator.hasNext(); value--) {
                    assertEquals(value, ((Entry) headSetIterator.next())
                            .getValue());
                }
                try {
                    headSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                headSet = descendingSubMapEntrySet.headSet(entry, true);
                headSetIterator = headSet.iterator();
                for (value = 109; headSetIterator.hasNext(); value--) {
                    assertEquals(value, ((Entry) headSetIterator.next())
                            .getValue());
                }
                try {
                    headSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }
            }
        }

        entrySet = navigableMap_startIncluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            iterator = descendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                headSet = descendingSubMapEntrySet.headSet(entry);
                headSetIterator = headSet.iterator();
                for (value = 108; headSetIterator.hasNext(); value--) {
                    assertEquals(value, ((Entry) headSetIterator.next())
                            .getValue());
                }
                try {
                    headSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                headSet = descendingSubMapEntrySet.headSet(entry, false);
                headSetIterator = headSet.iterator();
                for (value = 108; headSetIterator.hasNext(); value--) {
                    assertEquals(value, ((Entry) headSetIterator.next())
                            .getValue());
                }
                try {
                    headSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                headSet = descendingSubMapEntrySet.headSet(entry, true);
                headSetIterator = headSet.iterator();
                for (value = 108; headSetIterator.hasNext(); value--) {
                    assertEquals(value, ((Entry) headSetIterator.next())
                            .getValue());
                }
                try {
                    headSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }
            }
        }

        entrySet = navigableMap_startIncluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            iterator = descendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                headSet = descendingSubMapEntrySet.headSet(entry);
                headSetIterator = headSet.iterator();
                for (value = 109; headSetIterator.hasNext(); value--) {
                    assertEquals(value, ((Entry) headSetIterator.next())
                            .getValue());
                }
                try {
                    headSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                headSet = descendingSubMapEntrySet.headSet(entry, false);
                headSetIterator = headSet.iterator();
                for (value = 109; headSetIterator.hasNext(); value--) {
                    assertEquals(value, ((Entry) headSetIterator.next())
                            .getValue());
                }
                try {
                    headSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                headSet = descendingSubMapEntrySet.headSet(entry, true);
                headSetIterator = headSet.iterator();
                for (value = 109; headSetIterator.hasNext(); value--) {
                    assertEquals(value, ((Entry) headSetIterator.next())
                            .getValue());
                }
                try {
                    headSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }
            }
        }

        String endKey = new Integer(2).toString();
        entrySet = tm.headMap(endKey, true).entrySet();// 0...2
        if (entrySet instanceof NavigableSet) {
            // [2...0]
            descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            iterator = descendingSubMapEntrySet.iterator();
            iterator.next();// 2
            iterator.next();// 199
            entry = (Entry) iterator.next();// 198
            headSet = descendingSubMapEntrySet.headSet(entry);
            assertEquals(2, headSet.size());// 2 199
            headSetIterator = headSet.iterator();
            assertEquals(2, ((Entry) headSetIterator.next()).getValue());
            assertEquals(199, ((Entry) headSetIterator.next()).getValue());

            headSet = descendingSubMapEntrySet.headSet(entry, true);
            assertEquals(3, headSet.size());// 2 199
            headSetIterator = headSet.iterator();
            assertEquals(2, ((Entry) headSetIterator.next()).getValue());
            assertEquals(199, ((Entry) headSetIterator.next()).getValue());
            assertEquals(198, ((Entry) headSetIterator.next()).getValue());
        }
    }

    public void test_DescendingSubMapEntrySet_tailSet() {
        Set entrySet, tailSet;
        NavigableSet descendingSubMapEntrySet;
        Iterator iterator, tailSetIterator;
        Entry entry;
        int value;

        entrySet = navigableMap_startExcluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            iterator = descendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                tailSet = descendingSubMapEntrySet.tailSet(entry);
                tailSetIterator = tailSet.iterator();
                for (value = (Integer) entry.getValue(); tailSetIterator
                        .hasNext(); value--) {
                    assertEquals(value, ((Entry) tailSetIterator.next())
                            .getValue());
                }
                try {
                    tailSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                tailSet = descendingSubMapEntrySet.tailSet(entry, false);
                tailSetIterator = tailSet.iterator();
                for (value = (Integer) entry.getValue(); tailSetIterator
                        .hasNext(); value--) {
                    assertEquals(value - 1, ((Entry) tailSetIterator.next())
                            .getValue());
                }
                try {
                    tailSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                tailSet = descendingSubMapEntrySet.tailSet(entry, true);
                tailSetIterator = tailSet.iterator();
                for (value = (Integer) entry.getValue(); tailSetIterator
                        .hasNext(); value--) {
                    assertEquals(value, ((Entry) tailSetIterator.next())
                            .getValue());
                }
                try {
                    tailSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }
            }
        }

        entrySet = navigableMap_startExcluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            iterator = descendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                tailSet = descendingSubMapEntrySet.tailSet(entry);
                tailSetIterator = tailSet.iterator();
                for (value = (Integer) entry.getValue(); tailSetIterator
                        .hasNext(); value--) {
                    assertEquals(value, ((Entry) tailSetIterator.next())
                            .getValue());
                }
                try {
                    tailSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                tailSet = descendingSubMapEntrySet.tailSet(entry, false);
                tailSetIterator = tailSet.iterator();
                for (value = (Integer) entry.getValue(); tailSetIterator
                        .hasNext(); value--) {
                    assertEquals(value - 1, ((Entry) tailSetIterator.next())
                            .getValue());
                }
                try {
                    tailSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                tailSet = descendingSubMapEntrySet.tailSet(entry, true);
                tailSetIterator = tailSet.iterator();
                for (value = (Integer) entry.getValue(); tailSetIterator
                        .hasNext(); value--) {
                    assertEquals(value, ((Entry) tailSetIterator.next())
                            .getValue());
                }
                try {
                    tailSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }
            }
        }

        entrySet = navigableMap_startIncluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            iterator = descendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                tailSet = descendingSubMapEntrySet.tailSet(entry);
                tailSetIterator = tailSet.iterator();
                for (value = (Integer) entry.getValue(); tailSetIterator
                        .hasNext(); value--) {
                    assertEquals(value, ((Entry) tailSetIterator.next())
                            .getValue());
                }
                try {
                    tailSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                tailSet = descendingSubMapEntrySet.tailSet(entry, false);
                tailSetIterator = tailSet.iterator();
                for (value = (Integer) entry.getValue(); tailSetIterator
                        .hasNext(); value--) {
                    assertEquals(value - 1, ((Entry) tailSetIterator.next())
                            .getValue());
                }
                try {
                    tailSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                tailSet = descendingSubMapEntrySet.tailSet(entry, true);
                tailSetIterator = tailSet.iterator();
                for (value = (Integer) entry.getValue(); tailSetIterator
                        .hasNext(); value--) {
                    assertEquals(value, ((Entry) tailSetIterator.next())
                            .getValue());
                }
                try {
                    tailSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }
            }
        }

        entrySet = navigableMap_startIncluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            iterator = descendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                tailSet = descendingSubMapEntrySet.tailSet(entry);
                tailSetIterator = tailSet.iterator();
                for (value = (Integer) entry.getValue(); tailSetIterator
                        .hasNext(); value--) {
                    assertEquals(value, ((Entry) tailSetIterator.next())
                            .getValue());
                }
                try {
                    tailSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                tailSet = descendingSubMapEntrySet.tailSet(entry, false);
                tailSetIterator = tailSet.iterator();
                for (value = (Integer) entry.getValue(); tailSetIterator
                        .hasNext(); value--) {
                    assertEquals(value - 1, ((Entry) tailSetIterator.next())
                            .getValue());
                }
                try {
                    tailSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                tailSet = descendingSubMapEntrySet.tailSet(entry, true);
                tailSetIterator = tailSet.iterator();
                for (value = (Integer) entry.getValue(); tailSetIterator
                        .hasNext(); value--) {
                    assertEquals(value, ((Entry) tailSetIterator.next())
                            .getValue());
                }
                try {
                    tailSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }
            }
        }

        String endKey = new Integer(2).toString();
        entrySet = tm.headMap(endKey, true).entrySet();// 0...2
        if (entrySet instanceof NavigableSet) {
            // [2...0]
            descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            iterator = descendingSubMapEntrySet.iterator();
            iterator.next();// 2
            entry = (Entry) iterator.next();// 199
            tailSet = descendingSubMapEntrySet.tailSet(entry);
            tailSetIterator = tailSet.iterator();
            assertEquals(199, ((Entry) tailSetIterator.next()).getValue());

            tailSet = descendingSubMapEntrySet.tailSet(entry, false);
            tailSetIterator = tailSet.iterator();
            assertEquals(198, ((Entry) tailSetIterator.next()).getValue());

            tailSet = descendingSubMapEntrySet.tailSet(entry, true);
            tailSetIterator = tailSet.iterator();
            assertEquals(199, ((Entry) tailSetIterator.next()).getValue());
        }
    }

    public void test_DescendingSubMapEntrySet_subSet() {
        Set entrySet, subSet;
        NavigableSet descendingSubMapEntrySet;
        Entry startEntry, endEntry;
        Iterator subSetIterator;

        entrySet = navigableMap_startExcluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            Iterator iteratorStart = descendingSubMapEntrySet.iterator();
            while (iteratorStart.hasNext()) {
                startEntry = (Entry) iteratorStart.next();
                Iterator iteratorEnd = descendingSubMapEntrySet.iterator();
                while (iteratorEnd.hasNext()) {
                    endEntry = (Entry) iteratorEnd.next();
                    int startIndex = (Integer) startEntry.getValue();
                    int endIndex = (Integer) endEntry.getValue();
                    if (startIndex < endIndex) {
                        try {
                            descendingSubMapEntrySet.subSet(startEntry,
                                    endEntry);
                            fail("should throw IllegalArgumentException");
                        } catch (IllegalArgumentException e) {
                            // Expected
                        }

                        try {
                            descendingSubMapEntrySet.subSet(startEntry, false,
                                    endEntry, false);
                            fail("should throw IllegalArgumentException");
                        } catch (IllegalArgumentException e) {
                            // Expected
                        }

                        try {
                            descendingSubMapEntrySet.subSet(startEntry, false,
                                    endEntry, true);
                            fail("should throw IllegalArgumentException");
                        } catch (IllegalArgumentException e) {
                            // Expected
                        }

                        try {
                            descendingSubMapEntrySet.subSet(startEntry, true,
                                    endEntry, false);
                            fail("should throw IllegalArgumentException");
                        } catch (IllegalArgumentException e) {
                            // Expected
                        }

                        try {
                            descendingSubMapEntrySet.subSet(startEntry, true,
                                    endEntry, true);
                            fail("should throw IllegalArgumentException");
                        } catch (IllegalArgumentException e) {
                            // Expected
                        }
                    } else {
                        subSet = descendingSubMapEntrySet.subSet(startEntry,
                                endEntry);
                        subSetIterator = subSet.iterator();
                        for (int index = startIndex; subSetIterator.hasNext(); index--) {
                            assertEquals(index, ((Entry) subSetIterator.next())
                                    .getValue());
                        }

                        subSet = descendingSubMapEntrySet.subSet(startEntry,
                                false, endEntry, false);
                        subSetIterator = subSet.iterator();
                        for (int index = startIndex - 1; subSetIterator
                                .hasNext(); index--) {
                            assertEquals(index, ((Entry) subSetIterator.next())
                                    .getValue());
                        }

                        subSet = descendingSubMapEntrySet.subSet(startEntry,
                                false, endEntry, true);
                        subSetIterator = subSet.iterator();
                        for (int index = startIndex - 1; subSetIterator
                                .hasNext(); index--) {
                            assertEquals(index, ((Entry) subSetIterator.next())
                                    .getValue());
                        }

                        subSet = descendingSubMapEntrySet.subSet(startEntry,
                                true, endEntry, false);
                        subSetIterator = subSet.iterator();
                        for (int index = startIndex; subSetIterator.hasNext(); index--) {
                            assertEquals(index, ((Entry) subSetIterator.next())
                                    .getValue());
                        }

                        subSet = descendingSubMapEntrySet.subSet(startEntry,
                                true, endEntry, true);
                        subSetIterator = subSet.iterator();
                        for (int index = startIndex; subSetIterator.hasNext(); index--) {
                            assertEquals(index, ((Entry) subSetIterator.next())
                                    .getValue());
                        }
                    }
                }
            }
        }

        String endKey = new Integer(2).toString();
        entrySet = tm.headMap(endKey, true).entrySet();
        if (entrySet instanceof NavigableSet) {
            // [2...0]
            descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            Iterator iterator = descendingSubMapEntrySet.iterator();
            startEntry = (Entry) iterator.next();
            iterator.next();
            endEntry = (Entry) iterator.next();
            subSet = descendingSubMapEntrySet.subSet(startEntry, endEntry);
            assertEquals(2, subSet.size());

            subSet = descendingSubMapEntrySet.subSet(startEntry, false,
                    endEntry, false);
            assertEquals(1, subSet.size());
            subSetIterator = subSet.iterator();
            assertEquals(199, ((Entry) subSetIterator.next()).getValue());

            subSet = descendingSubMapEntrySet.subSet(startEntry, false,
                    endEntry, true);
            assertEquals(2, subSet.size());
            subSetIterator = subSet.iterator();
            assertEquals(199, ((Entry) subSetIterator.next()).getValue());
            assertEquals(198, ((Entry) subSetIterator.next()).getValue());

            subSet = descendingSubMapEntrySet.subSet(startEntry, true,
                    endEntry, false);
            assertEquals(2, subSet.size());
            subSetIterator = subSet.iterator();
            assertEquals(2, ((Entry) subSetIterator.next()).getValue());
            assertEquals(199, ((Entry) subSetIterator.next()).getValue());

            subSet = descendingSubMapEntrySet.subSet(startEntry, true,
                    endEntry, true);
            assertEquals(3, subSet.size());
            subSetIterator = subSet.iterator();
            assertEquals(2, ((Entry) subSetIterator.next()).getValue());
            assertEquals(199, ((Entry) subSetIterator.next()).getValue());
            assertEquals(198, ((Entry) subSetIterator.next()).getValue());
        }

        // With Comnparator
        entrySet = subMap_startExcluded_endExcluded_comparator.entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            Iterator iteratorStart = descendingSubMapEntrySet.iterator();
            while (iteratorStart.hasNext()) {
                startEntry = (Entry) iteratorStart.next();
                Iterator iteratorEnd = descendingSubMapEntrySet.iterator();
                while (iteratorEnd.hasNext()) {
                    endEntry = (Entry) iteratorEnd.next();
                    int startIndex = (Integer) startEntry.getValue();
                    int endIndex = (Integer) endEntry.getValue();
                    if (startIndex < endIndex) {
                        try {
                            descendingSubMapEntrySet.subSet(startEntry,
                                    endEntry);
                            fail("should throw IllegalArgumentException");
                        } catch (IllegalArgumentException e) {
                            // Expected
                        }

                        try {
                            descendingSubMapEntrySet.subSet(startEntry, false,
                                    endEntry, false);
                            fail("should throw IllegalArgumentException");
                        } catch (IllegalArgumentException e) {
                            // Expected
                        }

                        try {
                            descendingSubMapEntrySet.subSet(startEntry, false,
                                    endEntry, true);
                            fail("should throw IllegalArgumentException");
                        } catch (IllegalArgumentException e) {
                            // Expected
                        }

                        try {
                            descendingSubMapEntrySet.subSet(startEntry, true,
                                    endEntry, false);
                            fail("should throw IllegalArgumentException");
                        } catch (IllegalArgumentException e) {
                            // Expected
                        }

                        try {
                            descendingSubMapEntrySet.subSet(startEntry, true,
                                    endEntry, true);
                            fail("should throw IllegalArgumentException");
                        } catch (IllegalArgumentException e) {
                            // Expected
                        }
                    } else {
                        subSet = descendingSubMapEntrySet.subSet(startEntry,
                                endEntry);
                        subSetIterator = subSet.iterator();
                        for (int index = startIndex; subSetIterator.hasNext(); index--) {
                            assertEquals(index, ((Entry) subSetIterator.next())
                                    .getValue());
                        }

                        subSet = descendingSubMapEntrySet.subSet(startEntry,
                                false, endEntry, false);
                        subSetIterator = subSet.iterator();
                        for (int index = startIndex - 1; subSetIterator
                                .hasNext(); index--) {
                            assertEquals(index, ((Entry) subSetIterator.next())
                                    .getValue());
                        }

                        subSet = descendingSubMapEntrySet.subSet(startEntry,
                                false, endEntry, true);
                        subSetIterator = subSet.iterator();
                        for (int index = startIndex - 1; subSetIterator
                                .hasNext(); index--) {
                            assertEquals(index, ((Entry) subSetIterator.next())
                                    .getValue());
                        }

                        subSet = descendingSubMapEntrySet.subSet(startEntry,
                                true, endEntry, false);
                        subSetIterator = subSet.iterator();
                        for (int index = startIndex; subSetIterator.hasNext(); index--) {
                            assertEquals(index, ((Entry) subSetIterator.next())
                                    .getValue());
                        }

                        subSet = descendingSubMapEntrySet.subSet(startEntry,
                                true, endEntry, true);
                        subSetIterator = subSet.iterator();
                        for (int index = startIndex; subSetIterator.hasNext(); index--) {
                            assertEquals(index, ((Entry) subSetIterator.next())
                                    .getValue());
                        }
                    }
                }
            }
        }
    }

    public void test_DescendingSubMapEntrySet_lower() {
        Set entrySet, subSet;
        NavigableSet descendingSubMapEntrySet;
        Iterator iterator;
        Entry entry, lowerEntry;
        int value;

        entrySet = navigableMap_startExcluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            iterator = descendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                lowerEntry = (Entry) descendingSubMapEntrySet.lower(entry);
                value = (Integer) entry.getValue();
                if (value < 108) {
                    assertEquals(value + 1, lowerEntry.getValue());
                } else {
                    assertNull(lowerEntry);
                }
            }

            // System.out.println(descendingSubMapEntrySet);
            // System.out.println(tm);
            Object afterEnd = this.subMap_default_afterEnd_109.entrySet()
                    .iterator().next();
            // System.out.println("o:" + afterEnd);
            Object x = descendingSubMapEntrySet.lower(afterEnd);
            // System.out.println("x:" + x);
            assertNull(x);
            Object beforeStart = this.subMap_default_beforeStart_100.entrySet()
                    .iterator().next();
            // System.out.println("before: " + beforeStart);
            Object y = descendingSubMapEntrySet.lower(beforeStart);
            // System.out.println("y: " + y);
            assertNotNull(y);
            assertEquals(101, (((Entry) y).getValue()));
        }

        entrySet = navigableMap_startExcluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            iterator = descendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                lowerEntry = (Entry) descendingSubMapEntrySet.lower(entry);
                value = (Integer) entry.getValue();
                if (value < 109) {
                    assertEquals(value + 1, lowerEntry.getValue());
                } else {
                    assertNull(lowerEntry);
                }
            }
        }

        entrySet = navigableMap_startIncluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            iterator = descendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                lowerEntry = (Entry) descendingSubMapEntrySet.lower(entry);
                value = (Integer) entry.getValue();
                if (value < 108) {
                    assertEquals(value + 1, lowerEntry.getValue());
                } else {
                    assertNull(lowerEntry);
                }
            }
        }

        entrySet = navigableMap_startIncluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            iterator = descendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                lowerEntry = (Entry) descendingSubMapEntrySet.lower(entry);
                value = (Integer) entry.getValue();
                if (value < 109) {
                    assertEquals(value + 1, lowerEntry.getValue());
                } else {
                    assertNull(lowerEntry);
                }
            }
        }

        String endKey = new Integer(2).toString();
        entrySet = tm.headMap(endKey, true).entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            iterator = descendingSubMapEntrySet.iterator();
            iterator.next();// 2
            iterator.next();// 199
            entry = (Entry) iterator.next();// 198
            lowerEntry = (Entry) descendingSubMapEntrySet.lower(entry);
            assertEquals(199, lowerEntry.getValue());
        }
    }

    public void test_DescendingSubMapEntrySet_higher() {
        Set entrySet, subSet;
        NavigableSet descendingSubMapEntrySet;
        Iterator iterator;
        Entry entry, higherEntry;
        int value;

        entrySet = navigableMap_startExcluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            iterator = descendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                higherEntry = (Entry) descendingSubMapEntrySet.higher(entry);
                value = (Integer) entry.getValue();
                if (value > 101) {
                    assertEquals(value - 1, higherEntry.getValue());
                } else {
                    assertNull(higherEntry);
                }
            }

            Object afterEnd = this.subMap_default_afterEnd_109.entrySet()
                    .iterator().next();
            Object x = descendingSubMapEntrySet.higher(afterEnd);
            assertNotNull(x);
            assertEquals(108, ((Entry) x).getValue());
            Object beforeStart = this.subMap_default_beforeStart_100.entrySet()
                    .iterator().next();
            Object y = descendingSubMapEntrySet.higher(beforeStart);
            assertNull(y);
        }

        entrySet = navigableMap_startExcluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            iterator = descendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                higherEntry = (Entry) descendingSubMapEntrySet.higher(entry);
                value = (Integer) entry.getValue();
                if (value > 101) {
                    assertEquals(value - 1, higherEntry.getValue());
                } else {
                    assertNull(higherEntry);
                }
            }
        }

        entrySet = navigableMap_startIncluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            iterator = descendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                higherEntry = (Entry) descendingSubMapEntrySet.higher(entry);
                value = (Integer) entry.getValue();
                if (value > 100) {
                    assertEquals(value - 1, higherEntry.getValue());
                } else {
                    assertNull(higherEntry);
                }
            }
        }

        entrySet = navigableMap_startIncluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            iterator = descendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                higherEntry = (Entry) descendingSubMapEntrySet.higher(entry);
                value = (Integer) entry.getValue();
                if (value > 100) {
                    assertEquals(value - 1, higherEntry.getValue());
                } else {
                    assertNull(higherEntry);
                }
            }
        }

        String endKey = new Integer(2).toString();
        entrySet = tm.headMap(endKey, true).entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            iterator = descendingSubMapEntrySet.iterator();
            iterator.next();// 2
            iterator.next();// 199
            entry = (Entry) iterator.next();// 198
            higherEntry = (Entry) descendingSubMapEntrySet.higher(entry);
            assertEquals(197, higherEntry.getValue());
        }

        // With Comparator
        entrySet = subMap_startExcluded_endExcluded_comparator.entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSubMapEntrySet = ((NavigableSet) entrySet)
                    .descendingSet();
            iterator = descendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                higherEntry = (Entry) descendingSubMapEntrySet.higher(entry);
                value = (Integer) entry.getValue();
                if (value > 101) {
                    assertEquals(value - 1, higherEntry.getValue());
                } else {
                    assertNull(higherEntry);
                }
            }

            Object afterEnd = this.subMap_default_afterEnd_109.entrySet()
                    .iterator().next();
            Object x = descendingSubMapEntrySet.higher(afterEnd);
            assertNotNull(x);
            assertEquals(108, ((Entry) x).getValue());
            Object beforeStart = this.subMap_default_beforeStart_100.entrySet()
                    .iterator().next();
            Object y = descendingSubMapEntrySet.higher(beforeStart);
            assertNull(y);
        }
    }

    public void test_DescendingSubMapEntrySet_ceiling() {
        Set entrySet;
        NavigableSet ascendingSubMapEntrySet, descendingSet;
        Entry entry;
        Entry[] entryArray;

        entrySet = navigableMap_startExcluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            descendingSet = ascendingSubMapEntrySet.descendingSet();
            try {
                descendingSet.ceiling(null);
                fail("should throw NPE");
            } catch (NullPointerException e) {
                // Expected
            }

            entryArray = (Entry[]) descendingSet
                    .toArray(new Entry[descendingSet.size()]);
            for (int i = 0, j = 108; i < entryArray.length; i++) {
                entry = (Entry) descendingSet.ceiling(entryArray[i]);
                assertEquals(j - i, entry.getValue());
            }

            // System.out.println(descendingSet);
            // System.out.println(tm);
            Object afterEnd = this.subMap_default_afterEnd_109.entrySet()
                    .iterator().next();
            // System.out.println("o:" + afterEnd);//110
            Object x = descendingSet.ceiling(afterEnd);
            assertNotNull(x);
            // System.out.println("x:" + x);
            assertEquals(108, ((Entry) x).getValue());
            Object beforeStart = this.subMap_default_beforeStart_100.entrySet()
                    .iterator().next();
            // System.out.println("before: " + beforeStart);//0
            Object y = descendingSet.ceiling(beforeStart);
            assertNull(y);
        }

        entrySet = navigableMap_startExcluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            descendingSet = ascendingSubMapEntrySet.descendingSet();
            try {
                descendingSet.ceiling(null);
                fail("should throw NPE");
            } catch (NullPointerException e) {
                // Expected
            }

            entryArray = (Entry[]) descendingSet
                    .toArray(new Entry[descendingSet.size()]);
            for (int i = 0, j = 109; i < entryArray.length; i++) {
                entry = (Entry) descendingSet.ceiling(entryArray[i]);
                assertEquals(j - i, entry.getValue());
            }
        }

        entrySet = navigableMap_startIncluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            descendingSet = ascendingSubMapEntrySet.descendingSet();
            try {
                descendingSet.ceiling(null);
                fail("should throw NPE");
            } catch (NullPointerException e) {
                // Expected
            }

            entryArray = (Entry[]) descendingSet
                    .toArray(new Entry[descendingSet.size()]);
            for (int i = 0, j = 108; i < entryArray.length; i++) {
                entry = (Entry) descendingSet.ceiling(entryArray[i]);
                assertEquals(j - i, entry.getValue());
            }
        }

        entrySet = navigableMap_startIncluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSet = ((NavigableSet) entrySet).descendingSet();
            try {
                descendingSet.ceiling(null);
                fail("should throw NPE");
            } catch (NullPointerException e) {
                // Expected
            }

            entryArray = (Entry[]) descendingSet
                    .toArray(new Entry[descendingSet.size()]);
            for (int i = 0, j = 109; i < entryArray.length; i++) {
                entry = (Entry) descendingSet.ceiling(entryArray[i]);
                assertEquals(j - i, entry.getValue());
            }
        }

        String endKey = new Integer(2).toString();
        entrySet = tm.headMap(endKey, true).entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSet = ((NavigableSet) entrySet).descendingSet();
            try {
                descendingSet.ceiling(null);
                fail("should throw NPE");
            } catch (NullPointerException e) {
                // Expected
            }

            Iterator iterator = descendingSet.iterator();
            Entry ceilingEntry;
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                ceilingEntry = (Entry) descendingSet.ceiling(entry);
                assertEquals(entry, ceilingEntry);
            }
        }

    }

    public void test_DescendingSubMapEntrySet_floor() {
        Set entrySet;
        NavigableSet ascendingSubMapEntrySet, descendingSet;
        Entry entry;
        Entry[] entryArray;

        entrySet = navigableMap_startExcluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            descendingSet = ascendingSubMapEntrySet.descendingSet();
            try {
                descendingSet.floor(null);
                fail("should throw NPE");
            } catch (NullPointerException e) {
                // Expected
            }

            entryArray = (Entry[]) descendingSet
                    .toArray(new Entry[descendingSet.size()]);
            for (int i = 0, j = 108; i < entryArray.length; i++) {
                entry = (Entry) descendingSet.floor(entryArray[i]);
                assertEquals(j - i, entry.getValue());
            }

            Object afterEnd = this.subMap_default_afterEnd_109.entrySet()
                    .iterator().next();
            Object x = descendingSet.floor(afterEnd);
            assertNull(x);

            Object beforeStart = this.subMap_default_beforeStart_100.entrySet()
                    .iterator().next();
            Object y = descendingSet.floor(beforeStart);
            assertNotNull(y);
            assertEquals(101, (((Entry) y).getValue()));
        }

        entrySet = navigableMap_startExcluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            descendingSet = ascendingSubMapEntrySet.descendingSet();
            try {
                descendingSet.floor(null);
                fail("should throw NPE");
            } catch (NullPointerException e) {
                // Expected
            }

            entryArray = (Entry[]) descendingSet
                    .toArray(new Entry[descendingSet.size()]);
            for (int i = 0, j = 109; i < entryArray.length; i++) {
                entry = (Entry) descendingSet.floor(entryArray[i]);
                assertEquals(j - i, entry.getValue());
            }
        }

        entrySet = navigableMap_startIncluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            descendingSet = ascendingSubMapEntrySet.descendingSet();
            try {
                descendingSet.floor(null);
                fail("should throw NPE");
            } catch (NullPointerException e) {
                // Expected
            }

            entryArray = (Entry[]) descendingSet
                    .toArray(new Entry[descendingSet.size()]);
            for (int i = 0, j = 108; i < entryArray.length; i++) {
                entry = (Entry) descendingSet.floor(entryArray[i]);
                assertEquals(j - i, entry.getValue());
            }
        }

        entrySet = navigableMap_startIncluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSet = ((NavigableSet) entrySet).descendingSet();
            try {
                descendingSet.floor(null);
                fail("should throw NPE");
            } catch (NullPointerException e) {
                // Expected
            }

            entryArray = (Entry[]) descendingSet
                    .toArray(new Entry[descendingSet.size()]);
            for (int i = 0, j = 109; i < entryArray.length; i++) {
                entry = (Entry) descendingSet.floor(entryArray[i]);
                assertEquals(j - i, entry.getValue());
            }
        }

        String endKey = new Integer(2).toString();
        entrySet = tm.headMap(endKey, true).entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSet = ((NavigableSet) entrySet).descendingSet();

            Iterator iterator = descendingSet.iterator();
            Entry floorEntry;
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                floorEntry = (Entry) descendingSet.floor(entry);
                assertEquals(entry, floorEntry);
            }
        }

        // With Comparator
        entrySet = subMap_startExcluded_endExcluded_comparator.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            descendingSet = ascendingSubMapEntrySet.descendingSet();
            try {
                descendingSet.floor(null);
                fail("should throw NPE");
            } catch (NullPointerException e) {
                // Expected
            }

            entryArray = (Entry[]) descendingSet
                    .toArray(new Entry[descendingSet.size()]);
            for (int i = 0, j = 108; i < entryArray.length; i++) {
                entry = (Entry) descendingSet.floor(entryArray[i]);
                assertEquals(j - i, entry.getValue());
            }
        }

        entrySet = subMap_startExcluded_endIncluded_comparator.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            descendingSet = ascendingSubMapEntrySet.descendingSet();
            try {
                descendingSet.floor(null);
                fail("should throw NPE");
            } catch (NullPointerException e) {
                // Expected
            }

            entryArray = (Entry[]) descendingSet
                    .toArray(new Entry[descendingSet.size()]);
            for (int i = 0, j = 109; i < entryArray.length; i++) {
                entry = (Entry) descendingSet.floor(entryArray[i]);
                assertEquals(j - i, entry.getValue());
            }
        }

        entrySet = subMap_startIncluded_endExcluded_comparator.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            descendingSet = ascendingSubMapEntrySet.descendingSet();
            try {
                descendingSet.floor(null);
                fail("should throw NPE");
            } catch (NullPointerException e) {
                // Expected
            }

            entryArray = (Entry[]) descendingSet
                    .toArray(new Entry[descendingSet.size()]);
            for (int i = 0, j = 108; i < entryArray.length; i++) {
                entry = (Entry) descendingSet.floor(entryArray[i]);
                assertEquals(j - i, entry.getValue());
            }
        }

        entrySet = subMap_startIncluded_endIncluded_comparator.entrySet();
        if (entrySet instanceof NavigableSet) {
            descendingSet = ((NavigableSet) entrySet).descendingSet();
            try {
                descendingSet.floor(null);
                fail("should throw NPE");
            } catch (NullPointerException e) {
                // Expected
            }

            entryArray = (Entry[]) descendingSet
                    .toArray(new Entry[descendingSet.size()]);
            for (int i = 0, j = 109; i < entryArray.length; i++) {
                entry = (Entry) descendingSet.floor(entryArray[i]);
                assertEquals(j - i, entry.getValue());
            }
        }
    }

    public void test_DescendingSubMapKeySet_comparator() {
        NavigableSet keySet, descendingKeySet;
        Comparator comparator;
        String[] keyArray;
        Integer value1, value2;

        keySet = navigableMap_startExcluded_endExcluded.navigableKeySet();
        assertNull(keySet.comparator());
        descendingKeySet = keySet.descendingSet();
        comparator = descendingKeySet.comparator();
        assertNotNull(comparator);
        keyArray = (String[]) descendingKeySet
                .toArray(new String[descendingKeySet.size()]);
        for (int i = 1; i < keyArray.length; i++) {
            value1 = Integer.valueOf(keyArray[i - 1]);
            value2 = Integer.valueOf(keyArray[i]);
            assertTrue(value1 > value2);
            assertTrue(comparator.compare(value1, value2) < 0);
        }

        keySet = navigableMap_startExcluded_endIncluded.navigableKeySet();
        assertNull(keySet.comparator());
        descendingKeySet = keySet.descendingSet();
        comparator = descendingKeySet.comparator();
        assertNotNull(comparator);
        keyArray = (String[]) descendingKeySet
                .toArray(new String[descendingKeySet.size()]);
        for (int i = 1; i < keyArray.length; i++) {
            value1 = Integer.valueOf(keyArray[i - 1]);
            value2 = Integer.valueOf(keyArray[i]);
            assertTrue(value1 > value2);
            assertTrue(comparator.compare(value1, value2) < 0);
        }

        keySet = navigableMap_startIncluded_endExcluded.navigableKeySet();
        assertNull(keySet.comparator());
        descendingKeySet = keySet.descendingSet();
        comparator = descendingKeySet.comparator();
        assertNotNull(comparator);
        keyArray = (String[]) descendingKeySet
                .toArray(new String[descendingKeySet.size()]);
        for (int i = 1; i < keyArray.length; i++) {
            value1 = Integer.valueOf(keyArray[i - 1]);
            value2 = Integer.valueOf(keyArray[i]);
            assertTrue(value1 > value2);
            assertTrue(comparator.compare(value1, value2) < 0);
        }

        keySet = navigableMap_startIncluded_endIncluded.navigableKeySet();
        assertNull(keySet.comparator());
        descendingKeySet = keySet.descendingSet();
        comparator = descendingKeySet.comparator();
        assertNotNull(comparator);
        keyArray = (String[]) descendingKeySet
                .toArray(new String[descendingKeySet.size()]);
        for (int i = 1; i < keyArray.length; i++) {
            value1 = Integer.valueOf(keyArray[i - 1]);
            value2 = Integer.valueOf(keyArray[i]);
            assertTrue(value1 > value2);
            assertTrue(comparator.compare(value1, value2) < 0);
        }

        String endKey = new Integer(2).toString();
        keySet = tm.headMap(endKey, true).navigableKeySet();
        assertNull(keySet.comparator());
        descendingKeySet = keySet.descendingSet();
        assertNotNull(descendingKeySet.comparator());
    }

    public void test_AscendingSubMapKeySet_first() {
        NavigableSet keySet;
        String firstKey1 = new Integer(100).toString();
        String firstKey2 = new Integer(101).toString();

        keySet = navigableMap_startExcluded_endExcluded.navigableKeySet();
        assertEquals(firstKey2, keySet.first());

        keySet = navigableMap_startExcluded_endIncluded.navigableKeySet();
        assertEquals(firstKey2, keySet.first());

        keySet = navigableMap_startIncluded_endExcluded.navigableKeySet();
        assertEquals(firstKey1, keySet.first());

        keySet = navigableMap_startIncluded_endIncluded.navigableKeySet();
        assertEquals(firstKey1, keySet.first());
    }

    public void test_DescendingSubMapKeySet_pollFirst_startExcluded_endExcluded() {
        NavigableSet keySet = navigableMap_startExcluded_endExcluded
                .navigableKeySet();
        NavigableSet descendingKeySet = keySet.descendingSet();
        Iterator iterator = descendingKeySet.iterator();
        assertEquals(8, keySet.size());
        for (int value = 101; value < 109; value++) {
            assertEquals(new Integer(value).toString(), keySet.pollFirst());
        }
        assertEquals(0, keySet.size());
        assertNull(keySet.pollLast());
    }

    public void test_DescendingSubMapKeySet_pollFirst_startExcluded_endIncluded() {
        NavigableSet keySet = navigableMap_startExcluded_endIncluded
                .navigableKeySet();
        NavigableSet descendingKeySet = keySet.descendingSet();
        Iterator iterator = descendingKeySet.iterator();
        assertEquals(9, keySet.size());
        for (int value = 101; value < 110; value++) {
            assertEquals(new Integer(value).toString(), keySet.pollFirst());
        }
        assertEquals(0, keySet.size());
        assertNull(keySet.pollLast());
    }

    public void test_DescendingSubMapKeySet_pollFirst_startIncluded_endExcluded() {
        NavigableSet keySet = navigableMap_startIncluded_endExcluded
                .navigableKeySet();
        NavigableSet descendingKeySet = keySet.descendingSet();
        Iterator iterator = descendingKeySet.iterator();
        assertEquals(9, keySet.size());
        for (int value = 100; value < 109; value++) {
            assertEquals(new Integer(value).toString(), keySet.pollFirst());
        }
        assertEquals(0, keySet.size());
        assertNull(keySet.pollLast());
    }

    public void test_DescendingSubMapKeySet_pollFirst_startIncluded_endIncluded() {
        NavigableSet keySet = navigableMap_startIncluded_endIncluded
                .navigableKeySet();
        NavigableSet descendingKeySet = keySet.descendingSet();
        Iterator iterator = descendingKeySet.iterator();
        assertEquals(10, keySet.size());
        for (int value = 100; value < 110; value++) {
            assertEquals(new Integer(value).toString(), keySet.pollFirst());
        }
        assertEquals(0, keySet.size());
        assertNull(keySet.pollLast());
    }

    public void test_DescendingSubMapKeySet_pollFirst() {
        String endKey = new Integer(2).toString();
        NavigableSet keySet = tm.headMap(endKey, true).navigableKeySet();
        NavigableSet descendingKeySet = keySet.descendingSet();
        assertEquals(endKey, descendingKeySet.pollFirst());
    }

    public void test_DescendingSubMapKeySet_pollLast_startExcluded_endExcluded() {
        NavigableSet keySet = navigableMap_startExcluded_endExcluded
                .navigableKeySet();
        NavigableSet descendingKeySet = keySet.descendingSet();
        Iterator iterator = descendingKeySet.iterator();
        assertEquals(8, keySet.size());
        for (int value = 108; value > 100; value--) {
            assertEquals(new Integer(value).toString(), keySet.pollLast());
        }
        assertEquals(0, keySet.size());
        assertNull(keySet.pollLast());
    }

    public void test_DescendingSubMapKeySet_pollLast_startExcluded_endIncluded() {
        NavigableSet keySet = navigableMap_startExcluded_endIncluded
                .navigableKeySet();
        NavigableSet descendingKeySet = keySet.descendingSet();
        Iterator iterator = descendingKeySet.iterator();
        assertEquals(9, keySet.size());
        for (int value = 109; value > 100; value--) {
            assertEquals(new Integer(value).toString(), keySet.pollLast());
        }
        assertEquals(0, keySet.size());
        assertNull(keySet.pollLast());
    }

    public void test_DescendingSubMapKeySet_pollLast_startIncluded_endExcluded() {
        NavigableSet keySet = navigableMap_startIncluded_endExcluded
                .navigableKeySet();
        NavigableSet descendingKeySet = keySet.descendingSet();
        Iterator iterator = descendingKeySet.iterator();
        assertEquals(9, keySet.size());
        for (int value = 108; value > 99; value--) {
            assertEquals(new Integer(value).toString(), keySet.pollLast());
        }
        assertEquals(0, keySet.size());
        assertNull(keySet.pollLast());
    }

    public void test_DescendingSubMapKeySet_pollLast_startIncluded_endIncluded() {
        NavigableSet keySet = navigableMap_startIncluded_endIncluded
                .navigableKeySet();
        NavigableSet descendingKeySet = keySet.descendingSet();
        Iterator iterator = descendingKeySet.iterator();
        assertEquals(10, keySet.size());
        for (int value = 109; value > 99; value--) {
            assertEquals(new Integer(value).toString(), keySet.pollLast());
        }
        assertEquals(0, keySet.size());
        assertNull(keySet.pollLast());
    }

    public void test_DescendingSubMapKeySet_pollLast() {
        String endKey = new Integer(2).toString();
        NavigableSet keySet = tm.headMap(endKey, true).navigableKeySet();
        NavigableSet descendingKeySet = keySet.descendingSet();
        assertEquals(new Integer(0).toString(), descendingKeySet.pollLast());
    }

    public void test_DescendingSubMapKeySet_headSet() {
        NavigableSet keySet, descendingKeySet;
        SortedSet headSet;
        String endKey, key;
        Iterator iterator;
        int index;

        keySet = navigableMap_startExcluded_endExcluded.navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        endKey = new Integer(99).toString();
        try {
            descendingKeySet.headSet(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.headSet(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(100).toString();
        headSet = descendingKeySet.headSet(endKey);
        iterator = headSet.iterator();
        for (index = 108; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(100, index);

        headSet = descendingKeySet.headSet(endKey, false);
        iterator = headSet.iterator();
        for (index = 108; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(100, index);

        try {
            descendingKeySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(101).toString();
        headSet = descendingKeySet.headSet(endKey);
        iterator = headSet.iterator();
        for (index = 108; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(101, index);

        headSet = descendingKeySet.headSet(endKey, false);
        iterator = headSet.iterator();
        for (index = 108; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(101, index);

        headSet = descendingKeySet.headSet(endKey, true);
        iterator = headSet.iterator();
        for (index = 108; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(100, index);

        for (int i = 102; i < 109; i++) {
            endKey = new Integer(i).toString();
            headSet = descendingKeySet.headSet(endKey);
            iterator = headSet.iterator();
            int j;
            for (j = 108; iterator.hasNext(); j--) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i, j);

            headSet = descendingKeySet.headSet(endKey, false);
            iterator = headSet.iterator();
            for (j = 108; iterator.hasNext(); j--) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i, j);

            headSet = descendingKeySet.headSet(endKey, true);
            iterator = headSet.iterator();
            for (j = 108; iterator.hasNext(); j--) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i - 1, j);
        }

        endKey = new Integer(109).toString();
        headSet = descendingKeySet.headSet(endKey);
        iterator = headSet.iterator();
        for (index = 108; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(108, index);

        headSet = descendingKeySet.headSet(endKey, false);
        iterator = headSet.iterator();
        for (index = 108; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(108, index);

        try {
            descendingKeySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(110).toString();
        try {
            descendingKeySet.headSet(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.headSet(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        keySet = navigableMap_startExcluded_endIncluded.navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        endKey = new Integer(99).toString();
        try {
            descendingKeySet.headSet(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.headSet(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(100).toString();
        headSet = descendingKeySet.headSet(endKey);
        iterator = headSet.iterator();
        for (index = 109; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(100, index);

        headSet = descendingKeySet.headSet(endKey, false);
        iterator = headSet.iterator();
        for (index = 109; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(100, index);

        try {
            descendingKeySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(101).toString();
        headSet = descendingKeySet.headSet(endKey);
        iterator = headSet.iterator();
        for (index = 109; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(101, index);

        headSet = descendingKeySet.headSet(endKey, false);
        iterator = headSet.iterator();
        for (index = 109; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(101, index);

        headSet = descendingKeySet.headSet(endKey, true);
        iterator = headSet.iterator();
        for (index = 109; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(100, index);

        for (int i = 102; i < 109; i++) {
            endKey = new Integer(i).toString();
            headSet = descendingKeySet.headSet(endKey);
            iterator = headSet.iterator();
            int j;
            for (j = 109; iterator.hasNext(); j--) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i, j);

            headSet = descendingKeySet.headSet(endKey, false);
            iterator = headSet.iterator();
            for (j = 109; iterator.hasNext(); j--) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i, j);

            headSet = descendingKeySet.headSet(endKey, true);
            iterator = headSet.iterator();
            for (j = 109; iterator.hasNext(); j--) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i - 1, j);
        }

        endKey = new Integer(109).toString();
        headSet = descendingKeySet.headSet(endKey);
        iterator = headSet.iterator();
        for (index = 109; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(109, index);

        headSet = descendingKeySet.headSet(endKey, false);
        iterator = headSet.iterator();
        for (index = 109; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(109, index);

        headSet = descendingKeySet.headSet(endKey, true);
        iterator = headSet.iterator();
        for (index = 109; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(108, index);

        endKey = new Integer(110).toString();
        try {
            descendingKeySet.headSet(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.headSet(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        keySet = navigableMap_startIncluded_endExcluded.navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        endKey = new Integer(99).toString();
        try {
            descendingKeySet.headSet(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.headSet(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(100).toString();
        headSet = descendingKeySet.headSet(endKey);
        iterator = headSet.iterator();
        for (index = 108; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(100, index);

        headSet = descendingKeySet.headSet(endKey, false);
        iterator = headSet.iterator();
        for (index = 108; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(100, index);

        headSet = descendingKeySet.headSet(endKey, true);
        iterator = headSet.iterator();
        for (index = 108; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(99, index);

        endKey = new Integer(101).toString();
        headSet = descendingKeySet.headSet(endKey);
        iterator = headSet.iterator();
        for (index = 108; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(101, index);

        headSet = descendingKeySet.headSet(endKey, false);
        iterator = headSet.iterator();
        for (index = 108; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(101, index);

        headSet = descendingKeySet.headSet(endKey, true);
        iterator = headSet.iterator();
        for (index = 108; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(100, index);

        for (int i = 102; i < 109; i++) {
            endKey = new Integer(i).toString();
            headSet = descendingKeySet.headSet(endKey);
            iterator = headSet.iterator();
            int j;
            for (j = 108; iterator.hasNext(); j--) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i, j);

            headSet = descendingKeySet.headSet(endKey, false);
            iterator = headSet.iterator();
            for (j = 108; iterator.hasNext(); j--) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i, j);

            headSet = descendingKeySet.headSet(endKey, true);
            iterator = headSet.iterator();
            for (j = 108; iterator.hasNext(); j--) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i - 1, j);
        }

        endKey = new Integer(109).toString();
        headSet = descendingKeySet.headSet(endKey);
        iterator = headSet.iterator();
        for (index = 108; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(108, index);

        headSet = descendingKeySet.headSet(endKey, false);
        iterator = headSet.iterator();
        for (index = 108; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(108, index);

        try {
            descendingKeySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(110).toString();
        try {
            descendingKeySet.headSet(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.headSet(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        keySet = navigableMap_startIncluded_endIncluded.navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        endKey = new Integer(99).toString();
        try {
            descendingKeySet.headSet(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.headSet(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(100).toString();
        headSet = descendingKeySet.headSet(endKey);
        iterator = headSet.iterator();
        for (index = 109; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(100, index);

        headSet = descendingKeySet.headSet(endKey, false);
        iterator = headSet.iterator();
        for (index = 109; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(100, index);

        headSet = descendingKeySet.headSet(endKey, true);
        iterator = headSet.iterator();
        for (index = 109; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(99, index);

        endKey = new Integer(101).toString();
        headSet = descendingKeySet.headSet(endKey);
        iterator = headSet.iterator();
        for (index = 109; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(101, index);

        headSet = descendingKeySet.headSet(endKey, false);
        iterator = headSet.iterator();
        for (index = 109; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(101, index);

        headSet = descendingKeySet.headSet(endKey, true);
        iterator = headSet.iterator();
        for (index = 109; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(100, index);

        for (int i = 102; i < 109; i++) {
            endKey = new Integer(i).toString();
            headSet = descendingKeySet.headSet(endKey);
            iterator = headSet.iterator();
            int j;
            for (j = 109; iterator.hasNext(); j--) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i, j);

            headSet = descendingKeySet.headSet(endKey, false);
            iterator = headSet.iterator();
            for (j = 109; iterator.hasNext(); j--) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i, j);

            headSet = descendingKeySet.headSet(endKey, true);
            iterator = headSet.iterator();
            for (j = 109; iterator.hasNext(); j--) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i - 1, j);
        }

        endKey = new Integer(109).toString();
        headSet = descendingKeySet.headSet(endKey);
        iterator = headSet.iterator();
        for (index = 109; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(109, index);

        headSet = descendingKeySet.headSet(endKey, false);
        iterator = headSet.iterator();
        for (index = 109; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(109, index);

        headSet = descendingKeySet.headSet(endKey, true);
        iterator = headSet.iterator();
        for (index = 109; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(108, index);

        endKey = new Integer(110).toString();
        try {
            descendingKeySet.headSet(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.headSet(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        key = new Integer(2).toString();
        keySet = tm.headMap(key, true).navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        iterator = descendingKeySet.iterator();
        iterator.next();
        endKey = (String) iterator.next();

        headSet = descendingKeySet.headSet(endKey);
        assertEquals(1, headSet.size());

        headSet = descendingKeySet.headSet(endKey, false);
        assertEquals(1, headSet.size());

        headSet = descendingKeySet.headSet(endKey, true);
        assertEquals(2, headSet.size());

        key = new Integer(2).toString();
        keySet = tm.tailMap(key, true).navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        iterator = descendingKeySet.iterator();
        iterator.next();
        endKey = (String) iterator.next();
        headSet = descendingKeySet.headSet(endKey);
        assertEquals(1, headSet.size());
        iterator = headSet.iterator();
        assertEquals(999, Integer.parseInt((String) iterator.next()));
    }

    public void test_DescendingSubMapKeySet_tailSet() {
        NavigableSet keySet, descendingKeySet;
        SortedSet tailSet;
        String startKey, key;
        Iterator iterator;
        int index;

        keySet = navigableMap_startExcluded_endExcluded.navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        startKey = new Integer(99).toString();
        try {
            descendingKeySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            descendingKeySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            descendingKeySet.tailSet(startKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        startKey = new Integer(100).toString();
        try {
            descendingKeySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        tailSet = descendingKeySet.tailSet(startKey, false);
        assertEquals(0, tailSet.size());

        startKey = new Integer(101).toString();
        tailSet = descendingKeySet.tailSet(startKey);
        iterator = tailSet.iterator();
        for (index = 101; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(100, index);

        tailSet = descendingKeySet.tailSet(startKey, true);
        iterator = tailSet.iterator();
        for (index = 101; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(100, index);

        tailSet = descendingKeySet.tailSet(startKey, false);
        iterator = tailSet.iterator();
        for (index = 101; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(101, index);

        for (int i = 102; i < 109; i++) {
            startKey = new Integer(i).toString();

            tailSet = descendingKeySet.tailSet(startKey);
            iterator = tailSet.iterator();
            int j;
            for (j = i; iterator.hasNext(); j--) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(100, j);

            tailSet = descendingKeySet.tailSet(startKey, true);
            iterator = tailSet.iterator();
            for (j = i; iterator.hasNext(); j--) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(100, j);

            tailSet = descendingKeySet.tailSet(startKey, false);
            iterator = tailSet.iterator();
            for (j = i; iterator.hasNext(); j--) {
                key = (String) iterator.next();
                assertEquals(new Integer(j - 1).toString(), key);
            }
            assertEquals(101, j);
        }

        startKey = new Integer(109).toString();
        try {
            descendingKeySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        tailSet = descendingKeySet.tailSet(startKey, false);
        iterator = tailSet.iterator();
        for (index = 109; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index - 1).toString(), key);
        }
        assertEquals(101, index);

        startKey = new Integer(110).toString();
        try {
            descendingKeySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.tailSet(startKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        keySet = navigableMap_startExcluded_endIncluded.navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        startKey = new Integer(99).toString();
        try {
            descendingKeySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            descendingKeySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            descendingKeySet.tailSet(startKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        startKey = new Integer(100).toString();
        try {
            descendingKeySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        tailSet = descendingKeySet.tailSet(startKey, false);
        assertEquals(0, tailSet.size());

        startKey = new Integer(101).toString();
        tailSet = descendingKeySet.tailSet(startKey);
        iterator = tailSet.iterator();
        for (index = 101; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(100, index);

        tailSet = descendingKeySet.tailSet(startKey, true);
        iterator = tailSet.iterator();
        for (index = 101; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(100, index);

        tailSet = descendingKeySet.tailSet(startKey, false);
        iterator = tailSet.iterator();
        for (index = 101; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(101, index);

        for (int i = 102; i < 109; i++) {
            startKey = new Integer(i).toString();

            tailSet = descendingKeySet.tailSet(startKey);
            iterator = tailSet.iterator();
            int j;
            for (j = i; iterator.hasNext(); j--) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(100, j);

            tailSet = descendingKeySet.tailSet(startKey, true);
            iterator = tailSet.iterator();
            for (j = i; iterator.hasNext(); j--) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(100, j);

            tailSet = descendingKeySet.tailSet(startKey, false);
            iterator = tailSet.iterator();
            for (j = i; iterator.hasNext(); j--) {
                key = (String) iterator.next();
                assertEquals(new Integer(j - 1).toString(), key);
            }
            assertEquals(101, j);
        }

        startKey = new Integer(109).toString();
        tailSet = descendingKeySet.tailSet(startKey);
        iterator = tailSet.iterator();
        for (index = 109; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(100, index);

        tailSet = descendingKeySet.tailSet(startKey, true);
        iterator = tailSet.iterator();
        for (index = 109; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(100, index);

        tailSet = descendingKeySet.tailSet(startKey, false);
        iterator = tailSet.iterator();
        for (index = 109; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index - 1).toString(), key);
        }
        assertEquals(101, index);

        startKey = new Integer(110).toString();
        try {
            descendingKeySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.tailSet(startKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        keySet = navigableMap_startIncluded_endExcluded.navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        startKey = new Integer(99).toString();
        try {
            descendingKeySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.tailSet(startKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        startKey = new Integer(100).toString();
        tailSet = descendingKeySet.tailSet(startKey);
        assertEquals(1, tailSet.size());
        iterator = tailSet.iterator();
        assertEquals(startKey, iterator.next());

        tailSet = descendingKeySet.tailSet(startKey, true);
        assertEquals(1, tailSet.size());
        iterator = tailSet.iterator();
        assertEquals(startKey, iterator.next());

        tailSet = descendingKeySet.tailSet(startKey, false);
        assertEquals(0, tailSet.size());

        startKey = new Integer(101).toString();
        tailSet = descendingKeySet.tailSet(startKey);
        iterator = tailSet.iterator();
        for (index = 101; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(99, index);

        tailSet = descendingKeySet.tailSet(startKey, true);
        iterator = tailSet.iterator();
        for (index = 101; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(99, index);

        tailSet = descendingKeySet.tailSet(startKey, false);
        iterator = tailSet.iterator();
        for (index = 101; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index - 1).toString(), key);
        }
        assertEquals(100, index);

        for (int i = 102; i < 109; i++) {
            startKey = new Integer(i).toString();

            tailSet = descendingKeySet.tailSet(startKey);
            iterator = tailSet.iterator();
            int j;
            for (j = i; iterator.hasNext(); j--) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(99, j);

            tailSet = descendingKeySet.tailSet(startKey, true);
            iterator = tailSet.iterator();
            for (j = i; iterator.hasNext(); j--) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(99, j);

            tailSet = descendingKeySet.tailSet(startKey, false);
            iterator = tailSet.iterator();
            for (j = i; iterator.hasNext(); j--) {
                key = (String) iterator.next();
                assertEquals(new Integer(j - 1).toString(), key);
            }
            assertEquals(100, j);
        }

        startKey = new Integer(109).toString();
        try {
            descendingKeySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        tailSet = descendingKeySet.tailSet(startKey, false);
        iterator = tailSet.iterator();
        for (index = 109; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index - 1).toString(), key);
        }
        assertEquals(100, index);

        startKey = new Integer(110).toString();
        try {
            descendingKeySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.tailSet(startKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        keySet = navigableMap_startIncluded_endIncluded.navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        startKey = new Integer(99).toString();
        try {
            descendingKeySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.tailSet(startKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        startKey = new Integer(100).toString();
        tailSet = descendingKeySet.tailSet(startKey);
        assertEquals(1, tailSet.size());
        iterator = tailSet.iterator();
        assertEquals(startKey, iterator.next());

        tailSet = descendingKeySet.tailSet(startKey, true);
        assertEquals(1, tailSet.size());
        iterator = tailSet.iterator();
        assertEquals(startKey, iterator.next());

        tailSet = descendingKeySet.tailSet(startKey, false);
        assertEquals(0, tailSet.size());

        startKey = new Integer(101).toString();
        tailSet = descendingKeySet.tailSet(startKey);
        iterator = tailSet.iterator();
        for (index = 101; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(99, index);

        tailSet = descendingKeySet.tailSet(startKey, true);
        iterator = tailSet.iterator();
        for (index = 101; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(99, index);

        tailSet = descendingKeySet.tailSet(startKey, false);
        iterator = tailSet.iterator();
        for (index = 101; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index - 1).toString(), key);
        }
        assertEquals(100, index);

        for (int i = 102; i < 109; i++) {
            startKey = new Integer(i).toString();

            tailSet = descendingKeySet.tailSet(startKey);
            iterator = tailSet.iterator();
            int j;
            for (j = i; iterator.hasNext(); j--) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(99, j);

            tailSet = descendingKeySet.tailSet(startKey, true);
            iterator = tailSet.iterator();
            for (j = i; iterator.hasNext(); j--) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(99, j);

            tailSet = descendingKeySet.tailSet(startKey, false);
            iterator = tailSet.iterator();
            for (j = i; iterator.hasNext(); j--) {
                key = (String) iterator.next();
                assertEquals(new Integer(j - 1).toString(), key);
            }
            assertEquals(100, j);
        }

        startKey = new Integer(109).toString();
        tailSet = descendingKeySet.tailSet(startKey);
        iterator = tailSet.iterator();
        for (index = 109; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(99, index);

        tailSet = descendingKeySet.tailSet(startKey, true);
        iterator = tailSet.iterator();
        for (index = 109; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(99, index);

        tailSet = descendingKeySet.tailSet(startKey, false);
        iterator = tailSet.iterator();
        for (index = 109; iterator.hasNext(); index--) {
            key = (String) iterator.next();
            assertEquals(new Integer(index - 1).toString(), key);
        }
        assertEquals(100, index);

        startKey = new Integer(110).toString();
        try {
            descendingKeySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            descendingKeySet.tailSet(startKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        key = new Integer(2).toString();
        keySet = tm.headMap(key, true).navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        iterator = descendingKeySet.iterator();
        iterator.next();
        startKey = (String) iterator.next();

        tailSet = descendingKeySet.tailSet(startKey);
        assertEquals(112, tailSet.size());
        Iterator tailIterator = tailSet.iterator();
        assertEquals(new Integer(199).toString(), tailIterator.next());

        tailSet = descendingKeySet.tailSet(startKey, true);
        assertEquals(112, tailSet.size());
        tailIterator = tailSet.iterator();
        assertEquals(new Integer(199).toString(), tailIterator.next());

        tailSet = descendingKeySet.tailSet(startKey, false);
        assertEquals(111, tailSet.size());
        tailIterator = tailSet.iterator();
        assertEquals(new Integer(198).toString(), tailIterator.next());
    }

    public void test_DescendingSubMapKeySet_subSet() {
        NavigableSet keySet, descendingKeySet;
        SortedSet subSet;
        String startKey, endKey, key;
        Iterator startIterator, endIterator, subSetIterator;

        keySet = navigableMap_startExcluded_endExcluded.navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        startIterator = descendingKeySet.iterator();
        while (startIterator.hasNext()) {
            startKey = (String) startIterator.next();
            endIterator = descendingKeySet.iterator();
            while (endIterator.hasNext()) {
                endKey = (String) endIterator.next();
                int startIndex = Integer.valueOf(startKey);
                int endIndex = Integer.valueOf(endKey);
                if (startIndex < endIndex) {
                    try {
                        descendingKeySet.subSet(startKey, endKey);
                        fail("should throw IllegalArgumentException");
                    } catch (IllegalArgumentException e) {
                        // Expected
                    }

                    try {
                        descendingKeySet.subSet(startKey, false, endKey, false);
                        fail("shoudl throw IllegalArgumentException");
                    } catch (IllegalArgumentException e) {
                        // Expected
                    }

                    try {
                        descendingKeySet.subSet(startKey, false, endKey, true);
                        fail("shoudl throw IllegalArgumentException");
                    } catch (IllegalArgumentException e) {
                        // Expected
                    }

                    try {
                        descendingKeySet.subSet(startKey, true, endKey, false);
                        fail("shoudl throw IllegalArgumentException");
                    } catch (IllegalArgumentException e) {
                        // Expected
                    }

                    try {
                        descendingKeySet.subSet(startKey, true, endKey, true);
                        fail("shoudl throw IllegalArgumentException");
                    } catch (IllegalArgumentException e) {
                        // Expected
                    }
                } else {
                    subSet = descendingKeySet.subSet(startKey, endKey);
                    subSetIterator = subSet.iterator();
                    for (int index = startIndex; subSetIterator.hasNext(); index--) {
                        assertEquals(new Integer(index).toString(),
                                subSetIterator.next());
                    }

                    subSet = descendingKeySet.subSet(startKey, false, endKey,
                            false);
                    subSetIterator = subSet.iterator();
                    for (int index = startIndex - 1; subSetIterator.hasNext(); index--) {
                        assertEquals(new Integer(index).toString(),
                                subSetIterator.next());
                    }

                    subSet = descendingKeySet.subSet(startKey, false, endKey,
                            true);
                    subSetIterator = subSet.iterator();
                    for (int index = startIndex - 1; subSetIterator.hasNext(); index--) {
                        assertEquals(new Integer(index).toString(),
                                subSetIterator.next());
                    }

                    subSet = descendingKeySet.subSet(startKey, true, endKey,
                            false);
                    subSetIterator = subSet.iterator();
                    for (int index = startIndex; subSetIterator.hasNext(); index--) {
                        assertEquals(new Integer(index).toString(),
                                subSetIterator.next());
                    }

                    subSet = descendingKeySet.subSet(startKey, true, endKey,
                            true);
                    subSetIterator = subSet.iterator();
                    for (int index = startIndex; subSetIterator.hasNext(); index--) {
                        assertEquals(new Integer(index).toString(),
                                subSetIterator.next());
                    }
                }
            }
        }

        endKey = new Integer(2).toString();
        keySet = tm.headMap(endKey, true).navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        Iterator iterator = descendingKeySet.iterator();

        startKey = (String) iterator.next();
        iterator.next();
        endKey = (String) iterator.next();

        subSet = descendingKeySet.subSet(startKey, endKey);
        assertEquals(2, subSet.size());
        subSetIterator = subSet.iterator();
        assertEquals(startKey, subSetIterator.next());
        subSetIterator.next();
        try {
            subSetIterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        subSet = descendingKeySet.subSet(startKey, false, endKey, false);
        assertEquals(1, subSet.size());
        subSetIterator = subSet.iterator();
        subSetIterator.next();
        try {
            subSetIterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        subSet = descendingKeySet.subSet(startKey, false, endKey, true);
        assertEquals(2, subSet.size());
        subSetIterator = subSet.iterator();
        subSetIterator.next();
        assertEquals(endKey, subSetIterator.next());
        try {
            subSetIterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        subSet = descendingKeySet.subSet(startKey, true, endKey, false);
        assertEquals(2, subSet.size());
        subSetIterator = subSet.iterator();
        assertEquals(startKey, subSetIterator.next());
        subSetIterator.next();
        try {
            subSetIterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        subSet = descendingKeySet.subSet(startKey, true, endKey, true);
        assertEquals(3, subSet.size());
        subSetIterator = subSet.iterator();
        assertEquals(startKey, subSetIterator.next());
        subSetIterator.next();
        assertEquals(endKey, subSetIterator.next());
        try {
            subSetIterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        // With Comparator
        keySet = ((NavigableMap) subMap_startExcluded_endExcluded_comparator)
                .navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        startIterator = descendingKeySet.iterator();
        while (startIterator.hasNext()) {
            startKey = (String) startIterator.next();
            endIterator = descendingKeySet.iterator();
            while (endIterator.hasNext()) {
                endKey = (String) endIterator.next();
                int startIndex = Integer.valueOf(startKey);
                int endIndex = Integer.valueOf(endKey);
                if (startIndex < endIndex) {
                    try {
                        descendingKeySet.subSet(startKey, endKey);
                        fail("should throw IllegalArgumentException");
                    } catch (IllegalArgumentException e) {
                        // Expected
                    }

                    try {
                        descendingKeySet.subSet(startKey, false, endKey, false);
                        fail("shoudl throw IllegalArgumentException");
                    } catch (IllegalArgumentException e) {
                        // Expected
                    }

                    try {
                        descendingKeySet.subSet(startKey, false, endKey, true);
                        fail("shoudl throw IllegalArgumentException");
                    } catch (IllegalArgumentException e) {
                        // Expected
                    }

                    try {
                        descendingKeySet.subSet(startKey, true, endKey, false);
                        fail("shoudl throw IllegalArgumentException");
                    } catch (IllegalArgumentException e) {
                        // Expected
                    }

                    try {
                        descendingKeySet.subSet(startKey, true, endKey, true);
                        fail("shoudl throw IllegalArgumentException");
                    } catch (IllegalArgumentException e) {
                        // Expected
                    }
                } else {
                    subSet = descendingKeySet.subSet(startKey, endKey);
                    subSetIterator = subSet.iterator();
                    for (int index = startIndex; subSetIterator.hasNext(); index--) {
                        assertEquals(new Integer(index).toString(),
                                subSetIterator.next());
                    }

                    subSet = descendingKeySet.subSet(startKey, false, endKey,
                            false);
                    subSetIterator = subSet.iterator();
                    for (int index = startIndex - 1; subSetIterator.hasNext(); index--) {
                        assertEquals(new Integer(index).toString(),
                                subSetIterator.next());
                    }

                    subSet = descendingKeySet.subSet(startKey, false, endKey,
                            true);
                    subSetIterator = subSet.iterator();
                    for (int index = startIndex - 1; subSetIterator.hasNext(); index--) {
                        assertEquals(new Integer(index).toString(),
                                subSetIterator.next());
                    }

                    subSet = descendingKeySet.subSet(startKey, true, endKey,
                            false);
                    subSetIterator = subSet.iterator();
                    for (int index = startIndex; subSetIterator.hasNext(); index--) {
                        assertEquals(new Integer(index).toString(),
                                subSetIterator.next());
                    }

                    subSet = descendingKeySet.subSet(startKey, true, endKey,
                            true);
                    subSetIterator = subSet.iterator();
                    for (int index = startIndex; subSetIterator.hasNext(); index--) {
                        assertEquals(new Integer(index).toString(),
                                subSetIterator.next());
                    }
                }
            }
        }
    }

    public void test_DescendingSubMapKeySet_descendingSet() {
        NavigableSet keySet, descendingSet, descendingDescendingSet;
        int value;
        Iterator iterator;

        keySet = navigableMap_startExcluded_endExcluded.navigableKeySet();
        descendingSet = keySet.descendingSet();
        descendingDescendingSet = descendingSet.descendingSet();
        iterator = descendingDescendingSet.iterator();
        assertTrue(iterator.hasNext());
        for (value = 101; iterator.hasNext(); value++) {
            assertEquals(new Integer(value).toString(), iterator.next());
        }
        assertEquals(109, value);
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        keySet = navigableMap_startExcluded_endIncluded.navigableKeySet();
        descendingSet = keySet.descendingSet();
        descendingDescendingSet = descendingSet.descendingSet();
        iterator = descendingDescendingSet.iterator();
        assertTrue(iterator.hasNext());
        for (value = 101; iterator.hasNext(); value++) {
            assertEquals(new Integer(value).toString(), iterator.next());
        }
        assertEquals(110, value);
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        keySet = navigableMap_startIncluded_endExcluded.navigableKeySet();
        descendingSet = keySet.descendingSet();
        descendingDescendingSet = descendingSet.descendingSet();
        iterator = descendingDescendingSet.iterator();
        assertTrue(iterator.hasNext());
        for (value = 100; iterator.hasNext(); value++) {
            assertEquals(new Integer(value).toString(), iterator.next());
        }
        assertEquals(109, value);
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        keySet = navigableMap_startIncluded_endIncluded.navigableKeySet();
        descendingSet = keySet.descendingSet();
        descendingDescendingSet = descendingSet.descendingSet();
        iterator = descendingDescendingSet.iterator();
        assertTrue(iterator.hasNext());
        for (value = 100; iterator.hasNext(); value++) {
            assertEquals(new Integer(value).toString(), iterator.next());
        }
        assertEquals(110, value);
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        String endKey = new Integer(2).toString();
        keySet = tm.headMap(endKey, true).navigableKeySet();
        descendingSet = keySet.descendingSet();
        descendingDescendingSet = descendingSet.descendingSet();
        assertEquals(keySet, descendingDescendingSet);

        String startKey = new Integer(2).toString();
        keySet = tm.tailMap(startKey, true).navigableKeySet();
        descendingSet = keySet.descendingSet();
        descendingDescendingSet = descendingSet.descendingSet();
        assertEquals(keySet, descendingDescendingSet);
    }

    public void test_DescendingSubMapKeySet_descendingIterator() {
        NavigableSet keySet, descendingSet;
        int value;
        Iterator iterator, descendingIterator;

        keySet = navigableMap_startExcluded_endExcluded.navigableKeySet();
        descendingSet = keySet.descendingSet();
        descendingIterator = descendingSet.descendingIterator();
        assertTrue(descendingIterator.hasNext());
        for (value = 101; descendingIterator.hasNext(); value++) {
            assertEquals(new Integer(value).toString(), descendingIterator
                    .next());
        }
        assertEquals(109, value);
        try {
            descendingIterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        descendingSet = descendingSet
                .headSet(new Integer(105).toString(), true);
        descendingIterator = descendingSet.descendingIterator();
        for (value = 105; descendingIterator.hasNext(); value++) {
            assertEquals(new Integer(value).toString(), descendingIterator
                    .next());
        }

        descendingSet = keySet.descendingSet();
        descendingSet = descendingSet
                .tailSet(new Integer(105).toString(), true);
        descendingIterator = descendingSet.descendingIterator();
        for (value = 101; descendingIterator.hasNext(); value++) {
            assertEquals(new Integer(value).toString(), descendingIterator
                    .next());
        }

        keySet = navigableMap_startExcluded_endIncluded.navigableKeySet();
        descendingSet = keySet.descendingSet();
        descendingIterator = descendingSet.descendingIterator();
        assertTrue(descendingIterator.hasNext());
        for (value = 101; descendingIterator.hasNext(); value++) {
            assertEquals(new Integer(value).toString(), descendingIterator
                    .next());
        }
        assertEquals(110, value);
        try {
            descendingIterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        descendingSet = descendingSet
                .headSet(new Integer(105).toString(), true);
        descendingIterator = descendingSet.descendingIterator();
        for (value = 105; descendingIterator.hasNext(); value++) {
            assertEquals(new Integer(value).toString(), descendingIterator
                    .next());
        }

        descendingSet = keySet.descendingSet();
        descendingSet = descendingSet
                .tailSet(new Integer(105).toString(), true);
        descendingIterator = descendingSet.descendingIterator();
        for (value = 101; descendingIterator.hasNext(); value++) {
            assertEquals(new Integer(value).toString(), descendingIterator
                    .next());
        }

        keySet = navigableMap_startIncluded_endExcluded.navigableKeySet();
        descendingSet = keySet.descendingSet();
        descendingIterator = descendingSet.descendingIterator();
        assertTrue(descendingIterator.hasNext());
        for (value = 100; descendingIterator.hasNext(); value++) {
            assertEquals(new Integer(value).toString(), descendingIterator
                    .next());
        }
        assertEquals(109, value);
        try {
            descendingIterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        keySet = navigableMap_startIncluded_endIncluded.navigableKeySet();
        descendingSet = keySet.descendingSet();
        descendingIterator = descendingSet.descendingIterator();
        assertTrue(descendingIterator.hasNext());
        for (value = 100; descendingIterator.hasNext(); value++) {
            assertEquals(new Integer(value).toString(), descendingIterator
                    .next());
        }
        assertEquals(110, value);
        try {
            descendingIterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        String endKey = new Integer(2).toString();
        keySet = tm.headMap(endKey, true).navigableKeySet();
        iterator = keySet.iterator();

        descendingSet = keySet.descendingSet();
        descendingIterator = descendingSet.descendingIterator();

        while (iterator.hasNext()) {
            assertEquals(iterator.next(), descendingIterator.next());
        }

        String startKey = new Integer(2).toString();
        keySet = tm.tailMap(startKey, true).navigableKeySet();
        iterator = keySet.iterator();
        descendingSet = keySet.descendingSet();
        descendingIterator = descendingSet.descendingIterator();

        while (iterator.hasNext()) {
            assertEquals(iterator.next(), descendingIterator.next());
        }
    }

    public void test_DescendingSubMapKeySet_lower() {
        NavigableSet keySet, descendingKeySet;
        Iterator iterator;
        String key, lowerKey;
        int value, lowerValue;

        keySet = navigableMap_startExcluded_endExcluded.navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        iterator = descendingKeySet.iterator();
        while (iterator.hasNext()) {
            key = (String) iterator.next();
            value = Integer.valueOf(key);
            lowerKey = (String) descendingKeySet.lower(key);
            if (value < 108) {
                lowerValue = Integer.valueOf(lowerKey);
                assertEquals(value + 1, lowerValue);
            } else {
                assertNull(lowerKey);
            }
        }

        key = new Integer(0).toString();
        lowerKey = (String) descendingKeySet.lower(key);
        assertEquals(101, Integer.parseInt(lowerKey));

        key = new Integer(2).toString();
        lowerKey = (String) descendingKeySet.lower(key);
        assertNull(lowerKey);

        keySet = navigableMap_startExcluded_endIncluded.navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        iterator = descendingKeySet.iterator();
        while (iterator.hasNext()) {
            key = (String) iterator.next();
            value = Integer.valueOf(key);
            lowerKey = (String) descendingKeySet.lower(key);
            if (value < 109) {
                lowerValue = Integer.valueOf(lowerKey);
                assertEquals(value + 1, lowerValue);
            } else {
                assertNull(lowerKey);
            }
        }

        key = new Integer(0).toString();
        lowerKey = (String) descendingKeySet.lower(key);
        assertEquals(101, Integer.parseInt(lowerKey));

        key = new Integer(2).toString();
        lowerKey = (String) descendingKeySet.lower(key);
        assertNull(lowerKey);

        keySet = navigableMap_startIncluded_endExcluded.navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        iterator = descendingKeySet.iterator();
        while (iterator.hasNext()) {
            key = (String) iterator.next();
            value = Integer.valueOf(key);
            lowerKey = (String) descendingKeySet.lower(key);
            if (value < 108) {
                lowerValue = Integer.valueOf(lowerKey);
                assertEquals(value + 1, lowerValue);
            } else {
                assertNull(lowerKey);
            }
        }

        key = new Integer(0).toString();
        lowerKey = (String) descendingKeySet.lower(key);
        assertEquals(100, Integer.parseInt(lowerKey));

        key = new Integer(2).toString();
        lowerKey = (String) descendingKeySet.lower(key);
        assertNull(lowerKey);

        keySet = navigableMap_startIncluded_endIncluded.navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        iterator = descendingKeySet.iterator();
        while (iterator.hasNext()) {
            key = (String) iterator.next();
            value = Integer.valueOf(key);
            lowerKey = (String) descendingKeySet.lower(key);
            if (value < 109) {
                lowerValue = Integer.valueOf(lowerKey);
                assertEquals(value + 1, lowerValue);
            } else {
                assertNull(lowerKey);
            }
        }

        key = new Integer(0).toString();
        lowerKey = (String) descendingKeySet.lower(key);
        assertEquals(100, Integer.parseInt(lowerKey));

        key = new Integer(2).toString();
        lowerKey = (String) descendingKeySet.lower(key);
        assertNull(lowerKey);

        key = new Integer(2).toString();
        keySet = tm.headMap(key, true).navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        iterator = descendingKeySet.iterator();
        iterator.next();
        iterator.next();
        key = (String) iterator.next();
        lowerKey = (String) descendingKeySet.lower(key);
        assertEquals(new Integer(199).toString(), lowerKey);
        try {
            descendingKeySet.lower(null);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        key = new Integer(0).toString();
        String endKey = key;

        keySet = tm.headMap(endKey, true).navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        assertNull(descendingKeySet.lower(endKey));

        key = new Integer(0).toString();
        keySet = tm.headMap(endKey, false).navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        assertNull(descendingKeySet.lower(endKey));

        endKey = new Integer(999).toString();
        keySet = tm.headMap(endKey, true).navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        assertNull(descendingKeySet.lower(endKey));
        assertEquals(new Integer(1).toString(), descendingKeySet.lower(key));

        endKey = new Integer(999).toString();
        keySet = tm.headMap(endKey, false).navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        assertNull(descendingKeySet.lower(endKey));
        assertEquals(new Integer(1).toString(), descendingKeySet.lower(key));

        // With Comparator
        keySet = ((NavigableMap) subMap_startExcluded_endExcluded_comparator)
                .navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        iterator = descendingKeySet.iterator();
        while (iterator.hasNext()) {
            key = (String) iterator.next();
            value = Integer.valueOf(key);
            lowerKey = (String) descendingKeySet.lower(key);
            if (value < 108) {
                lowerValue = Integer.valueOf(lowerKey);
                assertEquals(value + 1, lowerValue);
            } else {
                assertNull(lowerKey);
            }
        }

        key = new Integer(0).toString();
        lowerKey = (String) descendingKeySet.lower(key);
        assertEquals(101, Integer.parseInt(lowerKey));

        key = new Integer(2).toString();
        lowerKey = (String) descendingKeySet.lower(key);
        assertNull(lowerKey);

        keySet = ((NavigableMap) subMap_startExcluded_endIncluded_comparator)
                .navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        iterator = descendingKeySet.iterator();
        while (iterator.hasNext()) {
            key = (String) iterator.next();
            value = Integer.valueOf(key);
            lowerKey = (String) descendingKeySet.lower(key);
            if (value < 109) {
                lowerValue = Integer.valueOf(lowerKey);
                assertEquals(value + 1, lowerValue);
            } else {
                assertNull(lowerKey);
            }
        }

        key = new Integer(0).toString();
        lowerKey = (String) descendingKeySet.lower(key);
        assertEquals(101, Integer.parseInt(lowerKey));

        key = new Integer(2).toString();
        lowerKey = (String) descendingKeySet.lower(key);
        assertNull(lowerKey);

        keySet = ((NavigableMap) subMap_startIncluded_endExcluded_comparator)
                .navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        iterator = descendingKeySet.iterator();
        while (iterator.hasNext()) {
            key = (String) iterator.next();
            value = Integer.valueOf(key);
            lowerKey = (String) descendingKeySet.lower(key);
            if (value < 108) {
                lowerValue = Integer.valueOf(lowerKey);
                assertEquals(value + 1, lowerValue);
            } else {
                assertNull(lowerKey);
            }
        }

        key = new Integer(0).toString();
        lowerKey = (String) descendingKeySet.lower(key);
        assertEquals(100, Integer.parseInt(lowerKey));

        key = new Integer(2).toString();
        lowerKey = (String) descendingKeySet.lower(key);
        assertNull(lowerKey);

        keySet = ((NavigableMap) subMap_startIncluded_endIncluded_comparator)
                .navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        iterator = descendingKeySet.iterator();
        while (iterator.hasNext()) {
            key = (String) iterator.next();
            value = Integer.valueOf(key);
            lowerKey = (String) descendingKeySet.lower(key);
            if (value < 109) {
                lowerValue = Integer.valueOf(lowerKey);
                assertEquals(value + 1, lowerValue);
            } else {
                assertNull(lowerKey);
            }
        }
    }

    public void test_DescendingSubMapKeySet_higher() {
        NavigableSet keySet, descendingKeySet;
        Iterator iterator;
        String key, higherKey;
        int value, higherValue;

        keySet = navigableMap_startExcluded_endExcluded.navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        iterator = descendingKeySet.iterator();
        while (iterator.hasNext()) {
            key = (String) iterator.next();
            value = Integer.valueOf(key);
            higherKey = (String) descendingKeySet.higher(key);
            if (value > 101) {
                higherValue = Integer.valueOf(higherKey);
                assertEquals(value - 1, higherValue);
            } else {
                assertNull(higherKey);
            }
        }

        key = new Integer(99999).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertEquals("108", higherKey);

        key = new Integer(-1).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertNull(higherKey);

        key = new Integer(100).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertNull(higherKey);

        key = new Integer(0).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertNull(higherKey);

        key = new Integer(2).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertEquals(108, Integer.parseInt(higherKey));

        keySet = navigableMap_startExcluded_endIncluded.navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        iterator = descendingKeySet.iterator();
        while (iterator.hasNext()) {
            key = (String) iterator.next();
            value = Integer.valueOf(key);
            higherKey = (String) descendingKeySet.higher(key);
            if (value > 101) {
                higherValue = Integer.valueOf(higherKey);
                assertEquals(value - 1, higherValue);
            } else {
                assertNull(higherKey);
            }
        }

        key = new Integer(99999).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertEquals("109", higherKey);

        key = new Integer(-1).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertNull(higherKey);

        key = new Integer(100).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertNull(higherKey);

        key = new Integer(2).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertEquals(109, Integer.parseInt(higherKey));

        keySet = navigableMap_startIncluded_endExcluded.navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        iterator = descendingKeySet.iterator();
        while (iterator.hasNext()) {
            key = (String) iterator.next();
            value = Integer.valueOf(key);
            higherKey = (String) descendingKeySet.higher(key);
            if (value > 100) {
                higherValue = Integer.valueOf(higherKey);
                assertEquals(value - 1, higherValue);
            } else {
                assertNull(higherKey);
            }
        }

        key = new Integer(99999).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertEquals("108", higherKey);

        key = new Integer(-1).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertNull(higherKey);

        key = new Integer(100).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertNull(higherKey);

        key = new Integer(2).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertEquals(108, Integer.parseInt(higherKey));

        keySet = navigableMap_startIncluded_endIncluded.navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        iterator = descendingKeySet.iterator();
        while (iterator.hasNext()) {
            key = (String) iterator.next();
            value = Integer.valueOf(key);
            higherKey = (String) descendingKeySet.higher(key);
            if (value > 100) {
                higherValue = Integer.valueOf(higherKey);
                assertEquals(value - 1, higherValue);
            } else {
                assertNull(higherKey);
            }
        }

        key = new Integer(99999).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertEquals("109", higherKey);

        key = new Integer(-1).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertNull(higherKey);

        key = new Integer(100).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertNull(higherKey);

        key = new Integer(2).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertEquals(109, Integer.parseInt(higherKey));

        key = new Integer(2).toString();
        keySet = tm.headMap(key, true).navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        iterator = descendingKeySet.iterator();
        key = (String) iterator.next();
        higherKey = (String) descendingKeySet.higher(key);
        assertEquals(new Integer(199).toString(), higherKey);
        try {
            descendingKeySet.higher(null);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        key = new Integer(0).toString();
        String endKey = key;

        keySet = tm.headMap(endKey, true).navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        assertNull(descendingKeySet.higher(endKey));

        key = new Integer(0).toString();
        keySet = tm.headMap(endKey, false).navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        assertNull(descendingKeySet.higher(endKey));

        endKey = new Integer(999).toString();
        keySet = tm.headMap(endKey, true).navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        assertEquals(new Integer(998).toString(), descendingKeySet
                .higher(endKey));
        assertNull(descendingKeySet.higher(key));

        endKey = new Integer(999).toString();
        keySet = tm.headMap(endKey, false).navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        assertEquals(new Integer(998).toString(), descendingKeySet
                .higher(endKey));
        assertNull(descendingKeySet.higher(key));

        // With Comparator
        keySet = ((NavigableMap) subMap_startExcluded_endExcluded_comparator)
                .navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        iterator = descendingKeySet.iterator();
        while (iterator.hasNext()) {
            key = (String) iterator.next();
            value = Integer.valueOf(key);
            higherKey = (String) descendingKeySet.higher(key);
            if (value > 101) {
                higherValue = Integer.valueOf(higherKey);
                assertEquals(value - 1, higherValue);
            } else {
                assertNull(higherKey);
            }
        }

        key = new Integer(99999).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertEquals("108", higherKey);

        key = new Integer(-1).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertNull(higherKey);

        key = new Integer(100).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertNull(higherKey);

        key = new Integer(0).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertNull(higherKey);

        key = new Integer(2).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertEquals(108, Integer.parseInt(higherKey));

        keySet = ((NavigableMap) subMap_startExcluded_endIncluded_comparator)
                .navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        iterator = descendingKeySet.iterator();
        while (iterator.hasNext()) {
            key = (String) iterator.next();
            value = Integer.valueOf(key);
            higherKey = (String) descendingKeySet.higher(key);
            if (value > 101) {
                higherValue = Integer.valueOf(higherKey);
                assertEquals(value - 1, higherValue);
            } else {
                assertNull(higherKey);
            }
        }

        key = new Integer(99999).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertEquals("109", higherKey);

        key = new Integer(-1).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertNull(higherKey);

        key = new Integer(100).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertNull(higherKey);

        key = new Integer(2).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertEquals(109, Integer.parseInt(higherKey));

        keySet = ((NavigableMap) subMap_startIncluded_endExcluded_comparator)
                .navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        iterator = descendingKeySet.iterator();
        while (iterator.hasNext()) {
            key = (String) iterator.next();
            value = Integer.valueOf(key);
            higherKey = (String) descendingKeySet.higher(key);
            if (value > 100) {
                higherValue = Integer.valueOf(higherKey);
                assertEquals(value - 1, higherValue);
            } else {
                assertNull(higherKey);
            }
        }

        key = new Integer(99999).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertEquals("108", higherKey);

        key = new Integer(-1).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertNull(higherKey);

        key = new Integer(100).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertNull(higherKey);

        key = new Integer(2).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertEquals(108, Integer.parseInt(higherKey));

        keySet = ((NavigableMap) subMap_startIncluded_endIncluded_comparator)
                .navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        iterator = descendingKeySet.iterator();
        while (iterator.hasNext()) {
            key = (String) iterator.next();
            value = Integer.valueOf(key);
            higherKey = (String) descendingKeySet.higher(key);
            if (value > 100) {
                higherValue = Integer.valueOf(higherKey);
                assertEquals(value - 1, higherValue);
            } else {
                assertNull(higherKey);
            }
        }

        key = new Integer(99999).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertEquals("109", higherKey);

        key = new Integer(-1).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertNull(higherKey);

        key = new Integer(100).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertNull(higherKey);

        key = new Integer(2).toString();
        higherKey = (String) descendingKeySet.higher(key);
        assertEquals(109, Integer.parseInt(higherKey));

        key = new Integer(2).toString();
        keySet = tm.headMap(key, true).navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        iterator = descendingKeySet.iterator();
        key = (String) iterator.next();
        higherKey = (String) descendingKeySet.higher(key);
        assertEquals(new Integer(199).toString(), higherKey);
        try {
            descendingKeySet.higher(null);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        key = new Integer(0).toString();
        endKey = key;

        keySet = tm.headMap(endKey, true).navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        assertNull(descendingKeySet.higher(endKey));

        key = new Integer(0).toString();
        keySet = tm.headMap(endKey, false).navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        assertNull(descendingKeySet.higher(endKey));

        endKey = new Integer(999).toString();
        keySet = tm.headMap(endKey, true).navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        assertEquals(new Integer(998).toString(), descendingKeySet
                .higher(endKey));
        assertNull(descendingKeySet.higher(key));

        endKey = new Integer(999).toString();
        keySet = tm.headMap(endKey, false).navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        assertEquals(new Integer(998).toString(), descendingKeySet
                .higher(endKey));
        assertNull(descendingKeySet.higher(key));
    }

    public void test_DescendingSubMapKeySet_ceiling() {
        NavigableSet keySet, descendingKeySet;
        String[] keyArray;
        String key, ceilingKey;

        keySet = navigableMap_startExcluded_endExcluded.navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        keyArray = (String[]) descendingKeySet
                .toArray(new String[descendingKeySet.size()]);
        for (int i = 0, j = 108; i < keyArray.length; i++) {
            ceilingKey = (String) descendingKeySet.ceiling(keyArray[i]);
            assertEquals(new Integer(j - i).toString(), ceilingKey);
        }

        key = new Integer(2).toString();
        ceilingKey = (String) descendingKeySet.ceiling(key);
        assertEquals(108, Integer.parseInt(ceilingKey));

        key = new Integer(0).toString();
        ceilingKey = (String) descendingKeySet.ceiling(key);
        assertNull(ceilingKey);

        key = new Integer(-1).toString();
        ceilingKey = (String) descendingKeySet.ceiling(key);
        assertNull(ceilingKey);

        key = new Integer(99999).toString();
        ceilingKey = (String) descendingKeySet.ceiling(key);
        assertEquals(108, Integer.parseInt(ceilingKey));

        keySet = navigableMap_startExcluded_endIncluded.navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        keyArray = (String[]) descendingKeySet
                .toArray(new String[descendingKeySet.size()]);
        for (int i = 0, j = 109; i < keyArray.length; i++) {
            ceilingKey = (String) descendingKeySet.ceiling(keyArray[i]);
            assertEquals(new Integer(j - i).toString(), ceilingKey);
        }

        keySet = navigableMap_startIncluded_endExcluded.navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        keyArray = (String[]) descendingKeySet
                .toArray(new String[descendingKeySet.size()]);
        for (int i = 0, j = 108; i < keyArray.length; i++) {
            ceilingKey = (String) descendingKeySet.ceiling(keyArray[i]);
            assertEquals(new Integer(j - i).toString(), ceilingKey);
        }

        keySet = navigableMap_startIncluded_endIncluded.navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        keyArray = (String[]) descendingKeySet
                .toArray(new String[descendingKeySet.size()]);
        for (int i = 0, j = 109; i < keyArray.length; i++) {
            ceilingKey = (String) descendingKeySet.ceiling(keyArray[i]);
            assertEquals(new Integer(j - i).toString(), ceilingKey);
        }

        key = new Integer(2).toString();
        keySet = tm.headMap(key, true).navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        Iterator iterator = descendingKeySet.iterator();
        assertEquals(key, descendingKeySet.ceiling(iterator.next()));
        try {
            descendingKeySet.ceiling(null);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        key = new Integer(0).toString();
        String endKey = key;

        keySet = tm.headMap(endKey, true).navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        assertEquals(key, descendingKeySet.ceiling(endKey));

        key = new Integer(0).toString();
        keySet = tm.headMap(endKey, false).navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        assertNull(descendingKeySet.ceiling(endKey));

        endKey = new Integer(999).toString();
        keySet = tm.headMap(endKey, true).navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        assertEquals(new Integer(999).toString(), descendingKeySet
                .ceiling(endKey));
        assertEquals(key, descendingKeySet.ceiling(key));

        endKey = new Integer(999).toString();
        keySet = tm.headMap(endKey, false).navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        assertEquals(new Integer(998).toString(), descendingKeySet
                .ceiling(endKey));
        assertEquals(key, descendingKeySet.ceiling(key));

        // With Comparator
        keySet = ((NavigableMap) subMap_startExcluded_endExcluded_comparator)
                .navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        keyArray = (String[]) descendingKeySet
                .toArray(new String[descendingKeySet.size()]);
        for (int i = 0, j = 108; i < keyArray.length; i++) {
            ceilingKey = (String) descendingKeySet.ceiling(keyArray[i]);
            assertEquals(new Integer(j - i).toString(), ceilingKey);
        }

        key = new Integer(2).toString();
        ceilingKey = (String) descendingKeySet.ceiling(key);
        assertEquals(108, Integer.parseInt(ceilingKey));

        key = new Integer(0).toString();
        ceilingKey = (String) descendingKeySet.ceiling(key);
        assertNull(ceilingKey);

        keySet = ((NavigableMap) subMap_startExcluded_endIncluded_comparator)
                .navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        keyArray = (String[]) descendingKeySet
                .toArray(new String[descendingKeySet.size()]);
        for (int i = 0, j = 109; i < keyArray.length; i++) {
            ceilingKey = (String) descendingKeySet.ceiling(keyArray[i]);
            assertEquals(new Integer(j - i).toString(), ceilingKey);
        }

        keySet = ((NavigableMap) subMap_startIncluded_endExcluded_comparator)
                .navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        keyArray = (String[]) descendingKeySet
                .toArray(new String[descendingKeySet.size()]);
        for (int i = 0, j = 108; i < keyArray.length; i++) {
            ceilingKey = (String) descendingKeySet.ceiling(keyArray[i]);
            assertEquals(new Integer(j - i).toString(), ceilingKey);
        }

        keySet = ((NavigableMap) subMap_startIncluded_endIncluded_comparator)
                .navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        keyArray = (String[]) descendingKeySet
                .toArray(new String[descendingKeySet.size()]);
        for (int i = 0, j = 109; i < keyArray.length; i++) {
            ceilingKey = (String) descendingKeySet.ceiling(keyArray[i]);
            assertEquals(new Integer(j - i).toString(), ceilingKey);
        }
    }

    public void test_DescendingSubMapKeySet_floor() {
        NavigableSet keySet, descendingKeySet;
        String[] keyArray;
        String floorKey;

        keySet = navigableMap_startExcluded_endExcluded.navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        keyArray = (String[]) descendingKeySet
                .toArray(new String[descendingKeySet.size()]);
        for (int i = 0, j = 108; i < keyArray.length; i++) {
            floorKey = (String) descendingKeySet.floor(keyArray[i]);
            assertEquals(new Integer(j - i).toString(), floorKey);
        }

        String key = new Integer(0).toString();
        floorKey = (String) descendingKeySet.floor(key);
        assertEquals(101, Integer.parseInt(floorKey));

        key = new Integer(2).toString();
        floorKey = (String) descendingKeySet.floor(key);
        assertNull(floorKey);

        keySet = navigableMap_startExcluded_endIncluded.navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        keyArray = (String[]) descendingKeySet
                .toArray(new String[descendingKeySet.size()]);
        for (int i = 0, j = 109; i < keyArray.length; i++) {
            floorKey = (String) descendingKeySet.floor(keyArray[i]);
            assertEquals(new Integer(j - i).toString(), floorKey);
        }

        key = new Integer(0).toString();
        floorKey = (String) descendingKeySet.floor(key);
        assertEquals(101, Integer.parseInt(floorKey));

        key = new Integer(2).toString();
        floorKey = (String) descendingKeySet.floor(key);
        assertNull(floorKey);

        keySet = navigableMap_startIncluded_endExcluded.navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        keyArray = (String[]) descendingKeySet
                .toArray(new String[descendingKeySet.size()]);
        for (int i = 0, j = 108; i < keyArray.length; i++) {
            floorKey = (String) descendingKeySet.floor(keyArray[i]);
            assertEquals(new Integer(j - i).toString(), floorKey);
        }

        key = new Integer(0).toString();
        floorKey = (String) descendingKeySet.floor(key);
        assertEquals(100, Integer.parseInt(floorKey));

        key = new Integer(2).toString();
        floorKey = (String) descendingKeySet.floor(key);
        assertNull(floorKey);

        keySet = navigableMap_startIncluded_endIncluded.navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        keyArray = (String[]) descendingKeySet
                .toArray(new String[descendingKeySet.size()]);
        for (int i = 0, j = 109; i < keyArray.length; i++) {
            floorKey = (String) descendingKeySet.floor(keyArray[i]);
            assertEquals(new Integer(j - i).toString(), floorKey);
        }

        key = new Integer(0).toString();
        floorKey = (String) descendingKeySet.floor(key);
        assertEquals(100, Integer.parseInt(floorKey));

        key = new Integer(2).toString();
        floorKey = (String) descendingKeySet.floor(key);
        assertNull(floorKey);

        key = new Integer(2).toString();
        keySet = tm.headMap(key, true).navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        Iterator iterator = descendingKeySet.iterator();
        assertEquals(key, descendingKeySet.floor(iterator.next()));
        try {
            descendingKeySet.floor(null);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        key = new Integer(0).toString();
        String endKey = key;

        keySet = tm.headMap(endKey, true).navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        assertEquals(key, descendingKeySet.floor(endKey));

        key = new Integer(0).toString();
        keySet = tm.headMap(endKey, false).navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        assertNull(descendingKeySet.floor(endKey));

        endKey = new Integer(999).toString();
        keySet = tm.headMap(endKey, true).navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        assertEquals(new Integer(999).toString(), descendingKeySet
                .floor(endKey));
        assertEquals(key, descendingKeySet.floor(key));

        endKey = new Integer(999).toString();
        keySet = tm.headMap(endKey, false).navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        assertNull(descendingKeySet.floor(endKey));
        assertEquals(key, descendingKeySet.floor(key));

        // With Comparator
        keySet = ((NavigableMap) subMap_startExcluded_endExcluded_comparator)
                .navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        keyArray = (String[]) descendingKeySet
                .toArray(new String[descendingKeySet.size()]);
        for (int i = 0, j = 108; i < keyArray.length; i++) {
            floorKey = (String) descendingKeySet.floor(keyArray[i]);
            assertEquals(new Integer(j - i).toString(), floorKey);
        }

        key = new Integer(0).toString();
        floorKey = (String) descendingKeySet.floor(key);
        assertEquals(101, Integer.parseInt(floorKey));

        key = new Integer(2).toString();
        floorKey = (String) descendingKeySet.floor(key);
        assertNull(floorKey);

        keySet = ((NavigableMap) subMap_startExcluded_endIncluded_comparator)
                .navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        keyArray = (String[]) descendingKeySet
                .toArray(new String[descendingKeySet.size()]);
        for (int i = 0, j = 109; i < keyArray.length; i++) {
            floorKey = (String) descendingKeySet.floor(keyArray[i]);
            assertEquals(new Integer(j - i).toString(), floorKey);
        }

        key = new Integer(0).toString();
        floorKey = (String) descendingKeySet.floor(key);
        assertEquals(101, Integer.parseInt(floorKey));

        key = new Integer(2).toString();
        floorKey = (String) descendingKeySet.floor(key);
        assertNull(floorKey);

        keySet = ((NavigableMap) subMap_startIncluded_endExcluded_comparator)
                .navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        keyArray = (String[]) descendingKeySet
                .toArray(new String[descendingKeySet.size()]);
        for (int i = 0, j = 108; i < keyArray.length; i++) {
            floorKey = (String) descendingKeySet.floor(keyArray[i]);
            assertEquals(new Integer(j - i).toString(), floorKey);
        }

        key = new Integer(0).toString();
        floorKey = (String) descendingKeySet.floor(key);
        assertEquals(100, Integer.parseInt(floorKey));

        key = new Integer(2).toString();
        floorKey = (String) descendingKeySet.floor(key);
        assertNull(floorKey);

        keySet = ((NavigableMap) subMap_startIncluded_endIncluded)
                .navigableKeySet();
        descendingKeySet = keySet.descendingSet();
        keyArray = (String[]) descendingKeySet
                .toArray(new String[descendingKeySet.size()]);
        for (int i = 0, j = 109; i < keyArray.length; i++) {
            floorKey = (String) descendingKeySet.floor(keyArray[i]);
            assertEquals(new Integer(j - i).toString(), floorKey);
        }

        key = new Integer(0).toString();
        floorKey = (String) descendingKeySet.floor(key);
        assertEquals(100, Integer.parseInt(floorKey));

        key = new Integer(2).toString();
        floorKey = (String) descendingKeySet.floor(key);
        assertNull(floorKey);
    }

    public void test_AscendingSubMapKeySet_last() {
        NavigableSet keySet;
        String firstKey1 = new Integer(108).toString();
        String firstKey2 = new Integer(109).toString();

        keySet = navigableMap_startExcluded_endExcluded.navigableKeySet();
        assertEquals(firstKey1, keySet.last());

        keySet = navigableMap_startExcluded_endIncluded.navigableKeySet();
        assertEquals(firstKey2, keySet.last());

        keySet = navigableMap_startIncluded_endExcluded.navigableKeySet();
        assertEquals(firstKey1, keySet.last());

        keySet = navigableMap_startIncluded_endIncluded.navigableKeySet();
        assertEquals(firstKey2, keySet.last());
    }

    public void test_AscendingSubMapKeySet_comparator() {
        NavigableSet keySet;
        keySet = navigableMap_startExcluded_endExcluded.navigableKeySet();
        assertNull(keySet.comparator());

        keySet = navigableMap_startExcluded_endIncluded.navigableKeySet();
        assertNull(keySet.comparator());

        keySet = navigableMap_startIncluded_endExcluded.navigableKeySet();
        assertNull(keySet.comparator());

        keySet = navigableMap_startIncluded_endIncluded.navigableKeySet();
        assertNull(keySet.comparator());

        String endKey = new Integer(2).toString();
        keySet = tm.headMap(endKey, true).navigableKeySet();
        assertNull(keySet.comparator());
    }

    public void test_AscendingSubMapKeySet_pollFirst_startExcluded_endExcluded() {
        NavigableSet keySet = navigableMap_startExcluded_endExcluded
                .navigableKeySet();
        Iterator iterator = keySet.iterator();
        assertEquals(8, keySet.size());
        for (int value = 101; value < 109; value++) {
            assertEquals(new Integer(value).toString(), keySet.pollFirst());
        }
        assertEquals(0, keySet.size());
        assertNull(keySet.pollFirst());
    }

    public void test_AscendingSubMapKeySet_pollFirst_startExcluded_endIncluded() {
        NavigableSet keySet = navigableMap_startExcluded_endIncluded
                .navigableKeySet();
        Iterator iterator = keySet.iterator();
        assertEquals(9, keySet.size());
        for (int value = 101; value < 110; value++) {
            assertEquals(new Integer(value).toString(), keySet.pollFirst());
        }
        assertEquals(0, keySet.size());
        assertNull(keySet.pollFirst());
    }

    public void test_AscendingSubMapKeySet_pollFirst_startIncluded_endExcluded() {
        NavigableSet keySet = navigableMap_startIncluded_endExcluded
                .navigableKeySet();
        Iterator iterator = keySet.iterator();
        assertEquals(9, keySet.size());
        for (int value = 100; value < 109; value++) {
            assertEquals(new Integer(value).toString(), keySet.pollFirst());
        }
        assertEquals(0, keySet.size());
        assertNull(keySet.pollFirst());
    }

    public void test_AscendingSubMapKeySet_pollFirst_startIncluded_endIncluded() {
        NavigableSet keySet = navigableMap_startIncluded_endIncluded
                .navigableKeySet();
        Iterator iterator = keySet.iterator();
        assertEquals(10, keySet.size());
        for (int value = 100; value < 110; value++) {
            assertEquals(new Integer(value).toString(), keySet.pollFirst());
        }
        assertEquals(0, keySet.size());
        assertNull(keySet.pollFirst());
    }

    public void test_AscendingSubMapKeySet_pollFirst() {
        String endKey = new Integer(2).toString();
        NavigableSet keySet = tm.headMap(endKey, true).navigableKeySet();
        assertEquals(new Integer(0).toString(), keySet.pollFirst());

        keySet = tm.tailMap(endKey, true).navigableKeySet();
        assertEquals(new Integer(2).toString(), keySet.pollFirst());
    }

    public void test_AscendingSubMapKeySet_pollLast_startExcluded_endExcluded() {
        NavigableSet keySet = navigableMap_startExcluded_endExcluded
                .navigableKeySet();
        Iterator iterator = keySet.iterator();
        assertEquals(8, keySet.size());
        for (int value = 108; value > 100; value--) {
            assertEquals(new Integer(value).toString(), keySet.pollLast());
        }
        assertEquals(0, keySet.size());
        assertNull(keySet.pollLast());
    }

    public void test_AscendingSubMapKeySet_pollLast_startExcluded_endIncluded() {
        NavigableSet keySet = navigableMap_startExcluded_endIncluded
                .navigableKeySet();
        Iterator iterator = keySet.iterator();
        assertEquals(9, keySet.size());
        for (int value = 109; value > 100; value--) {
            assertEquals(new Integer(value).toString(), keySet.pollLast());
        }
        assertEquals(0, keySet.size());
        assertNull(keySet.pollLast());
    }

    public void test_AscendingSubMapKeySet_pollLast_startIncluded_endExcluded() {
        NavigableSet keySet = navigableMap_startIncluded_endExcluded
                .navigableKeySet();
        Iterator iterator = keySet.iterator();
        assertEquals(9, keySet.size());
        for (int value = 108; value > 99; value--) {
            assertEquals(new Integer(value).toString(), keySet.pollLast());
        }
        assertEquals(0, keySet.size());
        assertNull(keySet.pollLast());
    }

    public void test_AscendingSubMapKeySet_pollLast_startIncluded_endIncluded() {
        NavigableSet keySet = navigableMap_startIncluded_endIncluded
                .navigableKeySet();
        Iterator iterator = keySet.iterator();
        assertEquals(10, keySet.size());
        for (int value = 109; value > 99; value--) {
            assertEquals(new Integer(value).toString(), keySet.pollLast());
        }
        assertEquals(0, keySet.size());
        assertNull(keySet.pollLast());
    }

    public void test_AscendingSubMapKeySet_pollLast() {
        String endKey = new Integer(2).toString();
        NavigableSet keySet = tm.headMap(endKey, true).navigableKeySet();
        assertEquals(new Integer(2).toString(), keySet.pollLast());

        keySet = tm.tailMap(endKey, true).navigableKeySet();
        assertEquals(new Integer(999).toString(), keySet.pollLast());
    }

    public void test_AscendingSubMapKeySet_descendingIterator() {
        NavigableSet keySet;
        Iterator iterator;

        keySet = navigableMap_startExcluded_endExcluded.navigableKeySet();
        iterator = keySet.descendingIterator();
        for (int value = 108; value > 100; value--) {
            assertTrue(iterator.hasNext());
            assertEquals(new Integer(value).toString(), iterator.next());
        }
        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        keySet = navigableMap_startExcluded_endIncluded.navigableKeySet();
        iterator = keySet.descendingIterator();
        for (int value = 109; value > 100; value--) {
            assertTrue(iterator.hasNext());
            assertEquals(new Integer(value).toString(), iterator.next());
        }
        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        keySet = navigableMap_startIncluded_endExcluded.navigableKeySet();
        iterator = keySet.descendingIterator();
        for (int value = 108; value > 99; value--) {
            assertTrue(iterator.hasNext());
            assertEquals(new Integer(value).toString(), iterator.next());
        }
        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        keySet = navigableMap_startIncluded_endIncluded.navigableKeySet();
        iterator = keySet.descendingIterator();
        for (int value = 109; value > 99; value--) {
            assertTrue(iterator.hasNext());
            assertEquals(new Integer(value).toString(), iterator.next());
        }
        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        String endKey = new Integer(2).toString();
        keySet = tm.headMap(endKey, true).navigableKeySet();
        iterator = keySet.descendingIterator();
        assertEquals(new Integer(2).toString(), iterator.next());
        assertEquals(new Integer(199).toString(), iterator.next());
    }

    public void test_AscendingSubMapKeySet_descendingSet() {
        NavigableSet keySet, descendingSet;
        Iterator iterator;

        keySet = navigableMap_startExcluded_endExcluded.navigableKeySet()
                .descendingSet();
        descendingSet = keySet.descendingSet();
        iterator = descendingSet.iterator();
        for (int value = 101; value < 109; value++) {
            assertTrue(iterator.hasNext());
            assertEquals(new Integer(value).toString(), iterator.next());
        }
        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        keySet = navigableMap_startExcluded_endIncluded.navigableKeySet()
                .descendingSet();
        descendingSet = keySet.descendingSet();
        iterator = descendingSet.iterator();
        for (int value = 101; value < 110; value++) {
            assertTrue(iterator.hasNext());
            assertEquals(new Integer(value).toString(), iterator.next());
        }
        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        keySet = navigableMap_startIncluded_endExcluded.navigableKeySet()
                .descendingSet();
        descendingSet = keySet.descendingSet();
        iterator = descendingSet.iterator();
        for (int value = 100; value < 109; value++) {
            assertTrue(iterator.hasNext());
            assertEquals(new Integer(value).toString(), iterator.next());
        }
        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        keySet = navigableMap_startIncluded_endIncluded.navigableKeySet()
                .descendingSet();
        descendingSet = keySet.descendingSet();
        iterator = descendingSet.iterator();
        for (int value = 100; value < 110; value++) {
            assertTrue(iterator.hasNext());
            assertEquals(new Integer(value).toString(), iterator.next());
        }
        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        String endKey = new Integer(1).toString();
        keySet = tm.headMap(endKey, true).navigableKeySet();
        descendingSet = keySet.descendingSet();
        iterator = descendingSet.iterator();
        assertEquals(new Integer(1).toString(), iterator.next());
        assertEquals(new Integer(0).toString(), iterator.next());
    }

    public void test_AscendingSubMapKeySet_headSet() {
        NavigableSet keySet;
        SortedSet headSet;
        String endKey, key;
        Iterator iterator;

        keySet = navigableMap_startExcluded_endExcluded.navigableKeySet();
        endKey = new Integer(99).toString();
        try {
            keySet.headSet(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.headSet(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(100).toString();
        assertEquals(0, keySet.headSet(endKey).size());
        assertEquals(0, keySet.headSet(endKey, false).size());
        try {
            keySet.headSet(endKey, true).size();
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(101).toString();
        assertEquals(0, keySet.headSet(endKey).size());
        assertEquals(0, keySet.headSet(endKey, false).size());
        assertEquals(1, keySet.headSet(endKey, true).size());

        for (int i = 102; i < 109; i++) {
            endKey = new Integer(i).toString();
            headSet = keySet.headSet(endKey);
            iterator = headSet.iterator();
            int j;
            for (j = 101; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i, j);

            headSet = keySet.headSet(endKey, false);
            iterator = headSet.iterator();
            for (j = 101; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i, j);

            headSet = keySet.headSet(endKey, true);
            iterator = headSet.iterator();
            for (j = 101; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i + 1, j);
        }

        endKey = new Integer(109).toString();
        headSet = keySet.headSet(endKey);
        iterator = headSet.iterator();
        int index;
        for (index = 101; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(109, index);

        headSet = keySet.headSet(endKey, false);
        iterator = headSet.iterator();
        for (index = 101; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(109, index);

        try {
            keySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(110).toString();
        try {
            keySet.headSet(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.headSet(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        keySet = navigableMap_startExcluded_endIncluded.navigableKeySet();
        endKey = new Integer(99).toString();
        try {
            keySet.headSet(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.headSet(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(100).toString();
        assertEquals(0, keySet.headSet(endKey).size());
        assertEquals(0, keySet.headSet(endKey, false).size());
        try {
            keySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(101).toString();
        assertEquals(0, keySet.headSet(endKey).size());
        assertEquals(0, keySet.headSet(endKey).size());
        assertEquals(1, keySet.headSet(endKey, true).size());

        for (int i = 102; i < 109; i++) {
            endKey = new Integer(i).toString();

            headSet = keySet.headSet(endKey);
            iterator = headSet.iterator();
            int j;
            for (j = 101; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i, j);

            headSet = keySet.headSet(endKey, false);
            iterator = headSet.iterator();
            for (j = 101; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i, j);

            headSet = keySet.headSet(endKey, true);
            iterator = headSet.iterator();
            for (j = 101; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i + 1, j);
        }

        endKey = new Integer(109).toString();
        headSet = keySet.headSet(endKey);
        iterator = headSet.iterator();
        for (index = 101; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(109, index);

        headSet = keySet.headSet(endKey, false);
        iterator = headSet.iterator();
        for (index = 101; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(109, index);

        headSet = keySet.headSet(endKey, true);
        iterator = headSet.iterator();
        for (index = 101; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(110, index);

        endKey = new Integer(110).toString();
        try {
            keySet.headSet(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.headSet(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        keySet = navigableMap_startIncluded_endExcluded.navigableKeySet();
        endKey = new Integer(99).toString();
        try {
            keySet.headSet(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.headSet(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(100).toString();
        assertEquals(0, keySet.headSet(endKey).size());
        assertEquals(0, keySet.headSet(endKey, false).size());
        assertEquals(1, keySet.headSet(endKey, true).size());

        endKey = new Integer(101).toString();
        headSet = keySet.headSet(endKey);
        iterator = headSet.iterator();
        for (index = 100; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(101, index);

        headSet = keySet.headSet(endKey, false);
        iterator = headSet.iterator();
        for (index = 100; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(101, index);

        headSet = keySet.headSet(endKey, true);
        iterator = headSet.iterator();
        for (index = 100; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(102, index);

        for (int i = 102; i < 109; i++) {
            endKey = new Integer(i).toString();

            headSet = keySet.headSet(endKey);
            iterator = headSet.iterator();
            int j;
            for (j = 100; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i, j);

            headSet = keySet.headSet(endKey, false);
            iterator = headSet.iterator();
            for (j = 100; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i, j);

            headSet = keySet.headSet(endKey, true);
            iterator = headSet.iterator();
            for (j = 100; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i + 1, j);
        }

        endKey = new Integer(109).toString();
        headSet = keySet.headSet(endKey);
        iterator = headSet.iterator();
        for (index = 100; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(109, index);

        headSet = keySet.headSet(endKey, false);
        iterator = headSet.iterator();
        for (index = 100; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(109, index);

        try {
            keySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(110).toString();
        try {
            keySet.headSet(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            keySet.headSet(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            keySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        keySet = navigableMap_startIncluded_endIncluded.navigableKeySet();
        endKey = new Integer(99).toString();
        try {
            keySet.headSet(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.headSet(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(100).toString();
        assertEquals(0, keySet.headSet(endKey).size());
        assertEquals(0, keySet.headSet(endKey, false).size());
        assertEquals(1, keySet.headSet(endKey, true).size());

        endKey = new Integer(101).toString();
        headSet = keySet.headSet(endKey);
        iterator = headSet.iterator();
        for (index = 100; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(101, index);

        headSet = keySet.headSet(endKey, false);
        iterator = headSet.iterator();
        for (index = 100; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(101, index);

        headSet = keySet.headSet(endKey, true);
        iterator = headSet.iterator();
        for (index = 100; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(102, index);

        for (int i = 102; i < 109; i++) {
            endKey = new Integer(i).toString();

            headSet = keySet.headSet(endKey);
            iterator = headSet.iterator();
            int j;
            for (j = 100; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i, j);

            headSet = keySet.headSet(endKey, false);
            iterator = headSet.iterator();
            for (j = 100; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i, j);

            headSet = keySet.headSet(endKey, true);
            iterator = headSet.iterator();
            for (j = 100; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i + 1, j);
        }

        endKey = new Integer(109).toString();
        headSet = keySet.headSet(endKey);
        iterator = headSet.iterator();
        for (index = 100; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(109, index);

        headSet = keySet.headSet(endKey, false);
        iterator = headSet.iterator();
        for (index = 100; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(109, index);

        headSet = keySet.headSet(endKey, true);
        iterator = headSet.iterator();
        for (index = 100; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(110, index);

        endKey = new Integer(110).toString();
        try {
            keySet.headSet(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.headSet(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        key = new Integer(1).toString();
        keySet = tm.headMap(key, true).navigableKeySet();
        iterator = keySet.iterator();
        iterator.next();
        endKey = (String) iterator.next();
        headSet = keySet.headSet(endKey, false);
        assertEquals(1, headSet.size());
        Iterator headSetIterator = headSet.iterator();
        assertEquals(new Integer(0).toString(), headSetIterator.next());
        assertFalse(headSetIterator.hasNext());
        try {
            headSetIterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }
        try {
            keySet.headSet(null, false);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        headSet = keySet.headSet(endKey, true);
        assertEquals(2, headSet.size());
        headSetIterator = headSet.iterator();
        assertEquals(new Integer(0).toString(), headSetIterator.next());
        assertEquals(new Integer(1).toString(), headSetIterator.next());
        assertFalse(headSetIterator.hasNext());
        try {
            headSetIterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }
        try {
            keySet.headSet(null, false);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        // With Comparator
        keySet = ((NavigableMap) subMap_startExcluded_endExcluded_comparator)
                .navigableKeySet();
        endKey = new Integer(99).toString();
        try {
            keySet.headSet(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.headSet(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(100).toString();
        assertEquals(0, keySet.headSet(endKey).size());
        assertEquals(0, keySet.headSet(endKey, false).size());
        try {
            keySet.headSet(endKey, true).size();
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(101).toString();
        assertEquals(0, keySet.headSet(endKey).size());
        assertEquals(0, keySet.headSet(endKey, false).size());
        assertEquals(1, keySet.headSet(endKey, true).size());

        for (int i = 102; i < 109; i++) {
            endKey = new Integer(i).toString();
            headSet = keySet.headSet(endKey);
            iterator = headSet.iterator();
            int j;
            for (j = 101; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i, j);

            headSet = keySet.headSet(endKey, false);
            iterator = headSet.iterator();
            for (j = 101; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i, j);

            headSet = keySet.headSet(endKey, true);
            iterator = headSet.iterator();
            for (j = 101; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i + 1, j);
        }

        endKey = new Integer(109).toString();
        headSet = keySet.headSet(endKey);
        iterator = headSet.iterator();
        for (index = 101; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(109, index);

        headSet = keySet.headSet(endKey, false);
        iterator = headSet.iterator();
        for (index = 101; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(109, index);

        try {
            keySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(110).toString();
        try {
            keySet.headSet(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.headSet(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        keySet = ((NavigableMap) subMap_startExcluded_endIncluded_comparator)
                .navigableKeySet();
        endKey = new Integer(99).toString();
        try {
            keySet.headSet(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.headSet(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(100).toString();
        assertEquals(0, keySet.headSet(endKey).size());
        assertEquals(0, keySet.headSet(endKey, false).size());
        try {
            keySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(101).toString();
        assertEquals(0, keySet.headSet(endKey).size());
        assertEquals(0, keySet.headSet(endKey).size());
        assertEquals(1, keySet.headSet(endKey, true).size());

        for (int i = 102; i < 109; i++) {
            endKey = new Integer(i).toString();

            headSet = keySet.headSet(endKey);
            iterator = headSet.iterator();
            int j;
            for (j = 101; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i, j);

            headSet = keySet.headSet(endKey, false);
            iterator = headSet.iterator();
            for (j = 101; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i, j);

            headSet = keySet.headSet(endKey, true);
            iterator = headSet.iterator();
            for (j = 101; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i + 1, j);
        }

        endKey = new Integer(109).toString();
        headSet = keySet.headSet(endKey);
        iterator = headSet.iterator();
        for (index = 101; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(109, index);

        headSet = keySet.headSet(endKey, false);
        iterator = headSet.iterator();
        for (index = 101; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(109, index);

        headSet = keySet.headSet(endKey, true);
        iterator = headSet.iterator();
        for (index = 101; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(110, index);

        endKey = new Integer(110).toString();
        try {
            keySet.headSet(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.headSet(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        keySet = ((NavigableMap) subMap_startIncluded_endExcluded_comparator)
                .navigableKeySet();
        endKey = new Integer(99).toString();
        try {
            keySet.headSet(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.headSet(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(100).toString();
        assertEquals(0, keySet.headSet(endKey).size());
        assertEquals(0, keySet.headSet(endKey, false).size());
        assertEquals(1, keySet.headSet(endKey, true).size());

        endKey = new Integer(101).toString();
        headSet = keySet.headSet(endKey);
        iterator = headSet.iterator();
        for (index = 100; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(101, index);

        headSet = keySet.headSet(endKey, false);
        iterator = headSet.iterator();
        for (index = 100; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(101, index);

        headSet = keySet.headSet(endKey, true);
        iterator = headSet.iterator();
        for (index = 100; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(102, index);

        for (int i = 102; i < 109; i++) {
            endKey = new Integer(i).toString();

            headSet = keySet.headSet(endKey);
            iterator = headSet.iterator();
            int j;
            for (j = 100; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i, j);

            headSet = keySet.headSet(endKey, false);
            iterator = headSet.iterator();
            for (j = 100; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i, j);

            headSet = keySet.headSet(endKey, true);
            iterator = headSet.iterator();
            for (j = 100; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i + 1, j);
        }

        endKey = new Integer(109).toString();
        headSet = keySet.headSet(endKey);
        iterator = headSet.iterator();
        for (index = 100; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(109, index);

        headSet = keySet.headSet(endKey, false);
        iterator = headSet.iterator();
        for (index = 100; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(109, index);

        try {
            keySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(110).toString();
        try {
            keySet.headSet(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            keySet.headSet(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            keySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        keySet = ((NavigableMap) subMap_startIncluded_endIncluded_comparator)
                .navigableKeySet();
        endKey = new Integer(99).toString();
        try {
            keySet.headSet(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.headSet(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        endKey = new Integer(100).toString();
        assertEquals(0, keySet.headSet(endKey).size());
        assertEquals(0, keySet.headSet(endKey, false).size());
        assertEquals(1, keySet.headSet(endKey, true).size());

        endKey = new Integer(101).toString();
        headSet = keySet.headSet(endKey);
        iterator = headSet.iterator();
        for (index = 100; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(101, index);

        headSet = keySet.headSet(endKey, false);
        iterator = headSet.iterator();
        for (index = 100; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(101, index);

        headSet = keySet.headSet(endKey, true);
        iterator = headSet.iterator();
        for (index = 100; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(102, index);

        for (int i = 102; i < 109; i++) {
            endKey = new Integer(i).toString();

            headSet = keySet.headSet(endKey);
            iterator = headSet.iterator();
            int j;
            for (j = 100; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i, j);

            headSet = keySet.headSet(endKey, false);
            iterator = headSet.iterator();
            for (j = 100; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i, j);

            headSet = keySet.headSet(endKey, true);
            iterator = headSet.iterator();
            for (j = 100; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(i + 1, j);
        }

        endKey = new Integer(109).toString();
        headSet = keySet.headSet(endKey);
        iterator = headSet.iterator();
        for (index = 100; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(109, index);

        headSet = keySet.headSet(endKey, false);
        iterator = headSet.iterator();
        for (index = 100; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(109, index);

        headSet = keySet.headSet(endKey, true);
        iterator = headSet.iterator();
        for (index = 100; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(110, index);

        endKey = new Integer(110).toString();
        try {
            keySet.headSet(endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.headSet(endKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.headSet(endKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        key = new Integer(1).toString();
        keySet = tm.headMap(key, true).navigableKeySet();
        iterator = keySet.iterator();
        iterator.next();
        endKey = (String) iterator.next();
        headSet = keySet.headSet(endKey, false);
        assertEquals(1, headSet.size());
        headSetIterator = headSet.iterator();
        assertEquals(new Integer(0).toString(), headSetIterator.next());
        assertFalse(headSetIterator.hasNext());
        try {
            headSetIterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }
        try {
            keySet.headSet(null, false);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        headSet = keySet.headSet(endKey, true);
        assertEquals(2, headSet.size());
        headSetIterator = headSet.iterator();
        assertEquals(new Integer(0).toString(), headSetIterator.next());
        assertEquals(new Integer(1).toString(), headSetIterator.next());
        assertFalse(headSetIterator.hasNext());
        try {
            headSetIterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }
        try {
            keySet.headSet(null, false);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

    }

    public void test_AscendingSubMapKeySet_remove() {
        TreeMap tm_rm = new TreeMap(tm);
        SortedMap subMap_startExcluded_endExcluded_rm = tm_rm.subMap(
                objArray[100].toString(), false, objArray[109].toString(),
                false);
        assertNull(subMap_startExcluded_endExcluded_rm.remove("0"));
        try {
            subMap_startExcluded_endExcluded_rm.remove(null);
            fail("should throw NPE");
        } catch (Exception e) {
            // Expected
        }
        for (int i = 101; i < 108; i++) {
            assertNotNull(subMap_startExcluded_endExcluded_rm
                    .remove(new Integer(i).toString()));
        }
    }

    public void test_AscendingSubMapKeySet_tailSet() {
        NavigableSet keySet;
        SortedSet tailSet;
        String startKey, key;
        Iterator iterator;

        keySet = navigableMap_startExcluded_endExcluded.navigableKeySet();
        startKey = new Integer(99).toString();
        try {
            keySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            keySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            keySet.tailSet(startKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        startKey = new Integer(100).toString();
        try {
            keySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        int index;
        tailSet = keySet.tailSet(startKey, false);
        iterator = tailSet.iterator();
        for (index = 101; index < 109; index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }

        startKey = new Integer(101).toString();
        tailSet = keySet.tailSet(startKey);
        iterator = tailSet.iterator();
        for (index = 101; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(109, index);

        tailSet = keySet.tailSet(startKey, true);
        iterator = tailSet.iterator();
        for (index = 101; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(109, index);

        tailSet = keySet.tailSet(startKey, false);
        iterator = tailSet.iterator();
        for (index = 101; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index + 1).toString(), key);
        }
        assertEquals(108, index);

        for (int i = 102; i < 109; i++) {
            startKey = new Integer(i).toString();

            tailSet = keySet.tailSet(startKey);
            iterator = tailSet.iterator();
            int j;
            for (j = i; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(109, j);

            tailSet = keySet.tailSet(startKey, true);
            iterator = tailSet.iterator();
            for (j = i; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(109, j);

            tailSet = keySet.tailSet(startKey, false);
            iterator = tailSet.iterator();
            for (j = i; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j + 1).toString(), key);
            }
            assertEquals(108, j);
        }

        startKey = new Integer(109).toString();
        try {
            keySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        tailSet = keySet.tailSet(startKey, false);
        iterator = tailSet.iterator();
        for (index = 109; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index + 1).toString(), key);
        }
        assertEquals(109, index);

        startKey = new Integer(110).toString();
        try {
            keySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.tailSet(startKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        keySet = navigableMap_startExcluded_endIncluded.navigableKeySet();
        startKey = new Integer(99).toString();
        try {
            keySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.tailSet(startKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        startKey = new Integer(100).toString();
        try {
            keySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        tailSet = keySet.tailSet(startKey, false);
        iterator = tailSet.iterator();
        for (index = 100; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index + 1).toString(), key);
        }
        assertEquals(109, index);

        for (int i = 102; i < 109; i++) {
            startKey = new Integer(i).toString();

            tailSet = keySet.tailSet(startKey);
            iterator = tailSet.iterator();
            int j;
            for (j = i; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(110, j);

            tailSet = keySet.tailSet(startKey, true);
            iterator = tailSet.iterator();
            for (j = i; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(110, j);

            tailSet = keySet.tailSet(startKey, false);
            iterator = tailSet.iterator();
            for (j = i; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j + 1).toString(), key);
            }
            assertEquals(109, j);
        }

        startKey = new Integer(109).toString();
        tailSet = keySet.tailSet(startKey);
        iterator = tailSet.iterator();
        for (index = 109; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(110, index);

        tailSet = keySet.tailSet(startKey, true);
        iterator = tailSet.iterator();
        for (index = 109; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(110, index);

        tailSet = keySet.tailSet(startKey, false);
        iterator = tailSet.iterator();
        for (index = 109; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index + 1).toString(), key);
        }
        assertEquals(109, index);

        startKey = new Integer(110).toString();
        try {
            keySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.tailSet(startKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        keySet = navigableMap_startIncluded_endExcluded.navigableKeySet();
        startKey = new Integer(99).toString();
        try {
            keySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.tailSet(startKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        startKey = new Integer(100).toString();
        tailSet = keySet.tailSet(startKey);
        iterator = tailSet.iterator();
        for (index = 100; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(109, index);

        tailSet = keySet.tailSet(startKey, true);
        iterator = tailSet.iterator();
        for (index = 100; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(109, index);

        tailSet = keySet.tailSet(startKey, false);
        iterator = tailSet.iterator();
        for (index = 100; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index + 1).toString(), key);
        }
        assertEquals(108, index);

        startKey = new Integer(101).toString();
        tailSet = keySet.tailSet(startKey);
        iterator = tailSet.iterator();
        for (index = 101; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(109, index);

        tailSet = keySet.tailSet(startKey, true);
        iterator = tailSet.iterator();
        for (index = 101; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(109, index);

        tailSet = keySet.tailSet(startKey, false);
        iterator = tailSet.iterator();
        for (index = 101; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index + 1).toString(), key);
        }
        assertEquals(108, index);

        for (int i = 102; i < 109; i++) {
            startKey = new Integer(i).toString();

            tailSet = keySet.tailSet(startKey);
            iterator = tailSet.iterator();
            int j;
            for (j = i; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(109, j);

            tailSet = keySet.tailSet(startKey, true);
            iterator = tailSet.iterator();
            for (j = i; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(109, j);

            tailSet = keySet.tailSet(startKey, false);
            iterator = tailSet.iterator();
            for (j = i; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j + 1).toString(), key);
            }
            assertEquals(108, j);
        }

        startKey = new Integer(109).toString();
        try {
            keySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        tailSet = keySet.tailSet(startKey, false);
        iterator = tailSet.iterator();
        for (index = 109; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index + 1).toString(), key);
        }
        assertEquals(109, index);

        startKey = new Integer(110).toString();
        try {
            keySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.tailSet(startKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        keySet = navigableMap_startIncluded_endIncluded.navigableKeySet();
        startKey = new Integer(99).toString();
        try {
            keySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.tailSet(startKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        startKey = new Integer(100).toString();
        tailSet = keySet.tailSet(startKey);
        iterator = tailSet.iterator();
        for (index = 100; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(110, index);

        tailSet = keySet.tailSet(startKey, true);
        iterator = tailSet.iterator();
        for (index = 100; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(110, index);

        tailSet = keySet.tailSet(startKey, false);
        iterator = tailSet.iterator();
        for (index = 100; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index + 1).toString(), key);
        }
        assertEquals(109, index);

        startKey = new Integer(101).toString();
        tailSet = keySet.tailSet(startKey);
        iterator = tailSet.iterator();
        for (index = 101; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(110, index);

        tailSet = keySet.tailSet(startKey, true);
        iterator = tailSet.iterator();
        for (index = 101; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(110, index);

        tailSet = keySet.tailSet(startKey, false);
        iterator = tailSet.iterator();
        for (index = 101; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index + 1).toString(), key);
        }
        assertEquals(109, index);

        for (int i = 102; i < 109; i++) {
            startKey = new Integer(i).toString();

            tailSet = keySet.tailSet(startKey);
            iterator = tailSet.iterator();
            int j;
            for (j = i; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(110, j);

            tailSet = keySet.tailSet(startKey, true);
            iterator = tailSet.iterator();
            for (j = i; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(110, j);

            tailSet = keySet.tailSet(startKey, false);
            iterator = tailSet.iterator();
            for (j = i; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j + 1).toString(), key);
            }
            assertEquals(109, j);
        }

        startKey = new Integer(109).toString();
        tailSet = keySet.tailSet(startKey);
        iterator = tailSet.iterator();
        for (index = 109; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(110, index);

        tailSet = keySet.tailSet(startKey, true);
        iterator = tailSet.iterator();
        for (index = 109; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(110, index);

        tailSet = keySet.tailSet(startKey, false);
        iterator = tailSet.iterator();
        for (index = 109; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index + 1).toString(), key);
        }
        assertEquals(109, index);

        startKey = new Integer(110).toString();
        try {
            keySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.tailSet(startKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        String endKey = new Integer(1).toString();
        keySet = tm.headMap(endKey, true).navigableKeySet();
        iterator = keySet.iterator();
        iterator.next();
        startKey = (String) iterator.next();
        tailSet = keySet.tailSet(startKey);
        assertEquals(1, tailSet.size());
        Iterator tailSetIterator = tailSet.iterator();
        assertEquals(endKey, tailSetIterator.next());
        try {
            tailSetIterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }
        try {
            keySet.tailSet(null);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        tailSet = keySet.tailSet(startKey, true);
        assertEquals(1, tailSet.size());
        tailSetIterator = tailSet.iterator();
        assertEquals(endKey, tailSetIterator.next());

        tailSet = keySet.tailSet(startKey, false);
        assertEquals(0, tailSet.size());
        tailSetIterator = tailSet.iterator();
        try {
            tailSetIterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }
        try {
            keySet.tailSet(null, false);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }
        try {
            keySet.tailSet(null, true);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        // With Comparator
        keySet = ((NavigableMap) subMap_startExcluded_endExcluded_comparator)
                .navigableKeySet();
        startKey = new Integer(99).toString();
        try {
            keySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            keySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            keySet.tailSet(startKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        startKey = new Integer(100).toString();
        try {
            keySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        tailSet = keySet.tailSet(startKey, false);
        iterator = tailSet.iterator();
        for (index = 101; index < 109; index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }

        startKey = new Integer(101).toString();
        tailSet = keySet.tailSet(startKey);
        iterator = tailSet.iterator();
        for (index = 101; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(109, index);

        tailSet = keySet.tailSet(startKey, true);
        iterator = tailSet.iterator();
        for (index = 101; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(109, index);

        tailSet = keySet.tailSet(startKey, false);
        iterator = tailSet.iterator();
        for (index = 101; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index + 1).toString(), key);
        }
        assertEquals(108, index);

        for (int i = 102; i < 109; i++) {
            startKey = new Integer(i).toString();

            tailSet = keySet.tailSet(startKey);
            iterator = tailSet.iterator();
            int j;
            for (j = i; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(109, j);

            tailSet = keySet.tailSet(startKey, true);
            iterator = tailSet.iterator();
            for (j = i; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(109, j);

            tailSet = keySet.tailSet(startKey, false);
            iterator = tailSet.iterator();
            for (j = i; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j + 1).toString(), key);
            }
            assertEquals(108, j);
        }

        startKey = new Integer(109).toString();
        try {
            keySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        tailSet = keySet.tailSet(startKey, false);
        iterator = tailSet.iterator();
        for (index = 109; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index + 1).toString(), key);
        }
        assertEquals(109, index);

        startKey = new Integer(110).toString();
        try {
            keySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.tailSet(startKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        keySet = navigableMap_startExcluded_endIncluded.navigableKeySet();
        startKey = new Integer(99).toString();
        try {
            keySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.tailSet(startKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        startKey = new Integer(100).toString();
        try {
            keySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        tailSet = keySet.tailSet(startKey, false);
        iterator = tailSet.iterator();
        for (index = 100; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index + 1).toString(), key);
        }
        assertEquals(109, index);

        for (int i = 102; i < 109; i++) {
            startKey = new Integer(i).toString();

            tailSet = keySet.tailSet(startKey);
            iterator = tailSet.iterator();
            int j;
            for (j = i; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(110, j);

            tailSet = keySet.tailSet(startKey, true);
            iterator = tailSet.iterator();
            for (j = i; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j).toString(), key);
            }
            assertEquals(110, j);

            tailSet = keySet.tailSet(startKey, false);
            iterator = tailSet.iterator();
            for (j = i; iterator.hasNext(); j++) {
                key = (String) iterator.next();
                assertEquals(new Integer(j + 1).toString(), key);
            }
            assertEquals(109, j);
        }

        startKey = new Integer(109).toString();
        tailSet = keySet.tailSet(startKey);
        iterator = tailSet.iterator();
        for (index = 109; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(110, index);

        tailSet = keySet.tailSet(startKey, true);
        iterator = tailSet.iterator();
        for (index = 109; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index).toString(), key);
        }
        assertEquals(110, index);

        tailSet = keySet.tailSet(startKey, false);
        iterator = tailSet.iterator();
        for (index = 109; iterator.hasNext(); index++) {
            key = (String) iterator.next();
            assertEquals(new Integer(index + 1).toString(), key);
        }
        assertEquals(109, index);

        startKey = new Integer(110).toString();
        try {
            keySet.tailSet(startKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.tailSet(startKey, true);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            keySet.tailSet(startKey, false);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
    }

    public void test_AscendingSubMapKeySet_subSet() {
        NavigableSet keySet;
        SortedSet subSet;
        String startKey, endKey, key;
        Iterator startIterator, endIterator, subSetIterator;

        keySet = navigableMap_startExcluded_endExcluded.navigableKeySet();
        startIterator = keySet.iterator();
        while (startIterator.hasNext()) {
            startKey = (String) startIterator.next();
            endIterator = keySet.iterator();
            while (endIterator.hasNext()) {
                endKey = (String) endIterator.next();
                int startIndex = Integer.valueOf(startKey);
                int endIndex = Integer.valueOf(endKey);
                if (startIndex > endIndex) {
                    try {
                        keySet.subSet(startKey, endKey);
                        fail("should throw IllegalArgumentException");
                    } catch (IllegalArgumentException e) {
                        // Expected
                    }

                    try {
                        keySet.subSet(startKey, false, endKey, false);
                        fail("shoudl throw IllegalArgumentException");
                    } catch (IllegalArgumentException e) {
                        // Expected
                    }

                    try {
                        keySet.subSet(startKey, false, endKey, true);
                        fail("shoudl throw IllegalArgumentException");
                    } catch (IllegalArgumentException e) {
                        // Expected
                    }

                    try {
                        keySet.subSet(startKey, true, endKey, false);
                        fail("shoudl throw IllegalArgumentException");
                    } catch (IllegalArgumentException e) {
                        // Expected
                    }

                    try {
                        keySet.subSet(startKey, true, endKey, true);
                        fail("shoudl throw IllegalArgumentException");
                    } catch (IllegalArgumentException e) {
                        // Expected
                    }
                } else {
                    subSet = keySet.subSet(startKey, endKey);
                    subSetIterator = subSet.iterator();
                    for (int index = startIndex; subSetIterator.hasNext(); index++) {
                        assertEquals(new Integer(index).toString(),
                                subSetIterator.next());
                    }

                    subSet = keySet.subSet(startKey, false, endKey, false);
                    subSetIterator = subSet.iterator();
                    for (int index = startIndex + 1; subSetIterator.hasNext(); index++) {
                        assertEquals(new Integer(index).toString(),
                                subSetIterator.next());
                    }

                    subSet = keySet.subSet(startKey, false, endKey, true);
                    subSetIterator = subSet.iterator();
                    for (int index = startIndex + 1; subSetIterator.hasNext(); index++) {
                        assertEquals(new Integer(index).toString(),
                                subSetIterator.next());
                    }

                    subSet = keySet.subSet(startKey, true, endKey, false);
                    subSetIterator = subSet.iterator();
                    for (int index = startIndex; subSetIterator.hasNext(); index++) {
                        assertEquals(new Integer(index).toString(),
                                subSetIterator.next());
                    }

                    subSet = keySet.subSet(startKey, true, endKey, true);
                    subSetIterator = subSet.iterator();
                    for (int index = startIndex; subSetIterator.hasNext(); index++) {
                        assertEquals(new Integer(index).toString(),
                                subSetIterator.next());
                    }
                }
            }
        }

        key = new Integer(1).toString();
        keySet = tm.headMap(key, true).navigableKeySet();
        Iterator iterator = keySet.iterator();
        startKey = (String) iterator.next();
        endKey = (String) iterator.next();

        subSet = keySet.subSet(startKey, endKey);
        assertEquals(1, subSet.size());
        subSetIterator = subSet.iterator();
        assertEquals(new Integer(0).toString(), subSetIterator.next());
        try {
            subSetIterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        subSet = keySet.subSet(startKey, false, endKey, false);
        assertEquals(0, subSet.size());

        subSet = keySet.subSet(startKey, false, endKey, true);
        assertEquals(1, subSet.size());
        subSetIterator = subSet.iterator();
        assertEquals(new Integer(1).toString(), subSetIterator.next());
        try {
            subSetIterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        subSet = keySet.subSet(startKey, true, endKey, false);
        assertEquals(1, subSet.size());
        subSetIterator = subSet.iterator();
        assertEquals(new Integer(0).toString(), subSetIterator.next());
        try {
            subSetIterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        subSet = keySet.subSet(startKey, true, endKey, true);
        assertEquals(2, subSet.size());
        subSetIterator = subSet.iterator();
        assertEquals(new Integer(0).toString(), subSetIterator.next());
        assertEquals(new Integer(1).toString(), subSetIterator.next());
        try {
            subSetIterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        try {
            keySet.subSet(null, null);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            keySet.subSet(null, false, null, false);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            keySet.subSet(null, false, null, true);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            keySet.subSet(null, true, null, false);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            keySet.subSet(null, true, null, true);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            keySet.subSet(null, endKey);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            keySet.subSet(null, false, endKey, false);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            keySet.subSet(null, false, endKey, true);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            keySet.subSet(null, true, endKey, false);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            keySet.subSet(null, true, endKey, true);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            keySet.subSet(startKey, null);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            keySet.subSet(startKey, false, null, false);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            keySet.subSet(startKey, false, null, true);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            keySet.subSet(startKey, true, null, false);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            keySet.subSet(startKey, true, null, true);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        // With Comparator
        keySet = ((NavigableMap) subMap_startExcluded_endExcluded_comparator)
                .navigableKeySet();
        startIterator = keySet.iterator();
        while (startIterator.hasNext()) {
            startKey = (String) startIterator.next();
            endIterator = keySet.iterator();
            while (endIterator.hasNext()) {
                endKey = (String) endIterator.next();
                int startIndex = Integer.valueOf(startKey);
                int endIndex = Integer.valueOf(endKey);
                if (startIndex > endIndex) {
                    try {
                        keySet.subSet(startKey, endKey);
                        fail("should throw IllegalArgumentException");
                    } catch (IllegalArgumentException e) {
                        // Expected
                    }

                    try {
                        keySet.subSet(startKey, false, endKey, false);
                        fail("shoudl throw IllegalArgumentException");
                    } catch (IllegalArgumentException e) {
                        // Expected
                    }

                    try {
                        keySet.subSet(startKey, false, endKey, true);
                        fail("shoudl throw IllegalArgumentException");
                    } catch (IllegalArgumentException e) {
                        // Expected
                    }

                    try {
                        keySet.subSet(startKey, true, endKey, false);
                        fail("shoudl throw IllegalArgumentException");
                    } catch (IllegalArgumentException e) {
                        // Expected
                    }

                    try {
                        keySet.subSet(startKey, true, endKey, true);
                        fail("shoudl throw IllegalArgumentException");
                    } catch (IllegalArgumentException e) {
                        // Expected
                    }
                } else {
                    subSet = keySet.subSet(startKey, endKey);
                    subSetIterator = subSet.iterator();
                    for (int index = startIndex; subSetIterator.hasNext(); index++) {
                        assertEquals(new Integer(index).toString(),
                                subSetIterator.next());
                    }

                    subSet = keySet.subSet(startKey, false, endKey, false);
                    subSetIterator = subSet.iterator();
                    for (int index = startIndex + 1; subSetIterator.hasNext(); index++) {
                        assertEquals(new Integer(index).toString(),
                                subSetIterator.next());
                    }

                    subSet = keySet.subSet(startKey, false, endKey, true);
                    subSetIterator = subSet.iterator();
                    for (int index = startIndex + 1; subSetIterator.hasNext(); index++) {
                        assertEquals(new Integer(index).toString(),
                                subSetIterator.next());
                    }

                    subSet = keySet.subSet(startKey, true, endKey, false);
                    subSetIterator = subSet.iterator();
                    for (int index = startIndex; subSetIterator.hasNext(); index++) {
                        assertEquals(new Integer(index).toString(),
                                subSetIterator.next());
                    }

                    subSet = keySet.subSet(startKey, true, endKey, true);
                    subSetIterator = subSet.iterator();
                    for (int index = startIndex; subSetIterator.hasNext(); index++) {
                        assertEquals(new Integer(index).toString(),
                                subSetIterator.next());
                    }
                }
            }
        }

        key = new Integer(1).toString();
        keySet = tm.headMap(key, true).navigableKeySet();
        iterator = keySet.iterator();
        startKey = (String) iterator.next();
        endKey = (String) iterator.next();

        subSet = keySet.subSet(startKey, endKey);
        assertEquals(1, subSet.size());
        subSetIterator = subSet.iterator();
        assertEquals(new Integer(0).toString(), subSetIterator.next());
        try {
            subSetIterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        subSet = keySet.subSet(startKey, false, endKey, false);
        assertEquals(0, subSet.size());

        subSet = keySet.subSet(startKey, false, endKey, true);
        assertEquals(1, subSet.size());
        subSetIterator = subSet.iterator();
        assertEquals(new Integer(1).toString(), subSetIterator.next());
        try {
            subSetIterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        subSet = keySet.subSet(startKey, true, endKey, false);
        assertEquals(1, subSet.size());
        subSetIterator = subSet.iterator();
        assertEquals(new Integer(0).toString(), subSetIterator.next());
        try {
            subSetIterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        subSet = keySet.subSet(startKey, true, endKey, true);
        assertEquals(2, subSet.size());
        subSetIterator = subSet.iterator();
        assertEquals(new Integer(0).toString(), subSetIterator.next());
        assertEquals(new Integer(1).toString(), subSetIterator.next());
        try {
            subSetIterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        try {
            keySet.subSet(null, null);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            keySet.subSet(null, false, null, false);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            keySet.subSet(null, false, null, true);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            keySet.subSet(null, true, null, false);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            keySet.subSet(null, true, null, true);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            keySet.subSet(null, endKey);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            keySet.subSet(null, false, endKey, false);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            keySet.subSet(null, false, endKey, true);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            keySet.subSet(null, true, endKey, false);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            keySet.subSet(null, true, endKey, true);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            keySet.subSet(startKey, null);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            keySet.subSet(startKey, false, null, false);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            keySet.subSet(startKey, false, null, true);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            keySet.subSet(startKey, true, null, false);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            keySet.subSet(startKey, true, null, true);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

    }

    public void test_AscendingSubMapKeySet_lower() {
        NavigableSet keySet;
        Iterator iterator;
        String key, lowerKey;
        int value, lowerValue;

        keySet = navigableMap_startExcluded_endExcluded.navigableKeySet();
        iterator = keySet.iterator();
        while (iterator.hasNext()) {
            key = (String) iterator.next();
            value = Integer.valueOf(key);
            lowerKey = (String) keySet.lower(key);
            if (value > 101) {
                lowerValue = Integer.valueOf(lowerKey);
                assertEquals(value - 1, lowerValue);
            } else {
                assertNull(lowerKey);
            }
        }

        keySet = navigableMap_startExcluded_endIncluded.navigableKeySet();
        iterator = keySet.iterator();
        while (iterator.hasNext()) {
            key = (String) iterator.next();
            value = Integer.valueOf(key);
            lowerKey = (String) keySet.lower(key);
            if (value > 101) {
                lowerValue = Integer.valueOf(lowerKey);
                assertEquals(value - 1, lowerValue);
            } else {
                assertNull(lowerKey);
            }
        }

        keySet = navigableMap_startIncluded_endExcluded.navigableKeySet();
        iterator = keySet.iterator();
        while (iterator.hasNext()) {
            key = (String) iterator.next();
            value = Integer.valueOf(key);
            lowerKey = (String) keySet.lower(key);
            if (value > 100) {
                lowerValue = Integer.valueOf(lowerKey);
                assertEquals(value - 1, lowerValue);
            } else {
                assertNull(lowerKey);
            }
        }

        keySet = navigableMap_startIncluded_endIncluded.navigableKeySet();
        iterator = keySet.iterator();
        while (iterator.hasNext()) {
            key = (String) iterator.next();
            value = Integer.valueOf(key);
            lowerKey = (String) keySet.lower(key);
            if (value > 100) {
                lowerValue = Integer.valueOf(lowerKey);
                assertEquals(value - 1, lowerValue);
            } else {
                assertNull(lowerKey);
            }
        }

        key = new Integer(2).toString();
        keySet = tm.headMap(key, true).navigableKeySet();
        iterator = keySet.iterator();
        iterator.next();// 0
        String expectedLowerKey = (String) iterator.next();// 1
        assertEquals(expectedLowerKey, keySet.lower(iterator.next()));

        try {
            keySet.lower(null);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        key = new Integer(0).toString();
        keySet = tm.headMap(key, true).navigableKeySet();
        assertNull(keySet.lower(key));

        key = new Integer(0).toString();
        keySet = tm.headMap(key, false).navigableKeySet();
        assertNull(keySet.lower(key));

        key = new Integer(999).toString();
        keySet = tm.headMap(key, true).navigableKeySet();
        assertNotNull(keySet.lower(key));

        key = new Integer(999).toString();
        keySet = tm.headMap(key, false).navigableKeySet();
        assertNotNull(keySet.lower(key));
    }

    public void test_AscendingSubMapKeySet_higher() {
        NavigableSet keySet;
        Iterator iterator;
        String key, lowerKey;
        int value, lowerValue;

        keySet = navigableMap_startExcluded_endExcluded.navigableKeySet();
        iterator = keySet.iterator();
        while (iterator.hasNext()) {
            key = (String) iterator.next();
            value = Integer.valueOf(key);
            lowerKey = (String) keySet.higher(key);
            if (value < 108) {
                lowerValue = Integer.valueOf(lowerKey);
                assertEquals(value + 1, lowerValue);
            } else {
                assertNull(lowerKey);
            }
        }

        keySet = navigableMap_startExcluded_endIncluded.navigableKeySet();
        iterator = keySet.iterator();
        while (iterator.hasNext()) {
            key = (String) iterator.next();
            value = Integer.valueOf(key);
            lowerKey = (String) keySet.higher(key);
            if (value < 109) {
                lowerValue = Integer.valueOf(lowerKey);
                assertEquals(value + 1, lowerValue);
            } else {
                assertNull(lowerKey);
            }
        }

        keySet = navigableMap_startIncluded_endExcluded.navigableKeySet();
        iterator = keySet.iterator();
        while (iterator.hasNext()) {
            key = (String) iterator.next();
            value = Integer.valueOf(key);
            lowerKey = (String) keySet.higher(key);
            if (value < 108) {
                lowerValue = Integer.valueOf(lowerKey);
                assertEquals(value + 1, lowerValue);
            } else {
                assertNull(lowerKey);
            }
        }

        keySet = navigableMap_startIncluded_endIncluded.navigableKeySet();
        iterator = keySet.iterator();
        while (iterator.hasNext()) {
            key = (String) iterator.next();
            value = Integer.valueOf(key);
            lowerKey = (String) keySet.higher(key);
            if (value < 109) {
                lowerValue = Integer.valueOf(lowerKey);
                assertEquals(value + 1, lowerValue);
            } else {
                assertNull(lowerKey);
            }
        }

        key = new Integer(2).toString();
        keySet = tm.headMap(key, true).navigableKeySet();
        iterator = keySet.iterator();
        iterator.next();// 0
        iterator.next();// 1
        lowerKey = (String) keySet.higher(iterator.next());
        String expectedLowerKey = (String) iterator.next();
        assertEquals(expectedLowerKey, lowerKey);

        try {
            keySet.higher(null);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        key = new Integer(0).toString();
        keySet = tm.headMap(key, true).navigableKeySet();
        assertNull(keySet.higher(key));

        key = new Integer(0).toString();
        keySet = tm.headMap(key, false).navigableKeySet();
        assertNull(keySet.higher(key));

        key = new Integer(999).toString();
        keySet = tm.headMap(key, true).navigableKeySet();
        assertNull(keySet.higher(key));

        key = new Integer(999).toString();
        keySet = tm.headMap(key, false).navigableKeySet();
        assertNull(keySet.higher(key));
    }

    public void test_AscendingSubMapKeySet_ceiling() {
        NavigableSet keySet;
        String key;
        String[] keyArray;

        keySet = navigableMap_startExcluded_endExcluded.navigableKeySet();
        keyArray = (String[]) keySet.toArray(new String[keySet.size()]);
        for (int i = 0, j = 101; i < keyArray.length; i++) {
            key = (String) keySet.ceiling(keyArray[i]);
            assertEquals(new Integer(i + j).toString(), key);
        }

        keySet = navigableMap_startExcluded_endIncluded.navigableKeySet();
        keyArray = (String[]) keySet.toArray(new String[keySet.size()]);
        for (int i = 0, j = 101; i < keyArray.length; i++) {
            key = (String) keySet.ceiling(keyArray[i]);
            assertEquals(new Integer(i + j).toString(), key);
        }

        keySet = navigableMap_startIncluded_endExcluded.navigableKeySet();
        keyArray = (String[]) keySet.toArray(new String[keySet.size()]);
        for (int i = 0, j = 100; i < keyArray.length; i++) {
            key = (String) keySet.ceiling(keyArray[i]);
            assertEquals(new Integer(i + j).toString(), key);
        }

        keySet = navigableMap_startIncluded_endIncluded.navigableKeySet();
        keyArray = (String[]) keySet.toArray(new String[keySet.size()]);
        for (int i = 0, j = 100; i < keyArray.length; i++) {
            key = (String) keySet.ceiling(keyArray[i]);
            assertEquals(new Integer(i + j).toString(), key);
        }

        key = new Integer(2).toString();
        keySet = tm.headMap(key, true).navigableKeySet();
        Iterator iterator = keySet.iterator();
        iterator.next();
        assertEquals(new Integer(1).toString(), keySet.ceiling(iterator.next()));

        try {
            keySet.ceiling(null);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        key = new Integer(0).toString();
        keySet = tm.headMap(key, true).navigableKeySet();
        assertEquals(key, keySet.ceiling(key));

        key = new Integer(0).toString();
        keySet = tm.headMap(key, false).navigableKeySet();
        assertNull(keySet.higher(key));

        key = new Integer(999).toString();
        keySet = tm.headMap(key, true).navigableKeySet();
        assertNull(keySet.higher(key));

        key = new Integer(999).toString();
        keySet = tm.headMap(key, false).navigableKeySet();
        assertNull(keySet.higher(key));
    }

    public void test_AscendingSubMapKeySet_floor() {
        NavigableSet keySet;
        String key;
        String[] keyArray;

        keySet = navigableMap_startExcluded_endExcluded.navigableKeySet();
        keyArray = (String[]) keySet.toArray(new String[keySet.size()]);
        for (int i = 0, j = 101; i < keyArray.length; i++) {
            key = (String) keySet.floor(keyArray[i]);
            assertEquals(new Integer(i + j).toString(), key);
        }

        keySet = navigableMap_startExcluded_endIncluded.navigableKeySet();
        keyArray = (String[]) keySet.toArray(new String[keySet.size()]);
        for (int i = 0, j = 101; i < keyArray.length; i++) {
            key = (String) keySet.floor(keyArray[i]);
            assertEquals(new Integer(i + j).toString(), key);
        }

        keySet = navigableMap_startIncluded_endExcluded.navigableKeySet();
        keyArray = (String[]) keySet.toArray(new String[keySet.size()]);
        for (int i = 0, j = 100; i < keyArray.length; i++) {
            key = (String) keySet.floor(keyArray[i]);
            assertEquals(new Integer(i + j).toString(), key);
        }

        keySet = navigableMap_startIncluded_endIncluded.navigableKeySet();
        keyArray = (String[]) keySet.toArray(new String[keySet.size()]);
        for (int i = 0, j = 100; i < keyArray.length; i++) {
            key = (String) keySet.floor(keyArray[i]);
            assertEquals(new Integer(i + j).toString(), key);
        }

        key = new Integer(2).toString();
        keySet = tm.headMap(key, true).navigableKeySet();
        Iterator iterator = keySet.iterator();
        iterator.next();
        assertEquals(new Integer(1).toString(), keySet.floor(iterator.next()));

        try {
            keySet.floor(null);
            fail("should throw NPE");
        } catch (NullPointerException e) {
            // Expected
        }

        key = new Integer(0).toString();
        keySet = tm.headMap(key, true).navigableKeySet();
        assertEquals(key, keySet.floor(key));

        key = new Integer(0).toString();
        keySet = tm.headMap(key, false).navigableKeySet();
        assertNull(keySet.floor(key));

        key = new Integer(999).toString();
        keySet = tm.headMap(key, true).navigableKeySet();
        assertEquals(key, keySet.floor(key));

        key = new Integer(999).toString();
        keySet = tm.headMap(key, false).navigableKeySet();
        assertEquals(new Integer(998).toString(), keySet.floor(key));
    }

    public void test_BoundedEntryIterator_next() {
        Iterator iterator = subMap_default.entrySet().iterator();
        assertTrue(iterator.hasNext());
        for (int i = 100; iterator.hasNext(); i++) {
            assertEquals(i, ((Entry) iterator.next()).getValue());
        }

        try {
            iterator.next();
            fail("should throw java.util.NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

    }

    public void test_BoundedKeyIterator_next() {
        Iterator iterator = subMap_default.keySet().iterator();
        assertTrue(iterator.hasNext());
        for (int i = 100; iterator.hasNext(); i++) {
            assertEquals(new Integer(i).toString(), iterator.next());
        }

        try {
            iterator.next();
            fail("should throw java.util.NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }
    }

    public void test_BoundedValueIterator_next() {
        String startKey = new Integer(101).toString();
        String endKey = new Integer(108).toString();

        Collection values = tm.subMap(startKey, endKey).values();
        Iterator iter = values.iterator();
        for (int i = 101; i < 108; i++) {
            assertEquals(i, iter.next());
        }
        try {
            iter.next();
            fail("should throw java.util.NoSuchElementException");
        } catch (Exception e) {
            // Expected
        }
    }

    /*
     * SubMapEntrySet
     */
    public void test_SubMapEntrySet_Constructor() {
    }

    public void test_SubMapEntrySet_contains() {
        // covered in test_SubMapEntrySet_remove
    }

    public void test_SubMapEntrySet_iterator() {
        Set entrySet = subMap_default.entrySet();
        Iterator iterator;
        Entry entry;
        Integer value = new Integer(100);
        for (iterator = entrySet.iterator(); iterator.hasNext(); value++) {
            entry = (Entry) iterator.next();
            assertEquals(value.toString(), entry.getKey());
            assertEquals(value, entry.getValue());
        }
        assertEquals(109, value.intValue());
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        entrySet = subMap_startExcluded_endExcluded.entrySet();
        value = new Integer(101);
        for (iterator = entrySet.iterator(); iterator.hasNext(); value++) {
            entry = (Entry) iterator.next();
            assertEquals(value.toString(), entry.getKey());
            assertEquals(value, entry.getValue());
        }
        assertEquals(109, value.intValue());
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        entrySet = subMap_startExcluded_endIncluded.entrySet();
        value = new Integer(101);
        for (iterator = entrySet.iterator(); iterator.hasNext(); value++) {
            entry = (Entry) iterator.next();
            assertEquals(value.toString(), entry.getKey());
            assertEquals(value, entry.getValue());
        }
        assertEquals(110, value.intValue());
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        entrySet = subMap_startIncluded_endExcluded.entrySet();
        value = new Integer(100);
        for (iterator = entrySet.iterator(); iterator.hasNext(); value++) {
            entry = (Entry) iterator.next();
            assertEquals(value.toString(), entry.getKey());
            assertEquals(value, entry.getValue());
        }
        assertEquals(109, value.intValue());
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        entrySet = subMap_startIncluded_endIncluded.entrySet();
        value = new Integer(100);
        for (iterator = entrySet.iterator(); iterator.hasNext(); value++) {
            entry = (Entry) iterator.next();
            assertEquals(value.toString(), entry.getKey());
            assertEquals(value, entry.getValue());
        }
        assertEquals(110, value.intValue());
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        String startKey = new Integer(-1).toString();
        String endKey = new Integer(0).toString();
        SortedMap subMap = tm.subMap(startKey, endKey);
        entrySet = subMap.entrySet();
        iterator = entrySet.iterator();
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        endKey = new Integer(1).toString();
        subMap = tm.subMap(startKey, endKey);
        entrySet = subMap.entrySet();
        iterator = entrySet.iterator();
        assertEquals(0, ((Entry) iterator.next()).getValue());
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        endKey = new Integer(2000).toString();
        subMap = tm.subMap(startKey, endKey);
        entrySet = subMap.entrySet();
        iterator = entrySet.iterator();
        for (int i = 0; i < subMap.size(); i++) {
            iterator.next();
        }
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        startKey = new Integer(9).toString();
        endKey = new Integer(100).toString();
        try {
            tm.subMap(startKey, endKey);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        // With Comparator
        entrySet = subMap_default_comparator.entrySet();
        value = new Integer(100);
        for (iterator = entrySet.iterator(); iterator.hasNext(); value++) {
            entry = (Entry) iterator.next();
            assertEquals(value.toString(), entry.getKey());
            assertEquals(value, entry.getValue());
        }
        assertEquals(109, value.intValue());
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        entrySet = subMap_startExcluded_endExcluded_comparator.entrySet();
        value = new Integer(101);
        for (iterator = entrySet.iterator(); iterator.hasNext(); value++) {
            entry = (Entry) iterator.next();
            assertEquals(value.toString(), entry.getKey());
            assertEquals(value, entry.getValue());
        }
        assertEquals(109, value.intValue());
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        entrySet = subMap_startExcluded_endIncluded_comparator.entrySet();
        value = new Integer(101);
        for (iterator = entrySet.iterator(); iterator.hasNext(); value++) {
            entry = (Entry) iterator.next();
            assertEquals(value.toString(), entry.getKey());
            assertEquals(value, entry.getValue());
        }
        assertEquals(110, value.intValue());
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        entrySet = subMap_startIncluded_endExcluded_comparator.entrySet();
        value = new Integer(100);
        for (iterator = entrySet.iterator(); iterator.hasNext(); value++) {
            entry = (Entry) iterator.next();
            assertEquals(value.toString(), entry.getKey());
            assertEquals(value, entry.getValue());
        }
        assertEquals(109, value.intValue());
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        entrySet = subMap_startIncluded_endIncluded_comparator.entrySet();
        value = new Integer(100);
        for (iterator = entrySet.iterator(); iterator.hasNext(); value++) {
            entry = (Entry) iterator.next();
            assertEquals(value.toString(), entry.getKey());
            assertEquals(value, entry.getValue());
        }
        assertEquals(110, value.intValue());
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }
    }

    public void test_SubMapEntrySet_remove() {
        Set entrySet = subMap_default.entrySet();
        assertFalse(entrySet.remove(null));
        int size = entrySet.size();
        for (int i = 0; i < size; i++) {
            Iterator iterator = entrySet.iterator();
            assertTrue(entrySet.remove(iterator.next()));
        }

        entrySet = subMap_startExcluded_endExcluded.entrySet();
        assertFalse(entrySet.remove(null));
        size = entrySet.size();
        for (int i = 0; i < size; i++) {
            Iterator iterator = entrySet.iterator();
            assertTrue(entrySet.remove(iterator.next()));
        }

        entrySet = subMap_startExcluded_endIncluded.entrySet();
        assertFalse(entrySet.remove(null));
        size = entrySet.size();
        for (int i = 0; i < size; i++) {
            Iterator iterator = entrySet.iterator();
            assertTrue(entrySet.remove(iterator.next()));
        }

        entrySet = subMap_startIncluded_endExcluded.entrySet();
        assertFalse(entrySet.remove(null));
        size = entrySet.size();
        for (int i = 0; i < size; i++) {
            Iterator iterator = entrySet.iterator();
            assertTrue(entrySet.remove(iterator.next()));
        }

        entrySet = subMap_startIncluded_endIncluded.entrySet();
        assertFalse(entrySet.remove(null));
        size = entrySet.size();
        for (int i = 0; i < size; i++) {
            Iterator iterator = entrySet.iterator();
            assertTrue(entrySet.remove(iterator.next()));
        }
    }

    public void test_SubMapEntrySet_isEmpty() {
        assertFalse(subMap_default.entrySet().isEmpty());
        assertFalse(subMap_startExcluded_endExcluded.entrySet().isEmpty());
        assertFalse(subMap_startExcluded_endIncluded.entrySet().isEmpty());
        assertFalse(subMap_startIncluded_endExcluded.entrySet().isEmpty());
        assertFalse(subMap_startIncluded_endIncluded.entrySet().isEmpty());

        String startKey = new Integer(0).toString();
        String endKey = startKey;
        SortedMap subMap = tm.subMap(startKey, endKey);
        assertTrue(subMap.entrySet().isEmpty());

        startKey = new Integer(-1).toString();
        subMap = tm.subMap(startKey, endKey);
        assertTrue(subMap.entrySet().isEmpty());

        endKey = new Integer(1).toString();
        subMap = tm.subMap(startKey, endKey);
        assertFalse(subMap.entrySet().isEmpty());
    }

    public void test_SubMapEntrySet_size() {
        assertEquals(9, subMap_default.entrySet().size());
        assertEquals(8, subMap_startExcluded_endExcluded.entrySet().size());
        assertEquals(9, subMap_startExcluded_endIncluded.entrySet().size());
        assertEquals(9, subMap_startIncluded_endExcluded.entrySet().size());
        assertEquals(10, subMap_startIncluded_endIncluded.entrySet().size());

        String startKey = new Integer(0).toString();
        String endKey = new Integer(2).toString();
        SortedMap subMap = tm.subMap(startKey, endKey);
        assertEquals(112, subMap.entrySet().size());

        startKey = new Integer(0).toString();
        endKey = startKey;
        subMap = tm.subMap(startKey, endKey);
        assertEquals(0, subMap.entrySet().size());

        startKey = new Integer(-1).toString();
        endKey = startKey;
        subMap = tm.subMap(startKey, endKey);
        assertEquals(0, subMap.entrySet().size());

        endKey = new Integer(1).toString();
        subMap = tm.subMap(startKey, endKey);
        assertEquals(1, subMap.entrySet().size());

        startKey = new Integer(999).toString();
        endKey = startKey;
        subMap = tm.subMap(startKey, endKey);
        assertEquals(0, subMap.entrySet().size());
    }

    /*
     * SubMapKeySet
     */
    public void test_SubMapKeySet_Constructor() {
        // covered in other test
    }

    public void test_SubMapKeySet_iterator() {
        Set keySet = subMap_default.keySet();
        Iterator iterator = keySet.iterator();
        for (int i = 0; i < keySet.size(); i++) {
            assertEquals(new Integer(100 + i).toString(), iterator.next());
        }
        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        keySet = subMap_startExcluded_endExcluded.keySet();
        iterator = keySet.iterator();
        for (int i = 0; i < keySet.size(); i++) {
            assertEquals(new Integer(101 + i).toString(), iterator.next());
        }
        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        keySet = subMap_startExcluded_endIncluded.keySet();
        iterator = keySet.iterator();
        for (int i = 0; i < keySet.size(); i++) {
            assertEquals(new Integer(101 + i).toString(), iterator.next());
        }
        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        keySet = subMap_startIncluded_endExcluded.keySet();
        iterator = keySet.iterator();
        for (int i = 0; i < keySet.size(); i++) {
            assertEquals(new Integer(100 + i).toString(), iterator.next());
        }
        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        keySet = subMap_startIncluded_endIncluded.keySet();
        iterator = keySet.iterator();
        for (int i = 0; i < keySet.size(); i++) {
            assertEquals(new Integer(100 + i).toString(), iterator.next());
        }
        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        // With Comparator
        keySet = subMap_default_comparator.keySet();
        iterator = keySet.iterator();
        for (int i = 0; i < keySet.size(); i++) {
            assertEquals(new Integer(100 + i).toString(), iterator.next());
        }
        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        keySet = subMap_startExcluded_endExcluded_comparator.keySet();
        iterator = keySet.iterator();
        for (int i = 0; i < keySet.size(); i++) {
            assertEquals(new Integer(101 + i).toString(), iterator.next());
        }
        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        keySet = subMap_startExcluded_endIncluded_comparator.keySet();
        iterator = keySet.iterator();
        for (int i = 0; i < keySet.size(); i++) {
            assertEquals(new Integer(101 + i).toString(), iterator.next());
        }
        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        keySet = subMap_startIncluded_endExcluded_comparator.keySet();
        iterator = keySet.iterator();
        for (int i = 0; i < keySet.size(); i++) {
            assertEquals(new Integer(100 + i).toString(), iterator.next());
        }
        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }

        keySet = subMap_startIncluded_endIncluded_comparator.keySet();
        iterator = keySet.iterator();
        for (int i = 0; i < keySet.size(); i++) {
            assertEquals(new Integer(100 + i).toString(), iterator.next());
        }
        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            fail("should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }
    }

    public void test_SubMapKeySet_isEmpty() {
        assertFalse(subMap_default.keySet().isEmpty());
        assertFalse(subMap_startExcluded_endExcluded.keySet().isEmpty());
        assertFalse(subMap_startExcluded_endIncluded.keySet().isEmpty());
        assertFalse(subMap_startIncluded_endExcluded.keySet().isEmpty());
        assertFalse(subMap_startIncluded_endIncluded.keySet().isEmpty());

        String startKey = new Integer(0).toString();
        String endKey = startKey;
        SortedMap subMap = tm.subMap(startKey, endKey);
        assertTrue(subMap.keySet().isEmpty());

        startKey = new Integer(999).toString();
        endKey = startKey;
        subMap = tm.subMap(startKey, endKey);
        assertTrue(subMap.keySet().isEmpty());

        startKey = new Integer(-1).toString();
        endKey = new Integer(1).toString();
        subMap = tm.subMap(startKey, endKey);
        assertFalse(subMap.keySet().isEmpty());

        endKey = new Integer(0).toString();
        subMap = tm.subMap(startKey, endKey);
        assertTrue(subMap.keySet().isEmpty());
    }

    public void test_SubMapKeySet_contains() {
        Set keySet = subMap_default.keySet();
        try {
            keySet.contains(null);
            fail("should throw NullPointerException");
        } catch (NullPointerException e) {
            // Expected
        }
        String key = new Integer(-1).toString();
        assertFalse(keySet.contains(key));
        key = new Integer(99).toString();
        assertFalse(keySet.contains(key));
        key = new Integer(100).toString();
        assertTrue(keySet.contains(key));
        for (int i = 101; i < 109; i++) {
            key = new Integer(i).toString();
            assertTrue(keySet.contains(key));
        }
        key = new Integer(109).toString();
        assertFalse(keySet.contains(key));
        key = new Integer(110).toString();
        assertFalse(keySet.contains(key));
        key = new Integer(1001).toString();
        assertFalse(keySet.contains(key));

        keySet = subMap_startExcluded_endExcluded.keySet();
        try {
            keySet.contains(null);
            fail("should throw NullPointerException");
        } catch (NullPointerException e) {
            // Expected
        }
        key = new Integer(-1).toString();
        assertFalse(keySet.contains(key));
        key = new Integer(99).toString();
        assertFalse(keySet.contains(key));
        key = new Integer(100).toString();
        assertFalse(keySet.contains(key));
        for (int i = 101; i < 109; i++) {
            key = new Integer(i).toString();
            assertTrue(keySet.contains(key));
        }
        key = new Integer(109).toString();
        assertFalse(keySet.contains(key));
        key = new Integer(110).toString();
        assertFalse(keySet.contains(key));
        key = new Integer(1001).toString();
        assertFalse(keySet.contains(key));

        keySet = subMap_startExcluded_endIncluded.keySet();
        try {
            keySet.contains(null);
            fail("should throw NullPointerException");
        } catch (NullPointerException e) {
            // Expected
        }
        key = new Integer(-1).toString();
        assertFalse(keySet.contains(key));
        key = new Integer(99).toString();
        assertFalse(keySet.contains(key));
        key = new Integer(100).toString();
        assertFalse(keySet.contains(key));
        for (int i = 101; i < 109; i++) {
            key = new Integer(i).toString();
            assertTrue(keySet.contains(key));
        }
        key = new Integer(109).toString();
        assertTrue(keySet.contains(key));
        key = new Integer(110).toString();
        assertFalse(keySet.contains(key));
        key = new Integer(1001).toString();
        assertFalse(keySet.contains(key));

        keySet = subMap_startIncluded_endExcluded.keySet();
        try {
            keySet.contains(null);
            fail("should throw NullPointerException");
        } catch (NullPointerException e) {
            // Expected
        }
        key = new Integer(-1).toString();
        assertFalse(keySet.contains(key));
        key = new Integer(99).toString();
        assertFalse(keySet.contains(key));
        key = new Integer(100).toString();
        assertTrue(keySet.contains(key));
        for (int i = 101; i < 109; i++) {
            key = new Integer(i).toString();
            assertTrue(keySet.contains(key));
        }
        key = new Integer(109).toString();
        assertFalse(keySet.contains(key));
        key = new Integer(110).toString();
        assertFalse(keySet.contains(key));
        key = new Integer(1001).toString();
        assertFalse(keySet.contains(key));

        keySet = subMap_startIncluded_endIncluded.keySet();
        try {
            keySet.contains(null);
            fail("should throw NullPointerException");
        } catch (NullPointerException e) {
            // Expected
        }
        key = new Integer(-1).toString();
        assertFalse(keySet.contains(key));
        key = new Integer(99).toString();
        assertFalse(keySet.contains(key));
        key = new Integer(100).toString();
        assertTrue(keySet.contains(key));
        for (int i = 101; i < 109; i++) {
            key = new Integer(i).toString();
            assertTrue(keySet.contains(key));
        }
        key = new Integer(109).toString();
        assertTrue(keySet.contains(key));
        key = new Integer(110).toString();
        assertFalse(keySet.contains(key));
        key = new Integer(1001).toString();
        assertFalse(keySet.contains(key));
    }

    public void test_SubMapKeySet_size() {
        assertEquals(9, subMap_default.keySet().size());
        assertEquals(8, subMap_startExcluded_endExcluded.keySet().size());
        assertEquals(9, subMap_startExcluded_endIncluded.keySet().size());
        assertEquals(9, subMap_startIncluded_endExcluded.keySet().size());
        assertEquals(10, subMap_startIncluded_endIncluded.keySet().size());

        String startKey = new Integer(0).toString();
        String endKey = new Integer(2).toString();
        SortedMap subMap = tm.subMap(startKey, endKey);
        assertEquals(112, subMap.keySet().size());

        startKey = new Integer(0).toString();
        endKey = startKey;
        subMap = tm.subMap(startKey, endKey);
        assertEquals(0, subMap.keySet().size());

        startKey = new Integer(-1).toString();
        endKey = startKey;
        subMap = tm.subMap(startKey, endKey);
        assertEquals(0, subMap.keySet().size());

        endKey = new Integer(1).toString();
        subMap = tm.subMap(startKey, endKey);
        assertEquals(1, subMap.keySet().size());

        startKey = new Integer(999).toString();
        endKey = startKey;
        subMap = tm.subMap(startKey, endKey);
        assertEquals(0, subMap.keySet().size());
    }

    public void test_SubMapKeySet_remove() {
        Set keySet = subMap_default.keySet();
        try {
            keySet.remove(null);
            fail("should throw NullPointerException");
        } catch (NullPointerException e) {
            // Expected
        }
        int size = keySet.size();
        for (int i = 0; i < size; i++) {
            Iterator iterator = keySet.iterator();
            assertTrue(keySet.remove(iterator.next()));
        }

        keySet = subMap_startExcluded_endExcluded.keySet();
        try {
            keySet.remove(null);
            fail("should throw NullPointerException");
        } catch (NullPointerException e) {
            // Expected
        }
        size = keySet.size();
        for (int i = 0; i < size; i++) {
            Iterator iterator = keySet.iterator();
            assertTrue(keySet.remove(iterator.next()));
        }

        keySet = subMap_startExcluded_endIncluded.keySet();
        try {
            keySet.remove(null);
            fail("should throw NullPointerException");
        } catch (NullPointerException e) {
            // Expected
        }
        size = keySet.size();
        for (int i = 0; i < size; i++) {
            Iterator iterator = keySet.iterator();
            assertTrue(keySet.remove(iterator.next()));
        }

        keySet = subMap_startIncluded_endExcluded.keySet();
        try {
            keySet.remove(null);
            fail("should throw NullPointerException");
        } catch (NullPointerException e) {
            // Expected
        }
        size = keySet.size();
        for (int i = 0; i < size; i++) {
            Iterator iterator = keySet.iterator();
            assertTrue(keySet.remove(iterator.next()));
        }

        keySet = subMap_startIncluded_endIncluded.keySet();
        try {
            keySet.remove(null);
            fail("should throw NullPointerException");
        } catch (NullPointerException e) {
            // Expected
        }
        size = keySet.size();
        for (int i = 0; i < size; i++) {
            Iterator iterator = keySet.iterator();
            assertTrue(keySet.remove(iterator.next()));
        }
    }

    /*
     * AscendingSubMapEntrySet
     */

    public void test_AscendingSubMapEntrySet_comparator() {
        Set entrySet;
        NavigableSet ascendingSubMapEntrySet;

        entrySet = navigableMap_startExcluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            assertNull(ascendingSubMapEntrySet.comparator());
        }

        entrySet = navigableMap_startExcluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            assertNull(ascendingSubMapEntrySet.comparator());
        }

        entrySet = navigableMap_startIncluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            assertNull(ascendingSubMapEntrySet.comparator());
        }

        entrySet = navigableMap_startIncluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            assertNull(ascendingSubMapEntrySet.comparator());
        }
    }

    public void test_AscendingSubMapEntrySet_descendingSet() {
        Set entrySet;
        NavigableSet ascendingSubMapEntrySet, descendingSet;
        Entry entry;
        int value;
        Iterator iterator;

        entrySet = navigableMap_startExcluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            descendingSet = ascendingSubMapEntrySet.descendingSet();
            iterator = descendingSet.iterator();
            assertTrue(iterator.hasNext());
            for (value = 108; iterator.hasNext(); value--) {
                entry = (Entry) iterator.next();
                assertEquals(value, entry.getValue());
            }
            assertEquals(100, value);
        }

        entrySet = navigableMap_startExcluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            descendingSet = ascendingSubMapEntrySet.descendingSet();
            iterator = descendingSet.iterator();
            assertTrue(iterator.hasNext());
            for (value = 109; iterator.hasNext(); value--) {
                entry = (Entry) iterator.next();
                assertEquals(value, entry.getValue());
            }
            assertEquals(100, value);
        }

        entrySet = navigableMap_startIncluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            descendingSet = ascendingSubMapEntrySet.descendingSet();
            iterator = descendingSet.iterator();
            assertTrue(iterator.hasNext());
            for (value = 108; iterator.hasNext(); value--) {
                entry = (Entry) iterator.next();
                assertEquals(value, entry.getValue());
            }
            assertEquals(99, value);
        }

        entrySet = navigableMap_startIncluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            descendingSet = ascendingSubMapEntrySet.descendingSet();
            iterator = descendingSet.iterator();
            assertTrue(iterator.hasNext());
            for (value = 109; iterator.hasNext(); value--) {
                entry = (Entry) iterator.next();
                assertEquals(value, entry.getValue());
            }
            assertEquals(99, value);
        }
    }

    public void test_AscendingSubMapEntrySet_descendingIterator() {
        Set entrySet;
        NavigableSet ascendingSubMapEntrySet;
        Iterator iterator;
        Entry entry;
        int value;

        entrySet = navigableMap_startExcluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = ascendingSubMapEntrySet.descendingIterator();
            assertTrue(iterator.hasNext());
            for (value = 108; iterator.hasNext(); value--) {
                entry = (Entry) iterator.next();
                assertEquals(value, entry.getValue());
            }
            assertEquals(100, value);
        }

        entrySet = navigableMap_startExcluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = ascendingSubMapEntrySet.descendingIterator();
            assertTrue(iterator.hasNext());
            for (value = 109; iterator.hasNext(); value--) {
                entry = (Entry) iterator.next();
                assertEquals(value, entry.getValue());
            }
            assertEquals(100, value);
        }

        entrySet = navigableMap_startIncluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = ascendingSubMapEntrySet.descendingIterator();
            assertTrue(iterator.hasNext());
            for (value = 108; iterator.hasNext(); value--) {
                entry = (Entry) iterator.next();
                assertEquals(value, entry.getValue());
            }
            assertEquals(99, value);
        }

        entrySet = navigableMap_startIncluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = ascendingSubMapEntrySet.descendingIterator();
            assertTrue(iterator.hasNext());
            for (value = 109; iterator.hasNext(); value--) {
                entry = (Entry) iterator.next();
                assertEquals(value, entry.getValue());
            }
            assertEquals(99, value);
        }

        String startKey = new Integer(2).toString();
        entrySet = tm.headMap(startKey, true).entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = ascendingSubMapEntrySet.descendingIterator();
            assertTrue(iterator.hasNext());
            assertEquals(2, ((Entry) iterator.next()).getValue());
        }
    }

    public void test_AscendingSubMapEntrySet_pollFirst_startExcluded_endExcluded() {
        Set entrySet = navigableMap_startExcluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            NavigableSet ascendingSubMapEntrySet = (NavigableSet) entrySet;
            for (int value = 101; value < 109; value++) {
                Entry entry = (Entry) ascendingSubMapEntrySet.pollFirst();
                assertEquals(value, entry.getValue());
            }
            assertTrue(ascendingSubMapEntrySet.isEmpty());
            // should return null if the set is empty.
            assertNull(ascendingSubMapEntrySet.pollFirst());
        }
    }

    public void test_AscendingSubMapEntrySet_pollFirst_startExcluded_endIncluded() {
        Set entrySet = navigableMap_startExcluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            NavigableSet ascendingSubMapEntrySet = (NavigableSet) entrySet;
            for (int value = 101; value < 110; value++) {
                Entry entry = (Entry) ascendingSubMapEntrySet.pollFirst();
                assertEquals(value, entry.getValue());
            }
            assertTrue(ascendingSubMapEntrySet.isEmpty());
            // should return null if the set is empty.
            assertNull(ascendingSubMapEntrySet.pollFirst());
        }
    }

    public void test_AscendingSubMapEntrySet_pollFirst_startIncluded_endExcluded() {
        Set entrySet = navigableMap_startIncluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            NavigableSet ascendingSubMapEntrySet = (NavigableSet) entrySet;
            for (int value = 100; value < 109; value++) {
                Entry entry = (Entry) ascendingSubMapEntrySet.pollFirst();
                assertEquals(value, entry.getValue());
            }
            assertTrue(ascendingSubMapEntrySet.isEmpty());
            // should return null if the set is empty.
            assertNull(ascendingSubMapEntrySet.pollFirst());
        }
    }

    public void test_AscendingSubMapEntrySet_pollFirst_startIncluded_endIncluded() {
        Set entrySet = navigableMap_startIncluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            NavigableSet ascendingSubMapEntrySet = (NavigableSet) entrySet;
            for (int value = 100; value < 110; value++) {
                Entry entry = (Entry) ascendingSubMapEntrySet.pollFirst();
                assertEquals(value, entry.getValue());
            }
            assertTrue(ascendingSubMapEntrySet.isEmpty());
            // should return null if the set is empty.
            assertNull(ascendingSubMapEntrySet.pollFirst());
        }
    }

    public void test_AscendingSubMapEntrySet_pollLast_startExcluded_endExcluded() {
        Set entrySet = navigableMap_startExcluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            NavigableSet ascendingSubMapEntrySet = (NavigableSet) entrySet;
            for (int value = 108; value > 100; value--) {
                Entry entry = (Entry) ascendingSubMapEntrySet.pollLast();
                assertEquals(value, entry.getValue());
            }
            assertTrue(ascendingSubMapEntrySet.isEmpty());
            // should return null if the set is empty
            assertNull(ascendingSubMapEntrySet.pollLast());
        }

        // NavigableMap ascendingSubMap = tm.headMap("2", true);
        // Set entrySet = ascendingSubMap.entrySet();
        // Object last;
        // if (entrySet instanceof NavigableSet) {
        // last = ((NavigableSet) entrySet).pollLast();
        // assertEquals("2=2", last.toString());
        // }
        //
        // ascendingSubMap = tm.tailMap("2", true);
        // entrySet = ascendingSubMap.entrySet();
        // if (entrySet instanceof NavigableSet) {
        // last = ((NavigableSet) entrySet).pollLast();
        // assertEquals("999=999", last.toString());
        // }
    }

    public void test_AscendingSubMapEntrySet_pollLast_startExcluded_endIncluded() {
        Set entrySet = navigableMap_startExcluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            NavigableSet ascendingSubMapEntrySet = (NavigableSet) entrySet;
            for (int value = 109; value > 100; value--) {
                Entry entry = (Entry) ascendingSubMapEntrySet.pollLast();
                assertEquals(value, entry.getValue());
            }
            assertTrue(ascendingSubMapEntrySet.isEmpty());
            // should return null if the set is empty
            assertNull(ascendingSubMapEntrySet.pollLast());
        }
    }

    public void test_AscendingSubMapEntrySet_pollLast_startIncluded_endExcluded() {
        Set entrySet = navigableMap_startIncluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            NavigableSet ascendingSubMapEntrySet = (NavigableSet) entrySet;
            for (int value = 108; value > 99; value--) {
                Entry entry = (Entry) ascendingSubMapEntrySet.pollLast();
                assertEquals(value, entry.getValue());
            }
            assertTrue(ascendingSubMapEntrySet.isEmpty());
            // should return null if the set is empty
            assertNull(ascendingSubMapEntrySet.pollLast());
        }
    }

    public void test_AscendingSubMapEntrySet_pollLast_startIncluded_endIncluded() {
        Set entrySet = navigableMap_startIncluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            NavigableSet ascendingSubMapEntrySet = (NavigableSet) entrySet;
            for (int value = 109; value > 99; value--) {
                Entry entry = (Entry) ascendingSubMapEntrySet.pollLast();
                assertEquals(value, entry.getValue());
            }
            assertTrue(ascendingSubMapEntrySet.isEmpty());
            // should return null if the set is empty
            assertNull(ascendingSubMapEntrySet.pollLast());
        }
    }

    public void test_AscendingSubMapEntrySet_headSet() {
        Set entrySet, headSet;
        NavigableSet ascendingSubMapEntrySet;
        Iterator iterator, headSetIterator;
        Entry entry;
        int value;

        entrySet = navigableMap_startExcluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = ascendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                headSet = ascendingSubMapEntrySet.headSet(entry);
                headSetIterator = headSet.iterator();
                for (value = 101; headSetIterator.hasNext(); value++) {
                    assertEquals(value, ((Entry) headSetIterator.next())
                            .getValue());
                }
                assertEquals(entry.getValue(), value);
                try {
                    headSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                headSet = ascendingSubMapEntrySet.headSet(entry, false);
                headSetIterator = headSet.iterator();
                for (value = 101; headSetIterator.hasNext(); value++) {
                    assertEquals(value, ((Entry) headSetIterator.next())
                            .getValue());
                }
                assertEquals(entry.getValue(), value);
                try {
                    headSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                headSet = ascendingSubMapEntrySet.headSet(entry, true);
                headSetIterator = headSet.iterator();
                for (value = 101; headSetIterator.hasNext(); value++) {
                    assertEquals(value, ((Entry) headSetIterator.next())
                            .getValue());
                }
                assertEquals(entry.getValue(), value - 1);
                try {
                    headSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }
            }
        }

        entrySet = navigableMap_startExcluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = ascendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                headSet = ascendingSubMapEntrySet.headSet(entry);
                headSetIterator = headSet.iterator();
                for (value = 101; headSetIterator.hasNext(); value++) {
                    assertEquals(value, ((Entry) headSetIterator.next())
                            .getValue());
                }
                assertEquals(entry.getValue(), value);
                try {
                    headSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                headSet = ascendingSubMapEntrySet.headSet(entry, false);
                headSetIterator = headSet.iterator();
                for (value = 101; headSetIterator.hasNext(); value++) {
                    assertEquals(value, ((Entry) headSetIterator.next())
                            .getValue());
                }
                assertEquals(entry.getValue(), value);
                try {
                    headSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                headSet = ascendingSubMapEntrySet.headSet(entry, true);
                headSetIterator = headSet.iterator();
                for (value = 101; headSetIterator.hasNext(); value++) {
                    assertEquals(value, ((Entry) headSetIterator.next())
                            .getValue());
                }
                assertEquals(entry.getValue(), value - 1);
                try {
                    headSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }
            }
        }

        entrySet = navigableMap_startIncluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = ascendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                headSet = ascendingSubMapEntrySet.headSet(entry);
                headSetIterator = headSet.iterator();
                for (value = 100; headSetIterator.hasNext(); value++) {
                    assertEquals(value, ((Entry) headSetIterator.next())
                            .getValue());
                }
                assertEquals(entry.getValue(), value);
                try {
                    headSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                headSet = ascendingSubMapEntrySet.headSet(entry, false);
                headSetIterator = headSet.iterator();
                for (value = 100; headSetIterator.hasNext(); value++) {
                    assertEquals(value, ((Entry) headSetIterator.next())
                            .getValue());
                }
                assertEquals(entry.getValue(), value);
                try {
                    headSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                headSet = ascendingSubMapEntrySet.headSet(entry, true);
                headSetIterator = headSet.iterator();
                for (value = 100; headSetIterator.hasNext(); value++) {
                    assertEquals(value, ((Entry) headSetIterator.next())
                            .getValue());
                }
                assertEquals(entry.getValue(), value - 1);
                try {
                    headSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }
            }
        }

        entrySet = navigableMap_startIncluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = ascendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                headSet = ascendingSubMapEntrySet.headSet(entry);
                headSetIterator = headSet.iterator();
                for (value = 100; headSetIterator.hasNext(); value++) {
                    assertEquals(value, ((Entry) headSetIterator.next())
                            .getValue());
                }
                assertEquals(entry.getValue(), value);
                try {
                    headSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                headSet = ascendingSubMapEntrySet.headSet(entry, false);
                headSetIterator = headSet.iterator();
                for (value = 100; headSetIterator.hasNext(); value++) {
                    assertEquals(value, ((Entry) headSetIterator.next())
                            .getValue());
                }
                assertEquals(entry.getValue(), value);
                try {
                    headSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                headSet = ascendingSubMapEntrySet.headSet(entry, true);
                headSetIterator = headSet.iterator();
                for (value = 100; headSetIterator.hasNext(); value++) {
                    assertEquals(value, ((Entry) headSetIterator.next())
                            .getValue());
                }
                assertEquals(entry.getValue(), value - 1);
                try {
                    headSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }
            }
        }

        // NavigableMap ascendingSubMap = tm.headMap("1", true);
        // entrySet = ascendingSubMap.entrySet();
        // if (entrySet instanceof SortedSet) {
        // Iterator it = entrySet.iterator();
        // it.next();
        // Object end = it.next();// 1=1
        // Set headSet = ((NavigableSet) entrySet).headSet(end);// inclusive
        // // false
        // assertEquals(1, headSet.size());
        // }
    }

    public void test_AscendingSubMapEntrySet_tailSet() {
        Set entrySet, tailSet;
        NavigableSet ascendingSubMapEntrySet;
        Iterator iterator, tailSetIterator;
        Entry entry;
        int value;

        entrySet = navigableMap_startExcluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = entrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                tailSet = ascendingSubMapEntrySet.tailSet(entry);
                tailSetIterator = tailSet.iterator();
                for (value = (Integer) entry.getValue() + 1; tailSetIterator
                        .hasNext(); value++) {
                    assertEquals(value, ((Entry) tailSetIterator.next())
                            .getValue());
                }
                assertEquals(109, value);
                try {
                    tailSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                tailSet = ascendingSubMapEntrySet.tailSet(entry, false);
                tailSetIterator = tailSet.iterator();
                for (value = (Integer) entry.getValue() + 1; tailSetIterator
                        .hasNext(); value++) {
                    assertEquals(value, ((Entry) tailSetIterator.next())
                            .getValue());
                }
                assertEquals(109, value);
                try {
                    tailSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                tailSet = ascendingSubMapEntrySet.tailSet(entry, true);
                tailSetIterator = tailSet.iterator();
                for (value = (Integer) entry.getValue(); tailSetIterator
                        .hasNext(); value++) {
                    assertEquals(value, ((Entry) tailSetIterator.next())
                            .getValue());
                }
                assertEquals(109, value);
                try {
                    tailSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }
            }
        }

        entrySet = navigableMap_startExcluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = entrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                tailSet = ascendingSubMapEntrySet.tailSet(entry);
                tailSetIterator = tailSet.iterator();
                for (value = (Integer) entry.getValue() + 1; tailSetIterator
                        .hasNext(); value++) {
                    assertEquals(value, ((Entry) tailSetIterator.next())
                            .getValue());
                }
                assertEquals(110, value);
                try {
                    tailSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                tailSet = ascendingSubMapEntrySet.tailSet(entry, false);
                tailSetIterator = tailSet.iterator();
                for (value = (Integer) entry.getValue() + 1; tailSetIterator
                        .hasNext(); value++) {
                    assertEquals(value, ((Entry) tailSetIterator.next())
                            .getValue());
                }
                assertEquals(110, value);
                try {
                    tailSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                tailSet = ascendingSubMapEntrySet.tailSet(entry, true);
                tailSetIterator = tailSet.iterator();
                for (value = (Integer) entry.getValue(); tailSetIterator
                        .hasNext(); value++) {
                    assertEquals(value, ((Entry) tailSetIterator.next())
                            .getValue());
                }
                assertEquals(110, value);
                try {
                    tailSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }
            }
        }

        entrySet = navigableMap_startIncluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = entrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                tailSet = ascendingSubMapEntrySet.tailSet(entry);
                tailSetIterator = tailSet.iterator();
                for (value = (Integer) entry.getValue() + 1; tailSetIterator
                        .hasNext(); value++) {
                    assertEquals(value, ((Entry) tailSetIterator.next())
                            .getValue());
                }
                assertEquals(109, value);
                try {
                    tailSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                tailSet = ascendingSubMapEntrySet.tailSet(entry, false);
                tailSetIterator = tailSet.iterator();
                for (value = (Integer) entry.getValue() + 1; tailSetIterator
                        .hasNext(); value++) {
                    assertEquals(value, ((Entry) tailSetIterator.next())
                            .getValue());
                }
                assertEquals(109, value);
                try {
                    tailSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                tailSet = ascendingSubMapEntrySet.tailSet(entry, true);
                tailSetIterator = tailSet.iterator();
                for (value = (Integer) entry.getValue(); tailSetIterator
                        .hasNext(); value++) {
                    assertEquals(value, ((Entry) tailSetIterator.next())
                            .getValue());
                }
                assertEquals(109, value);
                try {
                    tailSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }
            }
        }

        entrySet = navigableMap_startIncluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = entrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                tailSet = ascendingSubMapEntrySet.tailSet(entry);
                tailSetIterator = tailSet.iterator();
                for (value = (Integer) entry.getValue() + 1; tailSetIterator
                        .hasNext(); value++) {
                    assertEquals(value, ((Entry) tailSetIterator.next())
                            .getValue());
                }
                assertEquals(110, value);
                try {
                    tailSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                tailSet = ascendingSubMapEntrySet.tailSet(entry, false);
                tailSetIterator = tailSet.iterator();
                for (value = (Integer) entry.getValue() + 1; tailSetIterator
                        .hasNext(); value++) {
                    assertEquals(value, ((Entry) tailSetIterator.next())
                            .getValue());
                }
                assertEquals(110, value);
                try {
                    tailSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }

                tailSet = ascendingSubMapEntrySet.tailSet(entry, true);
                tailSetIterator = tailSet.iterator();
                for (value = (Integer) entry.getValue(); tailSetIterator
                        .hasNext(); value++) {
                    assertEquals(value, ((Entry) tailSetIterator.next())
                            .getValue());
                }
                assertEquals(110, value);
                try {
                    tailSetIterator.next();
                    fail("should throw NoSuchElementException");
                } catch (NoSuchElementException e) {
                    // Expected
                }
            }
        }

        // NavigableMap ascendingSubMap = tm.headMap("1", true);
        // Set entrySet = ascendingSubMap.entrySet();
        // if (entrySet instanceof NavigableSet) {
        // Iterator it = entrySet.iterator();
        // Object start = it.next();// 0=0
        // Set tailSet = ((NavigableSet) entrySet).tailSet(start);// default
        // // inclusive
        // // false
        // assertEquals(1, tailSet.size());
        // }
    }

    public void test_AscendingSubMapEntrySet_subSet() {
        Set entrySet, subSet;
        NavigableSet ascendingSubMapEntrySet;

        entrySet = navigableMap_startExcluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            Iterator iteratorStart = ascendingSubMapEntrySet.iterator();
            while (iteratorStart.hasNext()) {
                Entry startEntry = (Entry) iteratorStart.next();
                Iterator iteratorEnd = ascendingSubMapEntrySet.iterator();
                while (iteratorEnd.hasNext()) {
                    Entry endEntry = (Entry) iteratorEnd.next();
                    int startIndex = (Integer) startEntry.getValue();
                    int endIndex = (Integer) endEntry.getValue();
                    if (startIndex > endIndex) {
                        try {
                            ascendingSubMapEntrySet
                                    .subSet(startEntry, endEntry);
                            fail("should throw IllegalArgumentException");
                        } catch (IllegalArgumentException e) {
                            // Expected
                        }

                        try {
                            ascendingSubMapEntrySet.subSet(startEntry, false,
                                    endEntry, false);
                            fail("should throw IllegalArgumentException");
                        } catch (IllegalArgumentException e) {
                            // Expected
                        }

                        try {
                            ascendingSubMapEntrySet.subSet(startEntry, false,
                                    endEntry, true);
                            fail("should throw IllegalArgumentException");
                        } catch (IllegalArgumentException e) {
                            // Expected
                        }

                        try {
                            ascendingSubMapEntrySet.subSet(startEntry, true,
                                    endEntry, false);
                            fail("should throw IllegalArgumentException");
                        } catch (IllegalArgumentException e) {
                            // Expected
                        }

                        try {
                            ascendingSubMapEntrySet.subSet(startEntry, true,
                                    endEntry, true);
                            fail("should throw IllegalArgumentException");
                        } catch (IllegalArgumentException e) {
                            // Expected
                        }
                    } else {
                        subSet = ascendingSubMapEntrySet.subSet(startEntry,
                                endEntry);
                        Iterator subSetIterator = subSet.iterator();
                        for (int index = startIndex + 1; subSetIterator
                                .hasNext(); index++) {
                            assertEquals(index, ((Entry) subSetIterator.next())
                                    .getValue());
                        }

                        subSet = ascendingSubMapEntrySet.subSet(startEntry,
                                false, endEntry, false);
                        subSetIterator = subSet.iterator();
                        for (int index = startIndex + 1; subSetIterator
                                .hasNext(); index++) {
                            assertEquals(index, ((Entry) subSetIterator.next())
                                    .getValue());
                        }

                        subSet = ascendingSubMapEntrySet.subSet(startEntry,
                                false, endEntry, true);
                        subSetIterator = subSet.iterator();
                        for (int index = startIndex + 1; subSetIterator
                                .hasNext(); index++) {
                            assertEquals(index, ((Entry) subSetIterator.next())
                                    .getValue());
                        }

                        subSet = ascendingSubMapEntrySet.subSet(startEntry,
                                true, endEntry, false);
                        subSetIterator = subSet.iterator();
                        for (int index = startIndex; subSetIterator.hasNext(); index++) {
                            assertEquals(index, ((Entry) subSetIterator.next())
                                    .getValue());
                        }

                        subSet = ascendingSubMapEntrySet.subSet(startEntry,
                                true, endEntry, true);
                        subSetIterator = subSet.iterator();
                        for (int index = startIndex; subSetIterator.hasNext(); index++) {
                            assertEquals(index, ((Entry) subSetIterator.next())
                                    .getValue());
                        }
                    }
                }
            }
        }

        String endKey = new Integer(2).toString();
        entrySet = tm.headMap(endKey, true).entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            Iterator iterator = entrySet.iterator();
            Object startEntry = iterator.next();
            iterator.next();
            Object endEntry = iterator.next();
            subSet = ascendingSubMapEntrySet.subSet(startEntry, endEntry);
            assertEquals(1, subSet.size());

            subSet = ascendingSubMapEntrySet.subSet(startEntry, false,
                    endEntry, false);
            assertEquals(1, subSet.size());

            subSet = ascendingSubMapEntrySet.subSet(startEntry, false,
                    endEntry, true);
            assertEquals(2, subSet.size());

            subSet = ascendingSubMapEntrySet.subSet(startEntry, true, endEntry,
                    false);
            assertEquals(2, subSet.size());

            subSet = ascendingSubMapEntrySet.subSet(startEntry, true, endEntry,
                    true);
            assertEquals(3, subSet.size());
        }
    }

    public void test_AscendingSubMapEntrySet_lower() {
        Set entrySet;
        NavigableSet ascendingSubMapEntrySet;
        Iterator iterator;
        Entry entry, lowerEntry;
        int value;

        entrySet = navigableMap_startExcluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = ascendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                lowerEntry = (Entry) ascendingSubMapEntrySet.lower(entry);
                value = (Integer) entry.getValue();
                if (value > 101) {
                    assertEquals(value - 1, lowerEntry.getValue());
                } else {
                    assertNull(lowerEntry);
                }
            }
        }

        entrySet = navigableMap_startExcluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = ascendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                lowerEntry = (Entry) ascendingSubMapEntrySet.lower(entry);
                value = (Integer) entry.getValue();
                if (value > 101) {
                    assertEquals(value - 1, lowerEntry.getValue());
                } else {
                    assertNull(lowerEntry);
                }
            }
        }

        entrySet = navigableMap_startIncluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = ascendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                lowerEntry = (Entry) ascendingSubMapEntrySet.lower(entry);
                value = (Integer) entry.getValue();
                if (value > 100) {
                    assertEquals(value - 1, lowerEntry.getValue());
                } else {
                    assertNull(lowerEntry);
                }
            }
        }

        entrySet = navigableMap_startIncluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = ascendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                lowerEntry = (Entry) ascendingSubMapEntrySet.lower(entry);
                value = (Integer) entry.getValue();
                if (value > 100) {
                    assertEquals(value - 1, lowerEntry.getValue());
                } else {
                    assertNull(lowerEntry);
                }
            }
        }

        String endKey = new Integer(2).toString();
        entrySet = tm.headMap(endKey, true).entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = entrySet.iterator();
            Entry expectedEntry = (Entry) iterator.next();
            entry = (Entry) iterator.next();
            assertEquals(expectedEntry, ascendingSubMapEntrySet.lower(entry));
        }

        // With Comparator

        entrySet = subMap_startExcluded_endExcluded_comparator.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = ascendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                lowerEntry = (Entry) ascendingSubMapEntrySet.lower(entry);
                value = (Integer) entry.getValue();
                if (value > 101) {
                    assertEquals(value - 1, lowerEntry.getValue());
                } else {
                    assertNull(lowerEntry);
                }
            }
        }

        entrySet = subMap_startExcluded_endIncluded_comparator.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = ascendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                lowerEntry = (Entry) ascendingSubMapEntrySet.lower(entry);
                value = (Integer) entry.getValue();
                if (value > 101) {
                    assertEquals(value - 1, lowerEntry.getValue());
                } else {
                    assertNull(lowerEntry);
                }
            }
        }

        entrySet = subMap_startIncluded_endExcluded_comparator.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = ascendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                lowerEntry = (Entry) ascendingSubMapEntrySet.lower(entry);
                value = (Integer) entry.getValue();
                if (value > 100) {
                    assertEquals(value - 1, lowerEntry.getValue());
                } else {
                    assertNull(lowerEntry);
                }
            }
        }

        entrySet = subMap_startIncluded_endIncluded_comparator.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = ascendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                lowerEntry = (Entry) ascendingSubMapEntrySet.lower(entry);
                value = (Integer) entry.getValue();
                if (value > 100) {
                    assertEquals(value - 1, lowerEntry.getValue());
                } else {
                    assertNull(lowerEntry);
                }
            }
        }
    }

    public void test_AscendingSubMapEntrySet_higher() {
        Set entrySet;
        NavigableSet ascendingSubMapEntrySet;
        Iterator iterator;
        Entry entry, lowerEntry;
        int value;

        entrySet = navigableMap_startExcluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = ascendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                lowerEntry = (Entry) ascendingSubMapEntrySet.higher(entry);
                value = (Integer) entry.getValue();
                if (value < 108) {
                    assertEquals(value + 1, lowerEntry.getValue());
                } else {
                    assertNull(lowerEntry);
                }
            }
        }

        entrySet = navigableMap_startExcluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = ascendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                lowerEntry = (Entry) ascendingSubMapEntrySet.higher(entry);
                value = (Integer) entry.getValue();
                if (value < 109) {
                    assertEquals(value + 1, lowerEntry.getValue());
                } else {
                    assertNull(lowerEntry);
                }
            }
        }

        entrySet = navigableMap_startIncluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = ascendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                lowerEntry = (Entry) ascendingSubMapEntrySet.higher(entry);
                value = (Integer) entry.getValue();
                if (value < 108) {
                    assertEquals(value + 1, lowerEntry.getValue());
                } else {
                    assertNull(lowerEntry);
                }
            }
        }

        entrySet = navigableMap_startIncluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = ascendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                lowerEntry = (Entry) ascendingSubMapEntrySet.higher(entry);
                value = (Integer) entry.getValue();
                if (value < 109) {
                    assertEquals(value + 1, lowerEntry.getValue());
                } else {
                    assertNull(lowerEntry);
                }
            }
        }

        String endKey = new Integer(2).toString();
        entrySet = tm.headMap(endKey, true).entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = entrySet.iterator();
            entry = (Entry) iterator.next();
            Entry expectedEntry = (Entry) iterator.next();
            assertEquals(expectedEntry, ascendingSubMapEntrySet.higher(entry));
        }

        // With Comparator
        entrySet = subMap_startExcluded_endExcluded_comparator.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = ascendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                lowerEntry = (Entry) ascendingSubMapEntrySet.higher(entry);
                value = (Integer) entry.getValue();
                if (value < 108) {
                    assertEquals(value + 1, lowerEntry.getValue());
                } else {
                    assertNull(lowerEntry);
                }
            }
        }

        entrySet = subMap_startExcluded_endIncluded_comparator.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = ascendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                lowerEntry = (Entry) ascendingSubMapEntrySet.higher(entry);
                value = (Integer) entry.getValue();
                if (value < 109) {
                    assertEquals(value + 1, lowerEntry.getValue());
                } else {
                    assertNull(lowerEntry);
                }
            }
        }

        entrySet = subMap_startIncluded_endExcluded_comparator.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = ascendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                lowerEntry = (Entry) ascendingSubMapEntrySet.higher(entry);
                value = (Integer) entry.getValue();
                if (value < 108) {
                    assertEquals(value + 1, lowerEntry.getValue());
                } else {
                    assertNull(lowerEntry);
                }
            }
        }

        entrySet = subMap_startIncluded_endIncluded_comparator.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            iterator = ascendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                lowerEntry = (Entry) ascendingSubMapEntrySet.higher(entry);
                value = (Integer) entry.getValue();
                if (value < 109) {
                    assertEquals(value + 1, lowerEntry.getValue());
                } else {
                    assertNull(lowerEntry);
                }
            }
        }
    }

    public void test_AscendingSubMapEntrySet_ceiling() {
        Set entrySet;
        NavigableSet ascendingSubMapEntrySet;
        Iterator iterator;

        Set entrySet_beyondBound;
        Iterator iterator_beyondBound;
        Entry beyondBoundEntry;

        Entry entry, lowerEntry;
        int value = 0;

        entrySet = navigableMap_startExcluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            try {
                ascendingSubMapEntrySet.ceiling(null);
                fail("should throw NullPointerException");
            } catch (NullPointerException e) {
                // Expected
            }

            iterator = ascendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                lowerEntry = (Entry) ascendingSubMapEntrySet.ceiling(entry);
                value = (Integer) entry.getValue();
                assertEquals(value, lowerEntry.getValue());
            }
            assertEquals(108, value);

        }

        entrySet = navigableMap_startExcluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            try {
                ascendingSubMapEntrySet.ceiling(null);
                fail("should throw NullPointerException");
            } catch (NullPointerException e) {
                // Expected
            }

            iterator = ascendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                lowerEntry = (Entry) ascendingSubMapEntrySet.ceiling(entry);
                value = (Integer) entry.getValue();
                assertEquals(value, lowerEntry.getValue());
            }
            assertEquals(109, value);
        }

        entrySet = navigableMap_startIncluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            try {
                ascendingSubMapEntrySet.ceiling(null);
                fail("should throw NullPointerException");
            } catch (NullPointerException e) {
                // Expected
            }

            iterator = ascendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                lowerEntry = (Entry) ascendingSubMapEntrySet.ceiling(entry);
                value = (Integer) entry.getValue();
                assertEquals(value, lowerEntry.getValue());
            }
            assertEquals(108, value);
        }

        entrySet = navigableMap_startIncluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            try {
                ascendingSubMapEntrySet.ceiling(null);
                fail("should throw NullPointerException");
            } catch (NullPointerException e) {
                // Expected
            }

            iterator = ascendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                lowerEntry = (Entry) ascendingSubMapEntrySet.ceiling(entry);
                value = (Integer) entry.getValue();
                assertEquals(value, lowerEntry.getValue());
            }
            assertEquals(109, value);
        }

        // With Comparator
        entrySet = subMap_startIncluded_endIncluded_comparator.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            try {
                ascendingSubMapEntrySet.ceiling(null);
                fail("should throw NullPointerException");
            } catch (NullPointerException e) {
                // Expected
            }

            iterator = ascendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                lowerEntry = (Entry) ascendingSubMapEntrySet.ceiling(entry);
                value = (Integer) entry.getValue();
                assertEquals(value, lowerEntry.getValue());
            }
            assertEquals(109, value);
        }

        entrySet = subMap_startIncluded_endExcluded_comparator.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            try {
                ascendingSubMapEntrySet.ceiling(null);
                fail("should throw NullPointerException");
            } catch (NullPointerException e) {
                // Expected
            }

            iterator = ascendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                lowerEntry = (Entry) ascendingSubMapEntrySet.ceiling(entry);
                value = (Integer) entry.getValue();
                assertEquals(value, lowerEntry.getValue());
            }
            assertEquals(108, value);
        }

        entrySet = subMap_startExcluded_endIncluded_comparator.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            try {
                ascendingSubMapEntrySet.ceiling(null);
                fail("should throw NullPointerException");
            } catch (NullPointerException e) {
                // Expected
            }

            iterator = ascendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                lowerEntry = (Entry) ascendingSubMapEntrySet.ceiling(entry);
                value = (Integer) entry.getValue();
                assertEquals(value, lowerEntry.getValue());
            }
            assertEquals(109, value);
        }

        entrySet = subMap_startExcluded_endExcluded_comparator.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            try {
                ascendingSubMapEntrySet.ceiling(null);
                fail("should throw NullPointerException");
            } catch (NullPointerException e) {
                // Expected
            }

            iterator = ascendingSubMapEntrySet.iterator();
            while (iterator.hasNext()) {
                entry = (Entry) iterator.next();
                lowerEntry = (Entry) ascendingSubMapEntrySet.ceiling(entry);
                value = (Integer) entry.getValue();
                assertEquals(value, lowerEntry.getValue());
            }
            assertEquals(108, value);
        }
    }

    public void test_AscendingSubMapEntrySet_floor() {
        Set entrySet;
        NavigableSet ascendingSubMapEntrySet;
        Iterator iterator;
        Entry entry, floorEntry;
        int value;

        entrySet = navigableMap_startExcluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            try {
                ascendingSubMapEntrySet.floor(null);
                fail("should throw NullPointerException");
            } catch (NullPointerException e) {
                // Expected
            }

            iterator = ascendingSubMapEntrySet.iterator();
            for (int i = 101; i < 109; i++) {
                entry = (Entry) iterator.next();
                floorEntry = (Entry) ascendingSubMapEntrySet.floor(entry);
                assertEquals(entry.getValue(), floorEntry.getValue());
            }
            assertFalse(iterator.hasNext());
        }

        entrySet = navigableMap_startExcluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            try {
                ascendingSubMapEntrySet.floor(null);
                fail("should throw NullPointerException");
            } catch (NullPointerException e) {
                // Expected
            }

            iterator = ascendingSubMapEntrySet.iterator();
            for (int i = 101; i < 110; i++) {
                entry = (Entry) iterator.next();
                floorEntry = (Entry) ascendingSubMapEntrySet.floor(entry);
                assertEquals(entry.getValue(), floorEntry.getValue());
            }
            assertFalse(iterator.hasNext());
        }

        entrySet = navigableMap_startIncluded_endExcluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            try {
                ascendingSubMapEntrySet.floor(null);
                fail("should throw NullPointerException");
            } catch (NullPointerException e) {
                // Expected
            }

            iterator = ascendingSubMapEntrySet.iterator();
            for (int i = 100; i < 109; i++) {
                entry = (Entry) iterator.next();
                floorEntry = (Entry) ascendingSubMapEntrySet.floor(entry);
                assertEquals(entry.getValue(), floorEntry.getValue());
            }
            assertFalse(iterator.hasNext());
        }

        entrySet = navigableMap_startIncluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            try {
                ascendingSubMapEntrySet.floor(null);
                fail("should throw NullPointerException");
            } catch (NullPointerException e) {
                // Expected
            }

            iterator = ascendingSubMapEntrySet.iterator();
            for (int i = 100; i < 110; i++) {
                entry = (Entry) iterator.next();
                floorEntry = (Entry) ascendingSubMapEntrySet.floor(entry);
                assertEquals(entry.getValue(), floorEntry.getValue());
            }
            assertFalse(iterator.hasNext());
        }

        // With Comparator
        entrySet = subMap_startExcluded_endExcluded_comparator.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            try {
                ascendingSubMapEntrySet.floor(null);
                fail("should throw NullPointerException");
            } catch (NullPointerException e) {
                // Expected
            }

            iterator = ascendingSubMapEntrySet.iterator();
            for (int i = 101; i < 109; i++) {
                entry = (Entry) iterator.next();
                floorEntry = (Entry) ascendingSubMapEntrySet.floor(entry);
                assertEquals(entry.getValue(), floorEntry.getValue());
            }
            assertFalse(iterator.hasNext());
        }

        entrySet = subMap_startExcluded_endIncluded_comparator.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            try {
                ascendingSubMapEntrySet.floor(null);
                fail("should throw NullPointerException");
            } catch (NullPointerException e) {
                // Expected
            }

            iterator = ascendingSubMapEntrySet.iterator();
            for (int i = 101; i < 110; i++) {
                entry = (Entry) iterator.next();
                floorEntry = (Entry) ascendingSubMapEntrySet.floor(entry);
                assertEquals(entry.getValue(), floorEntry.getValue());
            }
            assertFalse(iterator.hasNext());
        }

        entrySet = subMap_startIncluded_endExcluded_comparator.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            try {
                ascendingSubMapEntrySet.floor(null);
                fail("should throw NullPointerException");
            } catch (NullPointerException e) {
                // Expected
            }

            iterator = ascendingSubMapEntrySet.iterator();
            for (int i = 100; i < 109; i++) {
                entry = (Entry) iterator.next();
                floorEntry = (Entry) ascendingSubMapEntrySet.floor(entry);
                assertEquals(entry.getValue(), floorEntry.getValue());
            }
            assertFalse(iterator.hasNext());
        }

        entrySet = subMap_startIncluded_endIncluded.entrySet();
        if (entrySet instanceof NavigableSet) {
            ascendingSubMapEntrySet = (NavigableSet) entrySet;
            try {
                ascendingSubMapEntrySet.floor(null);
                fail("should throw NullPointerException");
            } catch (NullPointerException e) {
                // Expected
            }

            iterator = ascendingSubMapEntrySet.iterator();
            for (int i = 100; i < 110; i++) {
                entry = (Entry) iterator.next();
                floorEntry = (Entry) ascendingSubMapEntrySet.floor(entry);
                assertEquals(entry.getValue(), floorEntry.getValue());
            }
            assertFalse(iterator.hasNext());
        }
    }

    @Override
    protected void setUp() {
        tm = new TreeMap();
        tm_comparator = new TreeMap(new MockComparator());
        for (int i = 0; i < objArray.length; i++) {
            Object x = objArray[i] = new Integer(i);
            tm.put(x.toString(), x);
            tm_comparator.put(x.toString(), x);
        }

        subMap_default = tm.subMap(objArray[100].toString(), objArray[109]
                .toString());
        subMap_startExcluded_endExcluded = tm.subMap(objArray[100].toString(),
                false, objArray[109].toString(), false);
        subMap_startExcluded_endIncluded = tm.subMap(objArray[100].toString(),
                false, objArray[109].toString(), true);
        subMap_startIncluded_endExcluded = tm.subMap(objArray[100].toString(),
                true, objArray[109].toString(), false);
        subMap_startIncluded_endIncluded = tm.subMap(objArray[100].toString(),
                true, objArray[109].toString(), true);

        subMap_default_beforeStart_100 = tm.subMap(objArray[0].toString(),
                objArray[1].toString());

        subMap_default_afterEnd_109 = tm.subMap(objArray[110].toString(),
                objArray[119].toString());

        assertTrue(subMap_startExcluded_endExcluded instanceof NavigableMap);
        assertTrue(subMap_startExcluded_endIncluded instanceof NavigableMap);
        assertTrue(subMap_startIncluded_endExcluded instanceof NavigableMap);
        assertTrue(subMap_startIncluded_endIncluded instanceof NavigableMap);

        navigableMap_startExcluded_endExcluded = (NavigableMap) subMap_startExcluded_endExcluded;
        navigableMap_startExcluded_endIncluded = (NavigableMap) subMap_startExcluded_endIncluded;
        navigableMap_startIncluded_endExcluded = (NavigableMap) subMap_startIncluded_endExcluded;
        navigableMap_startIncluded_endIncluded = (NavigableMap) subMap_startIncluded_endIncluded;

        subMap_default_comparator = tm_comparator.subMap(objArray[100]
                .toString(), objArray[109].toString());
        subMap_startExcluded_endExcluded_comparator = tm_comparator.subMap(
                objArray[100].toString(), false, objArray[109].toString(),
                false);

        subMap_startExcluded_endIncluded_comparator = tm_comparator
                .subMap(objArray[100].toString(), false, objArray[109]
                        .toString(), true);
        subMap_startIncluded_endExcluded_comparator = tm_comparator
                .subMap(objArray[100].toString(), true, objArray[109]
                        .toString(), false);
        subMap_startIncluded_endIncluded_comparator = tm_comparator.subMap(
                objArray[100].toString(), true, objArray[109].toString(), true);
    }

    @Override
    protected void tearDown() {
        tm = null;
        tm_comparator = null;

        subMap_default = null;
        subMap_startExcluded_endExcluded = null;
        subMap_startExcluded_endIncluded = null;
        subMap_startIncluded_endExcluded = null;
        subMap_startIncluded_endIncluded = null;

        subMap_default_beforeStart_100 = null;
        subMap_default_afterEnd_109 = null;

        subMap_default_comparator = null;
        subMap_startExcluded_endExcluded_comparator = null;
        subMap_startExcluded_endIncluded_comparator = null;
        subMap_startIncluded_endExcluded_comparator = null;
        subMap_startIncluded_endIncluded_comparator = null;
    }

    public void test_lower_null() throws Exception {
        NavigableMap map = tm.subMap(objArray[100].toString(), true,
                objArray[100].toString(), false);
        assertNull(map.ceilingKey(objArray[100].toString()));
        assertNull(map.floorKey(objArray[100].toString()));
        assertNull(map.lowerKey(objArray[100].toString()));
        assertNull(map.higherKey(objArray[100].toString()));
        assertNull(map.ceilingKey(objArray[111].toString()));
        assertNull(map.floorKey(objArray[111].toString()));
        assertNull(map.lowerKey(objArray[111].toString()));
        assertNull(map.higherKey(objArray[111].toString()));
        assertNull(map.ceilingKey(objArray[1].toString()));
        assertNull(map.floorKey(objArray[1].toString()));
        assertNull(map.lowerKey(objArray[1].toString()));
        assertNull(map.higherKey(objArray[1].toString()));
        map = map.descendingMap();
        assertNull(map.ceilingKey(objArray[100].toString()));
        assertNull(map.floorKey(objArray[100].toString()));
        assertNull(map.lowerKey(objArray[100].toString()));
        assertNull(map.higherKey(objArray[100].toString()));
        assertNull(map.ceilingKey(objArray[111].toString()));
        assertNull(map.floorKey(objArray[111].toString()));
        assertNull(map.lowerKey(objArray[111].toString()));
        assertNull(map.higherKey(objArray[111].toString()));
        assertNull(map.ceilingKey(objArray[1].toString()));
        assertNull(map.floorKey(objArray[1].toString()));
        assertNull(map.lowerKey(objArray[1].toString()));
        assertNull(map.higherKey(objArray[1].toString()));
    }

    public void test_lower_tail() throws Exception {
        NavigableMap map = tm.subMap(objArray[102].toString(), true,
                objArray[103].toString(), false);
        assertTrue(map.containsKey(objArray[102].toString()));
        assertFalse(map.containsKey(objArray[101].toString()));
        assertFalse(map.containsKey(objArray[103].toString()));
        assertFalse(map.containsKey(objArray[104].toString()));
        map = map.descendingMap();
        assertTrue(map.containsKey(objArray[102].toString()));
        assertFalse(map.containsKey(objArray[101].toString()));
        assertFalse(map.containsKey(objArray[103].toString()));
        assertFalse(map.containsKey(objArray[104].toString()));
        map = tm.subMap(objArray[102].toString(), true, objArray[102]
                .toString(), false);
        assertFalse(map.containsKey(objArray[102].toString()));
        assertFalse(map.containsKey(objArray[101].toString()));
        assertFalse(map.containsKey(objArray[103].toString()));
        assertFalse(map.containsKey(objArray[104].toString()));
        map = map.descendingMap();
        assertFalse(map.containsKey(objArray[102].toString()));
        assertFalse(map.containsKey(objArray[101].toString()));
        assertFalse(map.containsKey(objArray[103].toString()));
        assertFalse(map.containsKey(objArray[104].toString()));
    }

    public void test_contains_null() throws Exception {
        NavigableMap map = tm.subMap(objArray[100].toString(), true,
                objArray[100].toString(), false);
        assertFalse(map.containsKey(objArray[100].toString()));
        assertFalse(map.containsKey(objArray[10].toString()));
        assertFalse(map.containsKey(objArray[101].toString()));
        assertFalse(map.containsKey(objArray[102].toString()));
        assertFalse(map.containsKey(objArray[1].toString()));
        map = map.descendingMap();
        assertFalse(map.containsKey(objArray[100].toString()));
        assertFalse(map.containsKey(objArray[10].toString()));
        assertFalse(map.containsKey(objArray[101].toString()));
        assertFalse(map.containsKey(objArray[102].toString()));
        assertFalse(map.containsKey(objArray[1].toString()));
    }

    public void test_contains() throws Exception {
        NavigableMap map = tm.subMap(objArray[102].toString(), true,
                objArray[103].toString(), false);
        assertFalse(map.containsKey(objArray[100].toString()));
        assertFalse(map.containsKey(objArray[104].toString()));
        assertFalse(map.containsKey(objArray[101].toString()));
        assertTrue(map.containsKey(objArray[102].toString()));
        map = map.descendingMap();
        assertFalse(map.containsKey(objArray[100].toString()));
        assertFalse(map.containsKey(objArray[104].toString()));
        assertFalse(map.containsKey(objArray[101].toString()));
        assertTrue(map.containsKey(objArray[102].toString()));
    }

    public void test_size() throws Exception {
        NavigableMap map = tm.subMap(objArray[102].toString(), true,
                objArray[103].toString(), false);
        assertEquals(0, map.headMap(objArray[102].toString(), false).size());
        assertEquals(1, map.headMap(objArray[102].toString(), true).size());
        try {
            assertEquals(1, map.headMap(objArray[103].toString(), true).size());
            fail("should throw IAE");
        } catch (IllegalArgumentException e) {
        }
        assertEquals(1, map.headMap(objArray[103].toString(), false).size());
        assertEquals(1, map.tailMap(objArray[102].toString(), true).size());
        assertEquals(0, map.tailMap(objArray[102].toString(), false).size());
        assertTrue(map.headMap(objArray[103].toString(), false).containsKey(
                objArray[102].toString()));
        try {
            assertTrue(map.headMap(objArray[103].toString(), true).containsKey(
                    objArray[102].toString()));
            fail("should throw IAE");
        } catch (IllegalArgumentException e) {
        }
        assertFalse(map.headMap(objArray[102].toString(), false).containsKey(
                objArray[102].toString()));
        assertTrue(map.headMap(objArray[102].toString(), true).containsKey(
                objArray[102].toString()));
        assertTrue(map.tailMap(objArray[102].toString(), true).containsKey(
                objArray[102].toString()));
        assertFalse(map.tailMap(objArray[102].toString(), true).containsKey(
                objArray[103].toString()));
        try {
            assertEquals(0, map.tailMap(objArray[101].toString()).size());
            fail("should throw IAE");
        } catch (IllegalArgumentException e) {
        }
        map = map.descendingMap();
        try {
            map = map.subMap(objArray[103].toString(), true, objArray[102]
                    .toString(), true);
            fail("should throw IAE");
        } catch (IllegalArgumentException e) {
        }
        map = map.subMap(objArray[102].toString(), true, objArray[102]
                .toString(), true);
        assertEquals(1, map.headMap(objArray[102].toString(), true).size());
        assertEquals(0, map.headMap(objArray[102].toString(), false).size());
        try {
            assertEquals(0, map.headMap(objArray[103].toString(), true).size());
            fail("should throw IAE");
        } catch (IllegalArgumentException e) {
        }

        assertEquals(1, map.tailMap(objArray[102].toString(), true).size());
        try {
            assertFalse(map.headMap(objArray[103].toString(), true)
                    .containsKey(objArray[102].toString()));
            fail("should throw IAE");
        } catch (IllegalArgumentException e) {
        }
        assertTrue(map.headMap(objArray[102].toString(), true).containsKey(
                objArray[102].toString()));
        assertFalse(map.headMap(objArray[102].toString(), false).containsKey(
                objArray[102].toString()));
        assertTrue(map.tailMap(objArray[102].toString(), true).containsKey(
                objArray[102].toString()));
        assertFalse(map.tailMap(objArray[102].toString(), true).containsKey(
                objArray[103].toString()));
        try {
            assertEquals(0, map.tailMap(objArray[101].toString()).size());
            fail("should throw IAE");
        } catch (IllegalArgumentException e) {
        }
    }

    public void test_lower() throws Exception {
        NavigableMap map = tm.subMap(objArray[102].toString(), true,
                objArray[103].toString(), false);
        assertEquals(objArray[102].toString(), map.higherKey(objArray[101]
                .toString()));
        assertEquals(null, map.higherKey(objArray[102].toString()));
        assertEquals(null, map.higherKey(objArray[103].toString()));
        assertEquals(null, map.higherKey(objArray[104].toString()));
        assertEquals(objArray[102].toString(), map.ceilingKey(objArray[101]
                .toString()));
        assertEquals(objArray[102].toString(), map.ceilingKey(objArray[102]
                .toString()));
        assertEquals(null, map.ceilingKey(objArray[103].toString()));
        assertEquals(null, map.ceilingKey(objArray[104].toString()));
        assertEquals(null, map.lowerKey(objArray[101].toString()));
        assertEquals(null, map.lowerKey(objArray[102].toString()));
        assertEquals(objArray[102].toString(), map.lowerKey(objArray[103]
                .toString()));
        assertEquals(objArray[102].toString(), map.lowerKey(objArray[104]
                .toString()));
        assertEquals(null, map.floorKey(objArray[101].toString()));
        assertEquals(objArray[102].toString(), map.floorKey(objArray[102]
                .toString()));
        assertEquals(objArray[102].toString(), map.floorKey(objArray[103]
                .toString()));
        assertEquals(objArray[102].toString(), map.floorKey(objArray[104]
                .toString()));
        map = map.descendingMap();
        assertEquals(null, map.higherKey(objArray[101].toString()));
        assertEquals(null, map.higherKey(objArray[102].toString()));
        assertEquals(objArray[102].toString(), map.higherKey(objArray[103]
                .toString()));
        assertEquals(objArray[102].toString(), map.higherKey(objArray[104]
                .toString()));
        assertEquals(null, map.ceilingKey(objArray[101].toString()));
        assertEquals(objArray[102].toString(), map.ceilingKey(objArray[102]
                .toString()));
        assertEquals(objArray[102].toString(), map.ceilingKey(objArray[103]
                .toString()));
        assertEquals(objArray[102].toString(), map.ceilingKey(objArray[104]
                .toString()));
        assertEquals(objArray[102].toString(), map.lowerKey(objArray[101]
                .toString()));
        assertEquals(null, map.lowerKey(objArray[102].toString()));
        assertEquals(null, map.lowerKey(objArray[103].toString()));
        assertEquals(null, map.lowerKey(objArray[104].toString()));
        assertEquals(objArray[102].toString(), map.floorKey(objArray[101]
                .toString()));
        assertEquals(objArray[102].toString(), map.floorKey(objArray[102]
                .toString()));
        assertEquals(null, map.floorKey(objArray[103].toString()));
        assertEquals(null, map.floorKey(objArray[104].toString()));
    }

    public void test_lowerkey() throws Exception {
        try {
            tm.subMap(objArray[100].toString(), true, objArray[100].toString(),
                    false).descendingMap().firstKey();
            fail("should throw NoSuchElementException");
        } catch (Exception e) {
            // expected
        }
        try {
            tm.subMap(objArray[100].toString(), true, objArray[100].toString(),
                    false).descendingMap().lastKey();
            fail("should throw NoSuchElementException");
        } catch (Exception e) {
            // expected
        }
        try {
            tm.subMap(objArray[100].toString(), true, objArray[100].toString(),
                    false).firstKey();
            fail("should throw NoSuchElementException");
        } catch (Exception e) {
            // expected
        }
        try {
            tm.subMap(objArray[100].toString(), true, objArray[100].toString(),
                    false).lastKey();
            fail("should throw NoSuchElementException");
        } catch (Exception e) {
            // expected
        }

    }

    public void test_headMap() throws Exception {
        TreeMap tree = new TreeMap();
        tree.put(new Integer(0), null);
        tree.put(new Integer(1), null);
        Map submap = tree.subMap(tree.firstKey(), tree.lastKey());
        tree.remove(tree.lastKey());
        assertEquals(submap, tree);
    }

    public void testname() throws Exception {
        TreeMap nullTree = new TreeMap(new Comparator() {
            public int compare(Object o1, Object o2) {
                if (o1 == null) {
                    return o2 == null ? 0 : -1;
                }
                return ((String) o1).compareTo((String) o2);
            }
        });
        nullTree.put(new String("One"), 1);
        nullTree.put(new String("Two"), 2);
        nullTree.put(new String("Three"), 3);
        nullTree.put(new String("Four"), 4);
        nullTree.put(null, 0);
        nullTree.subMap(null, "two").size();
    }

}
