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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.Map.Entry;

import junit.framework.TestCase;



public class BTreeMapExtendTest extends TestCase {

    BTreeMap tm;

    BTreeMap tm_comparator;

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

    protected static BTreeMap newBTreeMap() {
        return DBMaker.newMemoryDB().cacheDisable().writeAheadLogDisable().asyncWriteDisable().make().getTreeMap("Test");
    }


    public void test_TreeMap_Constructor_Default() {
        BTreeMap treeMap = newBTreeMap();
        assertTrue(treeMap.isEmpty());
        assertNotNull(treeMap.comparator());
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




    public void test_TreeMap_clear() {
        tm.clear();
        assertEquals(0, tm.size());
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

//        assertFalse(subMap_default.containsValue(null));

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

//        try {
//            subMap_default.subMap(startKey, endKey);
//            fail("should throw IllegalArgumentException");
//        } catch (IllegalArgumentException e) {
//            // Expected
//        }
//
//        try {
//            subMap_startExcluded_endExcluded.subMap(startKey, endKey);
//            fail("should throw IllegalArgumentException");
//        } catch (IllegalArgumentException e) {
//            // Expected
//        }

        subSubMap = subMap_startExcluded_endIncluded.subMap(startKey, endKey);
        assertEquals(0, subSubMap.size());

//        try {
//            subMap_startIncluded_endExcluded.subMap(startKey, endKey);
//            fail("should throw IllegalArgumentException");
//        } catch (IllegalArgumentException e) {
//            // Expected
//        }

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
//
//        try {
//            subMap_default_comparator.subMap(startKey, endKey);
//            fail("should throw IllegalArgumentException");
//        } catch (IllegalArgumentException e) {
//            // Expected
//        }

//        try {
//            subMap_startExcluded_endExcluded_comparator
//                    .subMap(startKey, endKey);
//            fail("should throw IllegalArgumentException");
//        } catch (IllegalArgumentException e) {
//            // Expected
//        }

        subSubMap = subMap_startExcluded_endIncluded_comparator.subMap(
                startKey, endKey);
        assertEquals(0, subSubMap.size());

//        try {
//            subMap_startIncluded_endExcluded_comparator
//                    .subMap(startKey, endKey);
//            fail("should throw IllegalArgumentException");
//        } catch (IllegalArgumentException e) {
//            // Expected
//        }

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
//        try {
//            subMap_default.tailMap(startKey);
//            fail("should throw IllegalArgumentException");
//        } catch (IllegalArgumentException e) {
//            // Expected
//        }
//        try {
//            subMap_startExcluded_endExcluded.tailMap(startKey);
//            fail("should throw IllegalArgumentException");
//        } catch (IllegalArgumentException e) {
//            // Expected
//        }

        tailMap = subMap_startExcluded_endIncluded.tailMap(startKey);
        assertEquals(1, tailMap.size());

//        try {
//            subMap_startIncluded_endExcluded.tailMap(startKey);
//            fail("should throw IllegalArgumentException");
//        } catch (IllegalArgumentException e) {
//            // Expected
//        }

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
//        try {
//            keySet.headSet(endKey, true).size();
//            fail("should throw IllegalArgumentException");
//        } catch (IllegalArgumentException e) {
//            // Expected
//        }

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
//        try {
//            keySet.headSet(endKey, true);
//            fail("should throw IllegalArgumentException");
//        } catch (IllegalArgumentException e) {
//            // Expected
//        }

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
//        try {
//            keySet.headSet(endKey, true).size();
//            fail("should throw IllegalArgumentException");
//        } catch (IllegalArgumentException e) {
//            // Expected
//        }

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
//        try {
//            keySet.headSet(endKey, true);
//            fail("should throw IllegalArgumentException");
//        } catch (IllegalArgumentException e) {
//            // Expected
//        }

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
        SortedMap subMap_startExcluded_endExcluded_rm = tm.subMap(
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
//        try {
//            keySet.tailSet(startKey);
//            fail("should throw IllegalArgumentException");
//        } catch (IllegalArgumentException e) {
//            // Expected
//        }
//        try {
//            keySet.tailSet(startKey, true);
//            fail("should throw IllegalArgumentException");
//        } catch (IllegalArgumentException e) {
//            // Expected
//        }
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
//        try {
//            keySet.tailSet(startKey);
//            fail("should throw IllegalArgumentException");
//        } catch (IllegalArgumentException e) {
//            // Expected
//        }
//        try {
//            keySet.tailSet(startKey, true);
//            fail("should throw IllegalArgumentException");
//        } catch (IllegalArgumentException e) {
//            // Expected
//        }
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
//        try {
//            keySet.tailSet(startKey);
//            fail("should throw IllegalArgumentException");
//        } catch (IllegalArgumentException e) {
//            // Expected
//        }
//        try {
//            keySet.tailSet(startKey, true);
//            fail("should throw IllegalArgumentException");
//        } catch (IllegalArgumentException e) {
//            // Expected
//        }
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
        tm = newBTreeMap();
        tm_comparator = newBTreeMap();
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
//        map = map.descendingMap();
//        assertNull(map.ceilingKey(objArray[100].toString()));
//        assertNull(map.floorKey(objArray[100].toString()));
//        assertNull(map.lowerKey(objArray[100].toString()));
//        assertNull(map.higherKey(objArray[100].toString()));
//        assertNull(map.ceilingKey(objArray[111].toString()));
//        assertNull(map.floorKey(objArray[111].toString()));
//        assertNull(map.lowerKey(objArray[111].toString()));
//        assertNull(map.higherKey(objArray[111].toString()));
//        assertNull(map.ceilingKey(objArray[1].toString()));
//        assertNull(map.floorKey(objArray[1].toString()));
//        assertNull(map.lowerKey(objArray[1].toString()));
//        assertNull(map.higherKey(objArray[1].toString()));
    }

    public void test_lower_tail() throws Exception {
        NavigableMap map = tm.subMap(objArray[102].toString(), true,
                objArray[103].toString(), false);
        assertTrue(map.containsKey(objArray[102].toString()));
        assertFalse(map.containsKey(objArray[101].toString()));
        assertFalse(map.containsKey(objArray[103].toString()));
        assertFalse(map.containsKey(objArray[104].toString()));
//        map = map.descendingMap();
//        assertTrue(map.containsKey(objArray[102].toString()));
//        assertFalse(map.containsKey(objArray[101].toString()));
//        assertFalse(map.containsKey(objArray[103].toString()));
//        assertFalse(map.containsKey(objArray[104].toString()));
        map = tm.subMap(objArray[102].toString(), true, objArray[102]
                .toString(), false);
        assertFalse(map.containsKey(objArray[102].toString()));
        assertFalse(map.containsKey(objArray[101].toString()));
        assertFalse(map.containsKey(objArray[103].toString()));
        assertFalse(map.containsKey(objArray[104].toString()));
//        map = map.descendingMap();
//        assertFalse(map.containsKey(objArray[102].toString()));
//        assertFalse(map.containsKey(objArray[101].toString()));
//        assertFalse(map.containsKey(objArray[103].toString()));
//        assertFalse(map.containsKey(objArray[104].toString()));
    }

    public void test_contains_null() throws Exception {
        NavigableMap map = tm.subMap(objArray[100].toString(), true,
                objArray[100].toString(), false);
        assertFalse(map.containsKey(objArray[100].toString()));
        assertFalse(map.containsKey(objArray[10].toString()));
        assertFalse(map.containsKey(objArray[101].toString()));
        assertFalse(map.containsKey(objArray[102].toString()));
        assertFalse(map.containsKey(objArray[1].toString()));
//        map = map.descendingMap();
//        assertFalse(map.containsKey(objArray[100].toString()));
//        assertFalse(map.containsKey(objArray[10].toString()));
//        assertFalse(map.containsKey(objArray[101].toString()));
//        assertFalse(map.containsKey(objArray[102].toString()));
//        assertFalse(map.containsKey(objArray[1].toString()));
    }

    public void test_contains() throws Exception {
        NavigableMap map = tm.subMap(objArray[102].toString(), true,
                objArray[103].toString(), false);
        assertFalse(map.containsKey(objArray[100].toString()));
        assertFalse(map.containsKey(objArray[104].toString()));
        assertFalse(map.containsKey(objArray[101].toString()));
        assertTrue(map.containsKey(objArray[102].toString()));
//        map = map.descendingMap();
//        assertFalse(map.containsKey(objArray[100].toString()));
//        assertFalse(map.containsKey(objArray[104].toString()));
//        assertFalse(map.containsKey(objArray[101].toString()));
//        assertTrue(map.containsKey(objArray[102].toString()));
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
//        map = map.descendingMap();
//        try {
//            map = map.subMap(objArray[103].toString(), true, objArray[102]
//                    .toString(), true);
//            fail("should throw IAE");
//        } catch (IllegalArgumentException e) {
//        }
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
//        map = map.descendingMap();
//        assertEquals(null, map.higherKey(objArray[101].toString()));
//        assertEquals(null, map.higherKey(objArray[102].toString()));
//        assertEquals(objArray[102].toString(), map.higherKey(objArray[103]
//                .toString()));
//        assertEquals(objArray[102].toString(), map.higherKey(objArray[104]
//                .toString()));
//        assertEquals(null, map.ceilingKey(objArray[101].toString()));
//        assertEquals(objArray[102].toString(), map.ceilingKey(objArray[102]
//                .toString()));
//        assertEquals(objArray[102].toString(), map.ceilingKey(objArray[103]
//                .toString()));
//        assertEquals(objArray[102].toString(), map.ceilingKey(objArray[104]
//                .toString()));
//        assertEquals(objArray[102].toString(), map.lowerKey(objArray[101]
//                .toString()));
//        assertEquals(null, map.lowerKey(objArray[102].toString()));
//        assertEquals(null, map.lowerKey(objArray[103].toString()));
//        assertEquals(null, map.lowerKey(objArray[104].toString()));
//        assertEquals(objArray[102].toString(), map.floorKey(objArray[101]
//                .toString()));
//        assertEquals(objArray[102].toString(), map.floorKey(objArray[102]
//                .toString()));
//        assertEquals(null, map.floorKey(objArray[103].toString()));
//        assertEquals(null, map.floorKey(objArray[104].toString()));
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
        BTreeMap tree = newBTreeMap();
        tree.put(new Integer(0), "11");
        tree.put(new Integer(1), "ads");
        Map submap = tree.subMap(tree.firstKey(), tree.lastKey());
        tree.remove(tree.lastKey());
        assertEquals(submap, tree);
    }


}
