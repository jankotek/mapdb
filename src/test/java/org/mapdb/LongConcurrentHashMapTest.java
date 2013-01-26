/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 * Other contributors include Andrew Wright, Jeffrey Hayes,
 * Pat Fisher, Mike Judd.
 */

package org.mapdb;

import junit.framework.TestCase;

import java.util.Iterator;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class LongConcurrentHashMapTest extends TestCase{

    /**
     * Create a map from Integers 1-5 to Strings "A"-"E".
     */
    private static LongConcurrentHashMap map5() {
        LongConcurrentHashMap map = new LongConcurrentHashMap(5);
        assertTrue(map.isEmpty());
        map.put(1, "A");
        map.put(2, "B");
        map.put(3, "C");
        map.put(4, "D");
        map.put(5, "E");
        assertFalse(map.isEmpty());
        assertEquals(5, map.size());
        return map;
    }

    /**
     *  clear removes all pairs
     */
    public void testClear() {
        LongConcurrentHashMap map = map5();
        map.clear();
        assertEquals(map.size(), 0);
    }



    /**
     *  containsKey returns true for contained key
     */
    public void testContainsKey() {
        LongConcurrentHashMap map = map5();
        assertTrue(map.containsKey(1));
        assertFalse(map.containsKey(0));
    }

    /**
     *  containsValue returns true for held values
     */
    public void testContainsValue() {
        LongConcurrentHashMap map = map5();
        assertTrue(map.containsValue("A"));
        assertFalse(map.containsValue("Z"));
    }

    /**
     *   enumeration returns an enumeration containing the correct
     *   elements
     */
    public void testEnumeration() {
        LongConcurrentHashMap map = map5();
        Iterator e = map.valuesIterator();
        int count = 0;
        while(e.hasNext()){
            count++;
            e.next();
        }
        assertEquals(5, count);
    }

    /**
     *  get returns the correct element at the given key,
     *  or null if not present
     */
    public void testGet() {
        LongConcurrentHashMap map = map5();
        assertEquals("A", (String)map.get(1));
        assertNull(map.get(-1));
    }

    /**
     *  isEmpty is true of empty map and false for non-empty
     */
    public void testIsEmpty() {
        LongConcurrentHashMap empty = new LongConcurrentHashMap();
        LongConcurrentHashMap map = map5();
        assertTrue(empty.isEmpty());
        assertFalse(map.isEmpty());
    }




    /**
     *   putIfAbsent works when the given key is not present
     */
    public void testPutIfAbsent() {
        LongConcurrentHashMap map = map5();
        map.putIfAbsent(6, "Z");
        assertTrue(map.containsKey(6));
    }

    /**
     *   putIfAbsent does not add the pair if the key is already present
     */
    public void testPutIfAbsent2() {
        LongConcurrentHashMap map = map5();
        assertEquals("A", map.putIfAbsent(1, "Z"));
    }

    /**
     *   replace fails when the given key is not present
     */
    public void testReplace() {
        LongConcurrentHashMap map = map5();
        assertNull(map.replace(6, "Z"));
        assertFalse(map.containsKey(6));
    }

    /**
     *   replace succeeds if the key is already present
     */
    public void testReplace2() {
        LongConcurrentHashMap map = map5();
        assertNotNull(map.replace(1, "Z"));
        assertEquals("Z", map.get(1));
    }


    /**
     * replace value fails when the given key not mapped to expected value
     */
    public void testReplaceValue() {
        LongConcurrentHashMap map = map5();
        assertEquals("A", map.get(1));
        assertFalse(map.replace(1, "Z", "Z"));
        assertEquals("A", map.get(1));
    }

    /**
     * replace value succeeds when the given key mapped to expected value
     */
    public void testReplaceValue2() {
        LongConcurrentHashMap map = map5();
        assertEquals("A", map.get(1));
        assertTrue(map.replace(1, "A", "Z"));
        assertEquals("Z", map.get(1));
    }


    /**
     *   remove removes the correct key-value pair from the map
     */
    public void testRemove() {
        LongConcurrentHashMap map = map5();
        map.remove(5);
        assertEquals(4, map.size());
        assertFalse(map.containsKey(5));
    }

    /**
     * remove(key,value) removes only if pair present
     */
    public void testRemove2() {
        LongConcurrentHashMap map = map5();
        map.remove(5, "E");
        assertEquals(4, map.size());
        assertFalse(map.containsKey(5));
        map.remove(4, "A");
        assertEquals(4, map.size());
        assertTrue(map.containsKey(4));

    }

    /**
     *   size returns the correct values
     */
    public void testSize() {
        LongConcurrentHashMap map = map5();
        LongConcurrentHashMap empty = new LongConcurrentHashMap();
        assertEquals(0, empty.size());
        assertEquals(5, map.size());
    }


    // Exception tests

    /**
     * Cannot create with negative capacity
     */
    public void testConstructor1() {
        try {
            new LongConcurrentHashMap(-1,0,1);
            shouldThrow();
        } catch(IllegalArgumentException e){}
    }

    /**
     * Cannot create with negative concurrency level
     */
    public void testConstructor2() {
        try {
            new LongConcurrentHashMap(1,0,-1);
            shouldThrow();
        } catch(IllegalArgumentException e){}
    }

    /**
     * Cannot create with only negative capacity
     */
    public void testConstructor3() {
        try {
            new LongConcurrentHashMap(-1);
            shouldThrow();
        } catch(IllegalArgumentException e){}
    }



    /**
     * containsValue(null) throws NPE
     */
    public void testContainsValue_NullPointerException() {
        try {
            LongConcurrentHashMap c = new LongConcurrentHashMap(5);
            c.containsValue(null);
            shouldThrow();
        } catch(NullPointerException e){}
    }



    /**
     * fail with message "should throw exception"
     */
    public void shouldThrow() {
        fail("Should throw exception");
    }

}
