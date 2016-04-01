package org.mapdb.jsr166Tests;/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 * Other contributors include Andrew Wright, Jeffrey Hayes,
 * Pat Fisher, Mike Judd.
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.junit.Test;

public abstract class ConcurrentHashMapTest extends JSR166Test {

    public abstract ConcurrentMap<Integer, String> makeMap();
    public abstract ConcurrentMap makeGenericMap();

    /**
     * Returns a new map from Integers 1-5 to Strings "A"-"E".
     */
    ConcurrentMap<Integer, String> map5() {
        ConcurrentMap map = makeMap();
        assertTrue(map.isEmpty());
        map.put(one, "A");
        map.put(two, "B");
        map.put(three, "C");
        map.put(four, "D");
        map.put(five, "E");
        assertFalse(map.isEmpty());
        assertEquals(5, map.size());
        return map;
    }

    /** Re-implement Integer.compare for old java versions */
    static int compare(int x, int y) {
        return (x < y) ? -1 : (x > y) ? 1 : 0;
    }

    // classes for testing Comparable fallbacks
    static class BI implements Comparable<BI>,Serializable {
        private final int value;
        BI(int value) { this.value = value; }
        public int compareTo(BI other) {
            return compare(value, other.value);
        }
        public boolean equals(Object x) {
            return (x instanceof BI) && ((BI)x).value == value;
        }
        public int hashCode() { return 42; }
    }
    static class CI extends BI { CI(int value) { super(value); } }
    static class DI extends BI { DI(int value) { super(value); } }

    static class BS implements Comparable<BS>, Serializable {
        private final String value;
        BS(String value) { this.value = value; }
        public int compareTo(BS other) {
            return value.compareTo(other.value);
        }
        public boolean equals(Object x) {
            return (x instanceof BS) && value.equals(((BS)x).value);
        }
        public int hashCode() { return 42; }
    }

    static class LexicographicList<E extends Comparable<E>> extends ArrayList<E>
        implements Comparable<LexicographicList<E>> {
        LexicographicList(Collection<E> c) { super(c); }
        LexicographicList(E e) { super(Collections.singleton(e)); }
        public int compareTo(LexicographicList<E> other) {
            int common = Math.min(size(), other.size());
            int r = 0;
            for (int i = 0; i < common; i++) {
                if ((r = get(i).compareTo(other.get(i))) != 0)
                    break;
            }
            if (r == 0)
                r = compare(size(), other.size());
            return r;
        }
        private static final long serialVersionUID = 0;
    }


    static class CollidingObject implements Serializable {
        final String value;
        CollidingObject(final String value) { this.value = value; }
        public int hashCode() { return this.value.hashCode() & 1; }
        public boolean equals(final Object obj) {
            return (obj instanceof CollidingObject) && ((CollidingObject)obj).value.equals(value);
        }
    }

    static class ComparableCollidingObject extends CollidingObject implements Comparable<ComparableCollidingObject>,Serializable {
        ComparableCollidingObject(final String value) { super(value); }
        public int compareTo(final ComparableCollidingObject o) {
            return value.compareTo(o.value);
        }
    }

    /**
     * Inserted elements that are subclasses of the same Comparable
     * class are found.
     */
    @Test public void testComparableFamily() {
        int size = 500;         // makes measured test run time -> 60ms
        ConcurrentMap<BI, Boolean> m =
                makeGenericMap();
        for (int i = 0; i < size; i++) {
            assertTrue(m.put(new CI(i), true) == null);
        }
        for (int i = 0; i < size; i++) {
            assertTrue(m.containsKey(new CI(i)));
            assertTrue(m.containsKey(new DI(i)));
        }
    }

    /**
     * Elements of classes with erased generic type parameters based
     * on Comparable can be inserted and found.
     */
    @Test public void testGenericComparable() {
        int size = 120;         // makes measured test run time -> 60ms
        ConcurrentMap<Object, Boolean> m =
                makeGenericMap();
        for (int i = 0; i < size; i++) {
            BI bi = new BI(i);
            BS bs = new BS(String.valueOf(i));
            LexicographicList<BI> bis = new LexicographicList<BI>(bi);
            LexicographicList<BS> bss = new LexicographicList<BS>(bs);
            assertTrue(m.putIfAbsent(bis, true) == null);
            assertTrue(m.containsKey(bis));
            if (m.putIfAbsent(bss, true) == null)
                assertTrue(m.containsKey(bss));
            assertTrue(m.containsKey(bis));
        }
        for (int i = 0; i < size; i++) {
            assertTrue(m.containsKey(Collections.singletonList(new BI(i))));
        }
    }

    /**
     * Elements of non-comparable classes equal to those of classes
     * with erased generic type parameters based on Comparable can be
     * inserted and found.
     */
    @Test public void testGenericComparable2() {
        int size = 500;         // makes measured test run time -> 60ms
        ConcurrentMap<Object, Boolean> m =
                makeGenericMap();
        for (int i = 0; i < size; i++) {
            m.put(Collections.singletonList(new BI(i)), true);
        }

        for (int i = 0; i < size; i++) {
            LexicographicList<BI> bis = new LexicographicList<BI>(new BI(i));
            assertTrue(m.containsKey(bis));
        }
    }

    /**
     * Mixtures of instances of comparable and non-comparable classes
     * can be inserted and found.
     */
    @Test public void testMixedComparable() {
        int size = 1200;        // makes measured test run time -> 35ms
        ConcurrentMap<Object, Object> map =
                makeGenericMap();
        Random rng = new Random();
        for (int i = 0; i < size; i++) {
            Object x;
            switch (rng.nextInt(4)) {
            case 0:
                x = new CollidingObject(Integer.toString(i));
                break;
            default:
                x = new ComparableCollidingObject(Integer.toString(i));
            }
            assertNull(map.put(x, x));
        }
        int count = 0;
        for (Object k : map.keySet()) {
            assertEquals(map.get(k), k);
            ++count;
        }
        assertEquals(count, size);
        assertEquals(map.size(), size);
        for (Object k : map.keySet()) {
            assertEquals(map.put(k, k), k);
        }
    }

    /**
     * clear removes all pairs
     */
    @Test public void testClear() {
        ConcurrentMap map = map5();
        map.clear();
        assertEquals(0, map.size());
    }

    /**
     * Maps with same contents are equal
     */
    @Test public void testEquals() {
        ConcurrentMap map1 = map5();
        ConcurrentMap map2 = map5();
        assertEquals(map1, map2);
        assertEquals(map2, map1);
        map1.clear();
        assertFalse(map1.equals(map2));
        assertFalse(map2.equals(map1));
    }

    //TODO hash code
//    /**
//     * hashCode() equals sum of each key.hashCode ^ value.hashCode
//     */
//    @Test public void testHashCode() {
//        ConcurrentMap<Integer,String> map = map5();
//        int sum = 0;
//        for (Map.Entry<Integer,String> e : map.entrySet())
//            sum += e.getKey().hashCode() ^ e.getValue().hashCode();
//        assertEquals(sum, map.hashCode());
//    }

    /**
     * contains returns true for contained value
     */
    @Test public void testContains() {
        ConcurrentMap map = map5();
        assertTrue(map.containsValue("A"));
        assertFalse(map.containsValue("Z"));
    }

    /**
     * containsKey returns true for contained key
     */
    @Test public void testContainsKey() {
        ConcurrentMap map = map5();
        assertTrue(map.containsKey(one));
        assertFalse(map.containsKey(zero));
    }

    /**
     * containsValue returns true for held values
     */
    @Test public void testContainsValue() {
        ConcurrentMap map = map5();
        assertTrue(map.containsValue("A"));
        assertFalse(map.containsValue("Z"));
    }


    /**
     * get returns the correct element at the given key,
     * or null if not present
     */
    @Test public void testGet() {
        ConcurrentMap map = map5();
        assertEquals("A", (String)map.get(one));
        ConcurrentMap empty = makeGenericMap();
        assertNull(map.get(111111));
        assertNull(empty.get(111111111));
    }

    /**
     * isEmpty is true of empty map and false for non-empty
     */
    @Test public void testIsEmpty() {
        ConcurrentMap empty = makeMap();
        ConcurrentMap map = map5();
        assertTrue(empty.isEmpty());
        assertFalse(map.isEmpty());
    }


    /**
     * keySet returns a Set containing all the keys
     */
    @Test public void testKeySet() {
        ConcurrentMap map = map5();
        Set s = map.keySet();
        assertEquals(5, s.size());
        assertTrue(s.contains(one));
        assertTrue(s.contains(two));
        assertTrue(s.contains(three));
        assertTrue(s.contains(four));
        assertTrue(s.contains(five));
    }

    /**
     * keySet.toArray returns contains all keys
     */
    @Test public void testKeySetToArray() {
        ConcurrentMap map = map5();
        Set s = map.keySet();
        Object[] ar = s.toArray();
        assertTrue(s.containsAll(Arrays.asList(ar)));
        assertEquals(5, ar.length);
        ar[0] = m10;
        assertFalse(s.containsAll(Arrays.asList(ar)));
    }

    /**
     * Values.toArray contains all values
     */
    @Test public void testValuesToArray() {
        ConcurrentMap map = map5();
        Collection v = map.values();
        Object[] ar = v.toArray();
        ArrayList s = new ArrayList(Arrays.asList(ar));
        assertEquals(5, ar.length);
        assertTrue(s.contains("A"));
        assertTrue(s.contains("B"));
        assertTrue(s.contains("C"));
        assertTrue(s.contains("D"));
        assertTrue(s.contains("E"));
    }

    /**
     * entrySet.toArray contains all entries
     */
    @Test public void testEntrySetToArray() {
        ConcurrentMap map = map5();
        Set s = map.entrySet();
        Object[] ar = s.toArray();
        assertEquals(5, ar.length);
        for (int i = 0; i < 5; ++i) {
            assertTrue(map.containsKey(((Map.Entry)(ar[i])).getKey()));
            assertTrue(map.containsValue(((Map.Entry)(ar[i])).getValue()));
        }
    }

    /**
     * values collection contains all values
     */
    @Test public void testValues() {
        ConcurrentMap map = map5();
        Collection s = map.values();
        assertEquals(5, s.size());
        assertTrue(s.contains("A"));
        assertTrue(s.contains("B"));
        assertTrue(s.contains("C"));
        assertTrue(s.contains("D"));
        assertTrue(s.contains("E"));
    }

    /**
     * entrySet contains all pairs
     */
    @Test public void testEntrySet() {
        ConcurrentMap map = map5();
        Set s = map.entrySet();
        assertEquals(5, s.size());
        Iterator it = s.iterator();
        while (it.hasNext()) {
            Map.Entry e = (Map.Entry) it.next();
            assertTrue(
                       (e.getKey().equals(one) && e.getValue().equals("A")) ||
                       (e.getKey().equals(two) && e.getValue().equals("B")) ||
                       (e.getKey().equals(three) && e.getValue().equals("C")) ||
                       (e.getKey().equals(four) && e.getValue().equals("D")) ||
                       (e.getKey().equals(five) && e.getValue().equals("E")));
        }
    }

    /**
     * putAll adds all key-value pairs from the given map
     */
    @Test public void testPutAll() {
        ConcurrentMap empty = makeMap();
        ConcurrentMap map = map5();
        empty.putAll(map);
        assertEquals(5, empty.size());
        assertTrue(empty.containsKey(one));
        assertTrue(empty.containsKey(two));
        assertTrue(empty.containsKey(three));
        assertTrue(empty.containsKey(four));
        assertTrue(empty.containsKey(five));
    }

    /**
     * putIfAbsent works when the given key is not present
     */
    @Test public void testPutIfAbsent() {
        ConcurrentMap map = map5();
        map.putIfAbsent(six, "Z");
        assertTrue(map.containsKey(six));
    }

    /**
     * putIfAbsent does not add the pair if the key is already present
     */
    @Test public void testPutIfAbsent2() {
        ConcurrentMap map = map5();
        assertEquals("A", map.putIfAbsent(one, "Z"));
    }

    /**
     * replace fails when the given key is not present
     */
    @Test public void testReplace() {
        ConcurrentMap map = map5();
        assertNull(map.replace(six, "Z"));
        assertFalse(map.containsKey(six));
    }

    /**
     * replace succeeds if the key is already present
     */
    @Test public void testReplace2() {
        ConcurrentMap map = map5();
        assertNotNull(map.replace(one, "Z"));
        assertEquals("Z", map.get(one));
    }

    /**
     * replace value fails when the given key not mapped to expected value
     */
    @Test public void testReplaceValue() {
        ConcurrentMap map = map5();
        assertEquals("A", map.get(one));
        assertFalse(map.replace(one, "Z", "Z"));
        assertEquals("A", map.get(one));
    }

    /**
     * replace value succeeds when the given key mapped to expected value
     */
    @Test public void testReplaceValue2() {
        ConcurrentMap map = map5();
        assertEquals("A", map.get(one));
        assertTrue(map.replace(one, "A", "Z"));
        assertEquals("Z", map.get(one));
    }

    /**
     * remove removes the correct key-value pair from the map
     */
    @Test public void testRemove() {
        ConcurrentMap map = map5();
        map.remove(five);
        assertEquals(4, map.size());
        assertFalse(map.containsKey(five));
    }

    /**
     * remove(key,value) removes only if pair present
     */
    @Test public void testRemove2() {
        ConcurrentMap map = map5();
        map.remove(five, "E");
        assertEquals(4, map.size());
        assertFalse(map.containsKey(five));
        map.remove(four, "A");
        assertEquals(4, map.size());
        assertTrue(map.containsKey(four));
    }

    /**
     * size returns the correct values
     */
    @Test public void testSize() {
        ConcurrentMap map = map5();
        ConcurrentMap empty = makeMap();
        assertEquals(0, empty.size());
        assertEquals(5, map.size());
    }

//    /**
//     * toString contains toString of elements
//     */
//    @Test public void testToString() {
//        ConcurrentMap map = map5();
//        String s = map.toString();
//        for (int i = 1; i <= 5; ++i) {
//            assertTrue(s.contains(String.valueOf(i)));
//        }
//    }
//

    /**
     * get(null) throws NPE
     */
    @Test public void testGet_NullPointerException() {
        ConcurrentMap c = makeMap();
        try {
            c.get(null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * containsKey(null) throws NPE
     */
    @Test public void testContainsKey_NullPointerException() {
        ConcurrentMap c = makeMap();
        try {
            c.containsKey(null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * containsValue(null) throws NPE
     */
    @Test public void testContainsValue_NullPointerException() {
        ConcurrentMap c = makeMap();
        try {
            c.containsValue(null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * contains(null) throws NPE
     */
    @Test public void testContains_NullPointerException() {
        ConcurrentMap c = makeMap();
        try {
            c.containsKey(null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * put(null,x) throws NPE
     */
    @Test public void testPut1_NullPointerException() {
        ConcurrentMap c = makeMap();
        try {
            c.put(null, "whatever");
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * put(x, null) throws NPE
     */
    @Test public void testPut2_NullPointerException() {
        ConcurrentMap c = makeMap();
        try {
            c.put("whatever", null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * putIfAbsent(null, x) throws NPE
     */
    @Test public void testPutIfAbsent1_NullPointerException() {
        ConcurrentMap c = makeMap();
        try {
            c.putIfAbsent(null, "whatever");
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * replace(null, x) throws NPE
     */
    @Test public void testReplace_NullPointerException() {
        ConcurrentMap c = makeMap();
        try {
            c.replace(null, "whatever");
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * replace(null, x, y) throws NPE
     */
    @Test public void testReplaceValue_NullPointerException() {
        ConcurrentMap c = makeMap();
        try {
            c.replace(null, one, "whatever");
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * putIfAbsent(x, null) throws NPE
     */
    @Test public void testPutIfAbsent2_NullPointerException() {
        ConcurrentMap c = makeMap();
        try {
            c.putIfAbsent("whatever", null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * replace(x, null) throws NPE
     */
    @Test public void testReplace2_NullPointerException() {
        ConcurrentMap c = makeMap();
        try {
            c.replace("whatever", null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * replace(x, null, y) throws NPE
     */
    @Test public void testReplaceValue2_NullPointerException() {
        ConcurrentMap c = makeMap();
        try {
            c.replace("whatever", null, "A");
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * replace(x, y, null) throws NPE
     */
    @Test public void testReplaceValue3_NullPointerException() {
        ConcurrentMap c = makeMap();
        try {
            c.replace("whatever", one, null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * remove(null) throws NPE
     */
    @Test public void testRemove1_NullPointerException() {
        ConcurrentMap c = makeGenericMap();
        c.put("sadsdf", "asdads");
        try {
            c.remove(null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * remove(null, x) throws NPE
     */
    @Test public void testRemove2_NullPointerException() {
        ConcurrentMap c = makeGenericMap();
        c.put("sadsdf", "asdads");
        try {
            c.remove(null, "whatever");
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * remove(x, null) returns false
     */
    @Test(expected = NullPointerException.class)
    public void testRemove3() {
        ConcurrentMap c = makeGenericMap();
        c.put("sadsdf", "asdads");
        c.remove("sadsdf", null);
    }

    //TODO serialization
//    /**
//     * A deserialized map equals original
//     */
//    @Test public void testSerialization() throws Exception {
//        Map x = map5();
//        Map y = serialClone(x);
//
//        assertNotSame(x, y);
//        assertEquals(x.size(), y.size());
//        assertEquals(x, y);
//        assertEquals(y, x);
//    }

    /**
     * SetValue of an EntrySet entry sets value in the map.
     */
    @Test public void testSetValueWriteThrough() {
        // Adapted from a bug report by Eric Zoerner
        ConcurrentMap map = makeGenericMap();
        assertTrue(map.isEmpty());
        for (int i = 0; i < 20; i++)
            map.put(new Integer(i), new Integer(i));
        assertFalse(map.isEmpty());
        Map.Entry entry1 = (Map.Entry)map.entrySet().iterator().next();
        // Unless it happens to be first (in which case remainder of
        // test is skipped), remove a possibly-colliding key from map
        // which, under some implementations, may cause entry1 to be
        // cloned in map
        if (!entry1.getKey().equals(new Integer(16))) {
            map.remove(new Integer(16));
            entry1.setValue("XYZ");
            assertTrue(map.containsValue("XYZ")); // fails if write-through broken
        }
    }

}
