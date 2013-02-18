package org.mapdb;

import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * This code comes from GoogleCollections, was modified for JDBM by Jan Kotek
 *
 * Tests representing the contract of {@link java.util.SortedMap}. Concrete subclasses of
 * this base class test conformance of concrete {@link java.util.SortedMap} subclasses to
 * that contract.
 *
 * @author Jared Levy
 *
 */
public class BTreeMapTest3
        extends ConcurrentMapInterfaceTest<Integer, String> {

    public BTreeMapTest3() {
        super(false, false, true, true, true, true, false);
    }


    @Override
    protected Integer getKeyNotInPopulatedMap() throws UnsupportedOperationException {
        return -100;
    }

    @Override
    protected String getValueNotInPopulatedMap() throws UnsupportedOperationException {
        return "XYZ";
    }

    @Override
    protected String getSecondValueNotInPopulatedMap() throws UnsupportedOperationException {
        return "ASD";
    }

    @Override
    protected ConcurrentNavigableMap<Integer, String> makeEmptyMap() throws UnsupportedOperationException {
        return DBMaker.newMemoryDB().make().getTreeMap("test");
    }

    @Override
    protected ConcurrentNavigableMap<Integer, String> makePopulatedMap() throws UnsupportedOperationException {
        ConcurrentNavigableMap<Integer, String> map = makeEmptyMap();
        for (int i = 0; i < 100; i++){
            if(i%11==0||i%7==0) continue;

            map.put(i, "aa" + i);
        }
        return map;
    }
    @Override
    protected ConcurrentNavigableMap<Integer, String> makeEitherMap() {
        try {
            return makePopulatedMap();
        } catch (UnsupportedOperationException e) {
            return makeEmptyMap();
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" }) // Needed for null comparator
    public void testOrdering() {
        final SortedMap<Integer, String> map;
        try {
            map = makePopulatedMap();
        } catch (UnsupportedOperationException e) {
            return;
        }
        Iterator<Integer> iterator = map.keySet().iterator();
        Integer prior = iterator.next();
        Comparator<? super Integer> comparator = map.comparator();
        while (iterator.hasNext()) {
            Integer current = iterator.next();
            if (comparator == null) {
                Comparable comparable = (Comparable) prior;
                assertTrue(comparable.compareTo(current) < 0);
            } else {
                assertTrue(map.comparator().compare(prior, current) < 0);
            }
            current = prior;
        }
    }



    public void testFirstKeyNonEmpty() {
        final SortedMap<Integer, String> map;
        try {
            map = makePopulatedMap();
        } catch (UnsupportedOperationException e) {
            return;
        }
        Integer expected = map.keySet().iterator().next();
        assertEquals(expected, map.firstKey());
        assertInvariants(map);
    }


    public void testLastKeyNonEmpty() {
        final SortedMap<Integer, String> map;
        try {
            map = makePopulatedMap();
        } catch (UnsupportedOperationException e) {
            return;
        }
        Integer expected = null;
        for (Integer key : map.keySet()) {
            expected = key;
        }
        assertEquals(expected, map.lastKey());
        assertInvariants(map);
    }

    private static <E> List<E> toList(Collection<E> collection) {
        return new ArrayList<E>(collection);
    }

    private static <E> List<E> subListSnapshot(
            List<E> list, int fromIndex, int toIndex) {
        List<E> subList = new ArrayList<E>();
        for (int i = fromIndex; i < toIndex; i++) {
            subList.add(list.get(i));
        }
        return Collections.unmodifiableList(subList);
    }

    public void testHeadMap() {
        final NavigableMap<Integer, String> map;
        try {
            map = makeEitherMap();
        } catch (UnsupportedOperationException e) {
            return;
        }
        List<Map.Entry<Integer, String>> list = toList(map.entrySet());
        for (int i = 0; i < list.size(); i++) {
            List<Map.Entry<Integer, String>> expected = subListSnapshot(list, 0, i);
            SortedMap<Integer, String> headMap = map.headMap(list.get(i).getKey());
            assertEquals(expected, toList(headMap.entrySet()));
        }

        for (int i = 0; i < list.size(); i++) {
            List<Map.Entry<Integer, String>> expected = subListSnapshot(list, 0, i+1);
            SortedMap<Integer, String> headMap = map.headMap(list.get(i).getKey(),true);
            assertEquals(expected, toList(headMap.entrySet()));
        }

        for (int i = 0; i < list.size(); i++) {
            List<Map.Entry<Integer, String>> expected = subListSnapshot(list, 0, i);
            SortedMap<Integer, String> headMap = map.headMap(list.get(i).getKey(),false);
            assertEquals(expected, toList(headMap.entrySet()));
        }


    }



    public void testTailMap() {
        final NavigableMap<Integer, String> map;
        try {
            map = makeEitherMap();
        } catch (UnsupportedOperationException e) {
            return;
        }
        List<Map.Entry<Integer, String>> list = toList(map.entrySet());
        for (int i = 0; i < list.size(); i++) {
            List<Map.Entry<Integer, String>> expected = subListSnapshot(list, i, list.size());
            SortedMap<Integer, String> tailMap = map.tailMap(list.get(i).getKey());
            assertEquals(expected, toList(tailMap.entrySet()));
        }

        for (int i = 0; i < list.size(); i++) {
            List<Map.Entry<Integer, String>> expected = subListSnapshot(list, i, list.size());
            SortedMap<Integer, String> tailMap = map.tailMap(list.get(i).getKey(),true);
            assertEquals(expected, toList(tailMap.entrySet()));
        }

        for (int i = 0; i < list.size(); i++) {
            List<Map.Entry<Integer, String>> expected = subListSnapshot(list, i+1, list.size());
            SortedMap<Integer, String> tailMap = map.tailMap(list.get(i).getKey(),false);
            assertEquals(expected, toList(tailMap.entrySet()));
        }


    }


    public void testSubMap() {
        final NavigableMap<Integer, String> map;
        try {
            map = makeEitherMap();
        } catch (UnsupportedOperationException e) {
            return;
        }
        List<Map.Entry<Integer, String>> list = toList(map.entrySet());
        for (int i = 0; i < list.size(); i++) {
            for (int j = i; j < list.size(); j++) {
                List<Map.Entry<Integer, String>> expected = subListSnapshot(list, i, j);
                SortedMap<Integer, String> subMap
                        = map.subMap(list.get(i).getKey(), list.get(j).getKey());
                assertEquals(expected, toList(subMap.entrySet()));
                assertEquals(expected.size(), subMap.size());
                assertEquals(expected.size(), subMap.keySet().size());
                assertEquals(expected.size(), subMap.entrySet().size());
                assertEquals(expected.size(), subMap.values().size());
            }
        }

        for (int i = 0; i < list.size(); i++) {
            for (int j = i; j < list.size(); j++) {
                List<Map.Entry<Integer, String>> expected = subListSnapshot(list, i, j+1);
                SortedMap<Integer, String> subMap
                        = map.subMap(list.get(i).getKey(), true, list.get(j).getKey(), true);
                assertEquals(expected, toList(subMap.entrySet()));
                assertEquals(expected.size(), subMap.size());
                assertEquals(expected.size(), subMap.keySet().size());
                assertEquals(expected.size(), subMap.entrySet().size());
                assertEquals(expected.size(), subMap.values().size());
            }
        }


        for (int i = 0; i < list.size(); i++) {
            for (int j = i; j < list.size(); j++) {
                List<Map.Entry<Integer, String>> expected = subListSnapshot(list, i+1, j);
                SortedMap<Integer, String> subMap
                        = map.subMap(list.get(i).getKey(), false, list.get(j).getKey(), false);
                assertEquals(expected, toList(subMap.entrySet()));
                assertEquals(expected.size(), subMap.size());
                assertEquals(expected.size(), subMap.keySet().size());
                assertEquals(expected.size(), subMap.entrySet().size());
                assertEquals(expected.size(), subMap.values().size());
            }
        }



    }

    public void testSubMapIllegal() {
        final SortedMap<Integer, String> map;
        try {
            map = makePopulatedMap();
        } catch (UnsupportedOperationException e) {
            return;
        }
        if (map.size() < 2) {
            return;
        }
        Iterator<Integer> iterator = map.keySet().iterator();
        Integer first = iterator.next();
        Integer second = iterator.next();
        try {
            map.subMap(second, first);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }
    }




}
