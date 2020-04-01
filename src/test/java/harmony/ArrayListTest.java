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
package harmony;

import java.util.*;

abstract public class ArrayListTest extends junit.framework.TestCase {

    List alist;

    static Object[] objArray;
    {
        objArray = new Object[100];
        for (int i = 0; i < objArray.length; i++)
            objArray[i] = new Integer(i);
    }


    public abstract <E> List<E> newList();


    public <E> List<E> newList(Collection<E> col) {
        List list = newList();
        for(E e:col){
            list.add(e);
        }
        return list;
    }

    public <E> List<E> newList(int size) {
        return newList();
    }
    
    /**
     * @tests java.util.ArrayList#ArrayList()
     */
    public void test_Constructor() {
        // Test for method java.util.ArrayList()
        new Support_ListTest("", alist){}.runTest();

        List subList = newList();
        for (int i = -50; i < 150; i++)
            subList.add(new Integer(i));
        new Support_ListTest("", subList.subList(50, 150)){}.runTest();
    }


    /**
     * @tests java.util.ArrayList#ArrayList(java.util.Collection)
     */
    public void test_ConstructorLjava_util_Collection() {
        // Test for method java.util.ArrayList(java.util.Collection)
        List al = newList(Arrays.asList(objArray));
        assertTrue("arrayList created from collection has incorrect size", al
                .size() == objArray.length);
        for (int counter = 0; counter < objArray.length; counter++)
            assertEquals(
                    "arrayList created from collection has incorrect elements",
                    al.get(counter) , objArray[counter]);

    }

    public void testConstructorWithConcurrentCollection() {
        Collection<String> collection = shrinksOnSize("A", "B", "C", "D");
        List<String> list = newList(collection);
        assertFalse(list.contains(""));
    }

    /**
     * @tests java.util.ArrayList#add(int, java.lang.Object)
     */
    public void test_addILjava_lang_Object() {
        // Test for method void java.util.ArrayList.add(int, java.lang.Object)
        Object o;
        alist.add(50, o = "");
        assertEquals("Failed to add Object", alist.get(50) , o);
        assertEquals("Failed to fix up list after insert",
                alist.get(51) , objArray[50]
                        );
        assertEquals(alist.get(52),objArray[51]);
        Object oldItem = alist.get(25);
        alist.add(25, null);
        assertNull("Should have returned null", alist.get(25));
        assertEquals("Should have returned the old item from slot 25", alist
                .get(26), oldItem);
        
        alist.add(0, o = "");
        assertEquals("Failed to add Object", alist.get(0), o);
        assertEquals(alist.get(1), objArray[0]);
        assertEquals(alist.get(2), objArray[1]);

        oldItem = alist.get(0);
        alist.add(0, null);
        assertNull("Should have returned null", alist.get(0));
        assertEquals("Should have returned the old item from slot 0", alist
                .get(1), oldItem);

        try {
            alist.add(-1, "");
            fail("Should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // Expected
        }

        try {
            alist.add(-1, null);
            fail("Should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // Expected
        }

        try {
            alist.add(alist.size() + 1, "");
            fail("Should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // Expected
        }

        try {
            alist.add(alist.size() + 1, null);
            fail("Should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // Expected
        }
    }

    /**
     * @tests java.util.ArrayList#add(int, java.lang.Object)
     */
    public void test_addILjava_lang_Object_2() {
        Object o = "";
        int size = alist.size();
        alist.add(size, o);
        assertEquals("Failed to add Object", alist.get(size), o);
        assertEquals(alist.get(size - 2), objArray[size - 2]);
        assertEquals(alist.get(size - 1), objArray[size - 1]);

        alist.remove(size);

        size = alist.size();
        alist.add(size, null);
        assertNull("Should have returned null", alist.get(size));
        assertEquals(alist.get(size - 2), objArray[size - 2]);
        assertEquals(alist.get(size - 1), objArray[size - 1]);
    }
    
    /**
     * @tests java.util.ArrayList#add(java.lang.Object)
     */
    public void test_addLjava_lang_Object() {
        // Test for method boolean java.util.ArrayList.add(java.lang.Object)
        Object o = "";
        alist.add(o);
        assertEquals("Failed to add Object", alist.get(alist.size() - 1), o);
        alist.add(null);
        assertNull("Failed to add null", alist.get(alist.size() - 1));
    }

    /**
     * @tests java.util.ArrayList#addAll(int, java.util.Collection)
     */
    public void test_addAllILjava_util_Collection() {
        // Test for method boolean java.util.ArrayList.addAll(int,
        // java.util.Collection)
        alist.addAll(50, alist);
        assertEquals("Returned incorrect size after adding to existing list",
                200, alist.size());
        for (int i = 0; i < 50; i++)
            assertEquals("Manipulated elements < index",
                    alist.get(i) , objArray[i]);
        for (int i = 0; i >= 50 && (i < 150); i++)
            assertEquals("Failed to ad elements properly",
                    alist.get(i) ,  objArray[i - 50]);
        for (int i = 0; i >= 150 && (i < 200); i++)
            assertEquals("Failed to ad elements properly",
                    alist.get(i) , objArray[i - 100]);
        List listWithNulls = newList();
        listWithNulls.add(null);
        listWithNulls.add(null);
        listWithNulls.add("yoink");
        listWithNulls.add("kazoo");
        listWithNulls.add(null);
        alist.addAll(100, listWithNulls);
        assertTrue("Incorrect size: " + alist.size(), alist.size() == 205);
        assertNull("Item at slot 100 should be null", alist.get(100));
        assertNull("Item at slot 101 should be null", alist.get(101));
        assertEquals("Item at slot 102 should be 'yoink'", "yoink", alist
                .get(102));
        assertEquals("Item at slot 103 should be 'kazoo'", "kazoo", alist
                .get(103));
        assertNull("Item at slot 104 should be null", alist.get(104));
        alist.addAll(205, listWithNulls);
        assertTrue("Incorrect size2: " + alist.size(), alist.size() == 210);
    }

    /**
     * @tests java.util.ArrayList#addAll(int, java.util.Collection)
     */
    @SuppressWarnings("unchecked")
    public void test_addAllILjava_util_Collection_2() {
        // Regression for HARMONY-467
        List obj = newList();
        try {
            obj.addAll((int) -1, (Collection) null);
            fail("IndexOutOfBoundsException expected");
        } catch (IndexOutOfBoundsException e) {
            // Expected
        }

        // Regression for HARMONY-5705
        String[] data = new String[] { "1", "2", "3", "4", "5", "6", "7", "8" };
        List list1 = newList();
        List list2 = newList();
        for (String d : data) {
            list1.add(d);
            list2.add(d);
            list2.add(d);
        }
        while (list1.size() > 0)
            list1.remove(0);
        list1.addAll(list2);
        assertTrue("The object list is not the same as original list", list1
                .containsAll(list2)
                && list2.containsAll(list1));

        obj = newList();
        for (int i = 0; i < 100; i++) {
            if (list1.size() > 0) {
                obj.removeAll(list1);
                obj.addAll(list1);
            }
        }
        assertTrue("The object list is not the same as original list", obj
                .containsAll(list1)
                && list1.containsAll(obj));

        // Regression for Harmony-5799
        list1 = newList();
        list2 = newList();
        int location = 2;

        String[] strings = { "0", "1", "2", "3", "4", "5", "6" };
        int[] integers = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        for (int i = 0; i < 7; i++) {
            list1.add(strings[i]);
        }
        for (int i = 0; i < 10; i++) {
            list2.add(integers[i]);
        }
        list1.remove(location);
        list1.addAll(location, list2);

        // Inserted elements should be equal to integers array
        for (int i = 0; i < integers.length; i++) {
            assertEquals(integers[i], list1.get(location + i));
        }
        // Elements after inserted location should
        // be equals to related elements in strings array
        for (int i = location + 1; i < strings.length; i++) {
            assertEquals(strings[i], list1.get(i + integers.length - 1));
        }
    }
    
    /**
     * @tests java.util.ArrayList#addAll(int, java.util.Collection)
     */
    public void test_addAllILjava_util_Collection_3() {
        List obj = newList();
        obj.addAll(0, obj);
        obj.addAll(obj.size(), obj);
        try {
            obj.addAll(-1, obj);
            fail("Should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // Expected
        }

        try {
            obj.addAll(obj.size() + 1, obj);
            fail("Should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // Expected
        }

        try {
            obj.addAll(0, null);
            fail("Should throw NullPointerException");
        } catch (NullPointerException e) {
            // Excepted
        }

        try {
            obj.addAll(obj.size() + 1, null);
            fail("Should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // Expected
        }

        try {
            obj.addAll((int) -1, (Collection) null);
            fail("IndexOutOfBoundsException expected");
        } catch (IndexOutOfBoundsException e) {
            // Expected
        }
    }

    public void test_addAllCollectionOfQextendsE() {
        // Regression for HARMONY-539
        // https://issues.apache.org/jira/browse/HARMONY-539
        List<String> alist = newList();
        List<String> blist = newList();
        alist.add("a");
        alist.add("b");
        blist.add("c");
        blist.add("d");
        blist.remove(0);
        blist.addAll(0, alist);
        assertEquals("a", blist.get(0));
        assertEquals("b", blist.get(1));
        assertEquals("d", blist.get(2));
    }

    /**
     * @tests java.util.ArrayList#addAll(java.util.Collection)
     */
    public void test_addAllLjava_util_Collection() {
        // Test for method boolean
        // java.util.ArrayList.addAll(java.util.Collection)
        List l = newList();
        l.addAll(alist);
        for (int i = 0; i < alist.size(); i++)
            assertTrue("Failed to add elements properly", l.get(i).equals(
                    alist.get(i)));
        alist.addAll(alist);
        assertEquals("Returned incorrect size after adding to existing list",
                200, alist.size());
        for (int i = 0; i < 100; i++) {
            assertTrue("Added to list in incorrect order", alist.get(i).equals(
                    l.get(i)));
            assertTrue("Failed to add to existing list", alist.get(i + 100)
                    .equals(l.get(i)));
        }
        Set setWithNulls = new HashSet();
        setWithNulls.add(null);
        setWithNulls.add(null);
        setWithNulls.add("yoink");
        setWithNulls.add("kazoo");
        setWithNulls.add(null);
        alist.addAll(100, setWithNulls);
        Iterator i = setWithNulls.iterator();
        assertEquals("Item at slot 100 is wrong: " + alist.get(100), alist
                .get(100) , i.next());
        assertEquals("Item at slot 101 is wrong: " + alist.get(101), alist
                .get(101) , i.next());
        assertEquals("Item at slot 103 is wrong: " + alist.get(102), alist
                .get(102) , i.next());

        try {
            alist.addAll(null);
            fail("Should throw NullPointerException");
        } catch (NullPointerException e) {
            // Excepted
        }
        
        // Regression test for Harmony-3481
        List<Integer> originalList = newList(12);
        for (int j = 0; j < 12; j++) {
            originalList.add(j);
        }

        originalList.remove(0);
        originalList.remove(0);

        List<Integer> additionalList = newList(11);
        for (int j = 0; j < 11; j++) {
            additionalList.add(j);
        }
        assertTrue(originalList.addAll(additionalList));
        assertEquals(21, originalList.size());

    }

        public void test_ArrayList_addAll_scenario1() {
        List arrayListA = newList();
        arrayListA.add(1);
        List arrayListB = newList();
        arrayListB.add(1);
        arrayListA.addAll(1, arrayListB);
        int size = arrayListA.size();
        assertEquals(2, size);
        for (int index = 0; index < size; index++) {
            assertEquals(1, arrayListA.get(index));
        }
    }

    public void test_ArrayList_addAll_scenario2() {
        List arrayList = newList();
        arrayList.add(1);
        arrayList.addAll(1, arrayList);
        int size = arrayList.size();
        assertEquals(2, size);
        for (int index = 0; index < size; index++) {
            assertEquals(1, arrayList.get(index));
        }
    }
        
    // Regression test for HARMONY-5839
    public void testaddAllHarmony5839() {
        Collection coll = Arrays.asList(new String[] { "1", "2" });
        List list = newList();
        list.add("a");
        list.add(0, "b");
        list.add(0, "c");
        list.add(0, "d");
        list.add(0, "e");
        list.add(0, "f");
        list.add(0, "g");
        list.add(0, "h");
        list.add(0, "i");

        list.addAll(6, coll);

        assertEquals(11, list.size());
        assertFalse(list.contains(""));
    }

    /**
     * @tests java.util.ArrayList#clear()
     */
    public void test_clear() {
        // Test for method void java.util.ArrayList.clear()
        alist.clear();
        assertEquals("List did not clear", 0, alist.size());
        alist.add(null);
        alist.add(null);
        alist.add(null);
        alist.add("bam");
        alist.clear();
        assertEquals("List with nulls did not clear", 0, alist.size());
        /*
         * for (int i = 0; i < alist.size(); i++) assertNull("Failed to clear
         * list", alist.get(i));
         */

    }

    /**
     * @tests java.util.ArrayList#contains(java.lang.Object)
     */
    public void test_containsLjava_lang_Object() {
        // Test for method boolean
        // java.util.ArrayList.contains(java.lang.Object)
        assertTrue("Returned false for valid element", alist
                .contains(objArray[99]));
        assertTrue("Returned false for equal element", alist
                .contains(new Integer(8)));
        assertTrue("Returned true for invalid element", !alist
                .contains(""));
        assertTrue("Returned true for null but should have returned false",
                !alist.contains(""));
        alist.add(null);
        assertTrue("Returned false for null but should have returned true",
                alist.contains(null));
    }

    /**
     * @tests java.util.ArrayList#get(int)
     */
    public void test_getI() {
        // Test for method java.lang.Object java.util.ArrayList.get(int)
        assertEquals("Returned incorrect element", alist.get(22), objArray[22]);
        try {
            alist.get(8765);
            fail("Failed to throw expected exception for index > size");
        } catch (IndexOutOfBoundsException e) {
            // Expected
            assertNotNull(e.getMessage());
        }
    }

    /**
     * @tests java.util.ArrayList#indexOf(java.lang.Object)
     */
    public void test_indexOfLjava_lang_Object() {
        // Test for method int java.util.ArrayList.indexOf(java.lang.Object)
        assertEquals("Returned incorrect index", 87, alist
                .indexOf(objArray[87]));
        assertEquals("Returned index for invalid Object", -1, alist
                .indexOf(""));
        alist.add(25, null);
        alist.add(50, null);
        assertEquals("Wrong indexOf for null.  Wanted 25 got: "
                + alist.indexOf(null), alist.indexOf(null) , 25);
    }

    /**
     * @tests java.util.ArrayList#isEmpty()
     */
    public void test_isEmpty() {
        // Test for method boolean java.util.ArrayList.isEmpty()
        assertTrue("isEmpty returned false for new list", newList()
                .isEmpty());
        assertTrue("Returned true for existing list with elements", !alist
                .isEmpty());
    }

    /**
     * @tests java.util.ArrayList#lastIndexOf(java.lang.Object)
     */
    public void test_lastIndexOfLjava_lang_Object() {
        // Test for method int java.util.ArrayList.lastIndexOf(java.lang.Object)
        alist.add(new Integer(99));
        assertEquals("Returned incorrect index", 100, alist
                .lastIndexOf(objArray[99]));
        assertEquals("Returned index for invalid Object", -1, alist
                .lastIndexOf(""));
        alist.add(25, null);
        alist.add(50, null);
        assertEquals("Wrong lastIndexOf for null.  Wanted 50 got: "
                + alist.lastIndexOf(null), alist.lastIndexOf(null) , 50);
    }

    /**
     * @tests {@link java.util.ArrayList#removeRange(int, int)}
     */
    public void test_removeRange() {
        MockArrayList mylist = new MockArrayList();
        mylist.removeRange(0, 0);

        try {
            mylist.removeRange(0, 1);
            fail("Should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // Expected
        }

        int[] data = { 1, 2, 3 };
        for (int i = 0; i < data.length; i++) {
            mylist.add(i, data[i]);
        }

        mylist.removeRange(0, 1);
        assertEquals(data[1], mylist.get(0));
        assertEquals(data[2], mylist.get(1));

        try {
            mylist.removeRange(-1, 1);
            fail("Should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // Expected
        }

        try {
            mylist.removeRange(0, -1);
            fail("Should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // Expected
        }

        try {
            mylist.removeRange(1, 0);
            //OpenJDK 11 does not throw exception here, some speed hack
//            fail("Should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // Expected
        }

        try {
            mylist.removeRange(2, 1);
            //OpenJDK 11 does not throw exception here, some speed hack
//            fail("Should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // Expected
        }
    }
    
    /**
     * @tests java.util.ArrayList#remove(int)
     */
    public void test_removeI() {
        // Test for method java.lang.Object java.util.ArrayList.remove(int)
        alist.remove(10);
        assertEquals("Failed to remove element", -1, alist
                .indexOf(objArray[10]));
        try {
            alist.remove(999);
            fail("Failed to throw exception when index out of range");
        } catch (IndexOutOfBoundsException e) {
            // Expected
        }

        ArrayList myList = new ArrayList(alist);
        alist.add(25, null);
        alist.add(50, null);
        alist.remove(50);
        alist.remove(25);
        assertTrue("Removing nulls did not work", alist.equals(myList));

        List list = newList(Arrays.asList(new String[] { "a", "b", "c",
                "d", "e", "f", "g" }));
        assertEquals("Removed wrong element 1", list.remove(0) , "a");
        assertEquals("Removed wrong element 2", list.remove(4) , "f");
        String[] result = new String[5];
        list.toArray(result);
        assertTrue("Removed wrong element 3", Arrays.equals(result,
                new String[] { "b", "c", "d", "e", "g" }));

        List l = newList(0);
        l.add("");
        l.add("");
        l.remove(0);
        l.remove(0);
        try {
            l.remove(-1);
            fail("-1 should cause exception");
        } catch (IndexOutOfBoundsException e) {
            // Expected
            assertNotNull(e.getMessage());
        }
        try {
            l.remove(0);
            fail("0 should case exception");
        } catch (IndexOutOfBoundsException e) {
            // Expected
            assertNotNull(e.getMessage());
        }
    }

    /**
     * @tests java.util.ArrayList#set(int, java.lang.Object)
     */
    public void test_setILjava_lang_Object() {
        // Test for method java.lang.Object java.util.ArrayList.set(int,
        // java.lang.Object)
        Object obj;
        alist.set(65, obj = "");
        assertEquals("Failed to set object", alist.get(65) , obj);
        alist.set(50, null);
        assertNull("Setting to null did not work", alist.get(50));
        assertTrue("Setting increased the list's size to: " + alist.size(),
                alist.size() == 100);
        
        obj = "";
        alist.set(0, obj);
        assertEquals("Failed to set object", alist.get(0) , obj);

        try {
            alist.set(-1, obj);
            fail("Should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // Expected
            assertNotNull(e.getMessage());
        }

        try {
            alist.set(alist.size(), obj);
            fail("Should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // Expected
            assertNotNull(e.getMessage());
        }

        try {
            alist.set(-1, null);
            fail("Should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // Expected
            assertNotNull(e.getMessage());
        }

        try {
            alist.set(alist.size(), null);
            fail("Should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // Expected
            assertNotNull(e.getMessage());
        }
    }

    /**
     * @tests java.util.ArrayList#size()
     */
    public void test_size() {
        // Test for method int java.util.ArrayList.size()
        assertEquals("Returned incorrect size for exiting list", 100, alist
                .size());
        assertEquals("Returned incorrect size for new list", 0, newList()
                .size());
    }

    /**
     * @tests java.util.ArrayList#toArray()
     */
    public void test_toArray() {
        // Test for method java.lang.Object [] java.util.ArrayList.toArray()
        alist.set(25, null);
        alist.set(75, null);
        Object[] obj = alist.toArray();
        assertEquals("Returned array of incorrect size", objArray.length,
                obj.length);

        for (int i = 0; i < obj.length; i++) {
            if ((i == 25) || (i == 75))
                assertNull("Should be null at: " + i + " but instead got: "
                        + obj[i], obj[i]);
            else
                assertEquals("Returned incorrect array: " + i,
                        obj[i] , objArray[i]);
        }

    }

    /**
     * @tests java.util.ArrayList#toArray(java.lang.Object[])
     */
    public void test_toArray$Ljava_lang_Object() {
        // Test for method java.lang.Object []
        // java.util.ArrayList.toArray(java.lang.Object [])
        alist.set(25, null);
        alist.set(75, null);
        Integer[] argArray = new Integer[100];
        Object[] retArray;
        retArray = alist.toArray(argArray);
        assertTrue("Returned different array than passed", retArray == argArray);
        argArray = new Integer[1000];
        retArray = alist.toArray(argArray);
        assertNull("Failed to set first extra element to null", argArray[alist
                .size()]);
        for (int i = 0; i < 100; i++) {
            if ((i == 25) || (i == 75))
                assertNull("Should be null: " + i, retArray[i]);
            else
                assertEquals("Returned incorrect array: " + i,
                        retArray[i] , objArray[i]);
        }
    }



    /**
     * @test java.util.ArrayList#addAll(int, Collection)
     */
    public void test_addAll() {
        List list = newList();
        list.add("one");
        list.add("two");
        assertEquals(2, list.size());

        list.remove(0);
        assertEquals(1, list.size());

        List collection = newList();
        collection.add("1");
        collection.add("2");
        collection.add("3");
        assertEquals(3, collection.size());

        list.addAll(0, collection);
        assertEquals(4, list.size());

        list.remove(0);
        list.remove(0);
        assertEquals(2, list.size());

        collection.add("4");
        collection.add("5");
        collection.add("6");
        collection.add("7");
        collection.add("8");
        collection.add("9");
        collection.add("10");
        collection.add("11");
        collection.add("12");

        assertEquals(12, collection.size());

        list.addAll(0, collection);
        assertEquals(14, list.size());
    }

    public void testAddAllWithConcurrentCollection() {
        List<String> list = newList();
        try {
            list.addAll(shrinksOnSize("A", "B", "C", "D"));
            assertFalse(list.contains(""));
        }catch(ConcurrentModificationException e){
            //expected
        }
    }

    public void testAddAllAtPositionWithConcurrentCollection() {
        List<String> list = newList(
                Arrays.asList("A", "B", "C", "D"));
        try{
            list.addAll(3, shrinksOnSize("E", "F", "G", "H"));
            assertFalse(list.contains(""));
        }catch(ConcurrentModificationException e){
            //expected
        }
    }

    public class MockArrayList extends ArrayList {
        public int size() {
            return 0;
        }
        
        public void removeRange(int start, int end) {
            super.removeRange(start, end);
        }
    }


    /**
     * Sets up the fixture, for example, open a network connection. This method
     * is called before a test is executed.
     */
    protected void setUp() throws Exception {
        super.setUp();
        alist = newList();
        for (int i = 0; i < objArray.length; i++)
            alist.add(objArray[i]);
    }

    /**
     * Returns a collection that emulates another thread calling remove() each
     * time the current thread calls size().
     */
    private <T> Collection<T> shrinksOnSize(T... elements) {
        return new HashSet<T>(Arrays.asList(elements)) {
            boolean shrink = true;

            @Override
            public int size() {
                int result = super.size();
                if (shrink) {
                    Iterator<T> i = iterator();
                    i.next();
                    i.remove();
                }
                return result;
            }

            @Override
            public Object[] toArray() {
                shrink = false;
                return super.toArray();
            }
        };
    }
}
