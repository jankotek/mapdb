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

package net.kotek.jdbm;



import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.*;


/**
 * Tests for HashSet which comes with JDBM. Original code comes from Apache Harmony,
 * Modified by Jan Kotek for use in JDBM
 */
public class HashSet2Test extends JdbmTestCase {

    Set hs;

    static Object[] objArray;

    {
        objArray = new Object[1000];
        for (int i = 0; i < objArray.length; i++)
            objArray[i] = new Integer(i);
    }

    /**
     * @tests java.util.HashSet#HashSet()
     */
    @Test public void test_Constructor() {
        // Test for method java.util.HashSet()
        Set hs2 = new HashMap2(recman, false).keySet();
        assertEquals("Created incorrect HashSet", 0, hs2.size());
    }


    /**
     * @tests java.util.HashSet#add(java.lang.Object)
     */
    @Test public void test_addLjava_lang_Object() {
        // Test for method boolean java.util.HashSet.add(java.lang.Object)
        int size = hs.size();
        hs.add(new Integer(8));
        assertTrue("Added element already contained by set", hs.size() == size);
        hs.add(new Integer(-9));
        assertTrue("Failed to increment set size after add",
                hs.size() == size + 1);
        assertTrue("Failed to add element to set", hs.contains(new Integer(-9)));
    }

    /**
     * @tests java.util.HashSet#clear()
     */
    @Test public void test_clear() {
        // Test for method void java.util.HashSet.clear()
        Set orgSet = new java.util.HashSet(hs);
        hs.clear();
        Iterator i = orgSet.iterator();
        assertEquals("Returned non-zero size after clear", 0, hs.size());
        while (i.hasNext())
            assertTrue("Failed to clear set", !hs.contains(i.next()));
    }


    /**
     * @tests java.util.HashSet#contains(java.lang.Object)
     */
    @Test public void test_containsLjava_lang_Object() {
        // Test for method boolean java.util.HashSet.contains(java.lang.Object)
        assertTrue("Returned false for valid object", hs.contains(objArray[90]));
        assertTrue("Returned true for invalid Object", !hs
                .contains(new Object()));

    }

    /**
     * @tests java.util.HashSet#isEmpty()
     */
    @Test public void test_isEmpty() {
        // Test for method boolean java.util.HashSet.isEmpty()
        assertTrue("Empty set returned false", new HashMap2(recman, false).keySet().isEmpty());
        assertTrue("Non-empty set returned true", !hs.isEmpty());
    }

    /**
     * @tests java.util.HashSet#iterator()
     */
    @Test public void test_iterator() {
        // Test for method java.util.Iterator java.util.HashSet.iterator()
        Iterator i = hs.iterator();
        int x = 0;
        while (i.hasNext()) {
            assertTrue("Failed to iterate over all elements", hs.contains(i
                    .next()));
            ++x;
        }
        assertTrue("Returned iteration of incorrect size", hs.size() == x);

    }

    /**
     * @tests java.util.HashSet#remove(java.lang.Object)
     */
    @Test public void test_removeLjava_lang_Object() {
        // Test for method boolean java.util.HashSet.remove(java.lang.Object)
        int size = hs.size();
        hs.remove(new Integer(98));
        assertTrue("Failed to remove element", !hs.contains(new Integer(98)));
        assertTrue("Failed to decrement set size", hs.size() == size - 1);

    }

    /**
     * @tests java.util.HashSet#size()
     */
    @Test public void test_size() {
        // Test for method int java.util.HashSet.size()
        assertTrue("Returned incorrect size", hs.size() == (objArray.length));
        hs.clear();
        assertEquals("Cleared set returned non-zero size", 0, hs.size());
    }


    /**
     * Sets up the fixture, for example, open a network connection. This method
     * is called before a test is executed.
     */
    @Before public void setUp() throws Exception {
        super.setUp();
        hs = new HashMap2(recman, false).keySet();
        for (int i = 0; i < objArray.length; i++)
            hs.add(objArray[i]);
    }




}