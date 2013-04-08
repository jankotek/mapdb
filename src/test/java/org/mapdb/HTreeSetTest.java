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


import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * Tests for HashSet which comes with JDBM. Original code comes from Apache Harmony,
 * Modified by Jan Kotek for use in JDBM
 */
@SuppressWarnings({"unchecked","rawtypes"})
public class HTreeSetTest{

    Engine engine = new StoreDirect(Volume.memoryFactory(false));

    Set hs;

    static Object[] objArray;

    static {
        objArray = new Object[1000];
        for (int i = 0; i < objArray.length; i++)
            objArray[i] = i;
    }

    @Test public void test_Constructor() {
        // Test for method java.util.HashSet()
        Set hs2 = new HTreeMap(engine, false,false,0,null,null,null).keySet();
        assertEquals("Created incorrect HashSet", 0, hs2.size());
    }


    @Test public void test_addLjava_lang_Object() {
        // Test for method boolean java.util.HashSet.add(java.lang.Object)
        int size = hs.size();
        hs.add(8);
        assertTrue("Added element already contained by set", hs.size() == size);
        hs.add(-9);
        assertTrue("Failed to increment set size after add",
                hs.size() == size + 1);
        assertTrue("Failed to add element to set", hs.contains(new Integer(-9)));
    }

    @Test public void test_clear() {
        // Test for method void java.util.HashSet.clear()
        Set orgSet = new java.util.HashSet(hs);
        hs.clear();
        Iterator i = orgSet.iterator();
        assertEquals("Returned non-zero size after clear", 0, hs.size());
        while (i.hasNext())
            assertTrue("Failed to clear set", !hs.contains(i.next()));
    }


    @Test public void test_containsLjava_lang_Object() {
        // Test for method boolean java.util.HashSet.contains(java.lang.Object)
        assertTrue("Returned false for valid object", hs.contains(objArray[90]));
        assertTrue("Returned true for invalid Object", !hs
                .contains(-111111));

    }

    @Test public void test_isEmpty() {
        // Test for method boolean java.util.HashSet.isEmpty()
        assertTrue("Empty set returned false", new HTreeMap(engine, false,false,0,null,null,null).keySet().isEmpty());
        assertTrue("Non-empty set returned true", !hs.isEmpty());
    }

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

    @Test public void test_removeLjava_lang_Object() {
        // Test for method boolean java.util.HashSet.remove(java.lang.Object)
        int size = hs.size();
        hs.remove(new Integer(98));
        assertTrue("Failed to remove element", !hs.contains(new Integer(98)));
        assertTrue("Failed to decrement set size", hs.size() == size - 1);

    }

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
        hs = new HTreeMap(engine, false,false,0,null,null,null).keySet();
        Collections.addAll(hs, objArray);
    }

    @Test public void issue116_isEmpty(){
        Set s = DBMaker.newFileDB(Utils.tempDbFile())
                .writeAheadLogDisable()
                .make()
                .getHashSet("name");
        assertTrue(s.isEmpty());
        assertEquals(0,s.size());
        s.add("aa");
        assertEquals(1,s.size());
        assertFalse(s.isEmpty());
        s.remove("aa");
        assertTrue(s.isEmpty());
        assertEquals(0,s.size());
    }

}
