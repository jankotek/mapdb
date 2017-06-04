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

package org.mapdb.tree;

import org.mapdb.DBMaker;

import java.util.Iterator;
import java.util.Set;

public class HTreeSet_Harmony_Test extends junit.framework.TestCase {

	Set hs;

	static Object[] objArray;
	{
		objArray = new Object[1000];
		for (int i = 0; i < objArray.length; i++)
			objArray[i] = new Integer(i);
	}



	/**
	 * @tests java.util.Set#Set()
	 */
	public void test_Constructor() {
		// Test for method java.util.Set()
		Set hs2 = newSet();
		assertEquals("Created incorrect Set", 0, hs2.size());
	}

	protected Set newSet() {
		return DBMaker.memoryDB().make().hashSet("a").create();
	}



	/**
	 * @tests java.util.Set#add(java.lang.Object)
	 */
	public void test_addLjava_lang_Object() {
		// Test for method boolean java.util.Set.add(java.lang.Object)
		int size = hs.size();
		hs.add(new Integer(8));
		assertTrue("Added element already contained by set", hs.size() == size);
		hs.add(new Integer(-9));
		assertTrue("Failed to increment set size after add",
				hs.size() == size + 1);
		assertTrue("Failed to add element to set", hs.contains(new Integer(-9)));
	}

	/**
	 * @tests java.util.Set#clear()
	 */
	public void test_clear() {
		// Test for method void java.util.Set.clear()
		hs.clear();
		assertEquals("Returned non-zero size after clear", 0, hs.size());
	}
//
//	/**
//	 * @tests java.util.Set#clone()
//	 */
	//TODO hashSet clone and serialization
//	public void test_clone() {
//		// Test for method java.lang.Object java.util.Set.clone()
//		Set hs2 = (Set) hs.clone();
//		assertTrue("clone returned an equivalent Set", hs != hs2);
//		assertTrue("clone did not return an equal Set", hs.equals(hs2));
//	}

	/**
	 * @tests java.util.Set#contains(java.lang.Object)
	 */
	public void test_containsLjava_lang_Object() {
		// Test for method boolean java.util.Set.contains(java.lang.Object)
		assertTrue("Returned false for valid object", hs.contains(objArray[90]));
		assertTrue("Returned true for invalid Object", !hs
				.contains(new Object()));

		Set s = newSet();
//		s.add(null);
//		assertTrue("Cannot handle null", s.contains(null));
	}

	/**
	 * @tests java.util.Set#isEmpty()
	 */
	public void test_isEmpty() {
		// Test for method boolean java.util.Set.isEmpty()
		assertTrue("Empty set returned false", newSet().isEmpty());
		assertTrue("Non-empty set returned true", !hs.isEmpty());
	}

	/**
	 * @tests java.util.Set#iterator()
	 */
	public void test_iterator() {
		//TODO long test
		if(1==1) return;
		// Test for method java.util.Iterator java.util.Set.iterator()
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
	 * @tests java.util.Set#remove(java.lang.Object)
	 */
	public void test_removeLjava_lang_Object() {
		// Test for method boolean java.util.Set.remove(java.lang.Object)
		int size = hs.size();
		hs.remove(new Integer(98));
		assertTrue("Failed to remove element", !hs.contains(new Integer(98)));
		assertTrue("Failed to decrement set size", hs.size() == size - 1);
	}

	/**
	 * @tests java.util.Set#size()
	 */
	public void test_size() {
		// Test for method int java.util.Set.size()
		assertTrue("Returned incorrect size", hs.size() == (objArray.length));
		hs.clear();
		assertEquals("Cleared set returned non-zero size", 0, hs.size());
	}
	
//    /**
//     * @tests java.util.AbstractCollection#toString()
//     */
//    public void test_toString() {
//        Set s = newSet();
//        s.add(s);
//        String result = s.toString();
//        assertTrue("should contain self ref", result.indexOf("(this") > -1);
//    }


	/**
	 * Sets up the fixture, for example, open a network connection. This method
	 * is called before a test is executed.
	 */
	protected void setUp() {
		hs = newSet();
		for (int i = 0; i < objArray.length; i++)
			hs.add(objArray[i]);
	}

	/**
	 * Tears down the fixture, for example, close a network connection. This
	 * method is called after a test is executed.
	 */
	protected void tearDown() {
	}

}
