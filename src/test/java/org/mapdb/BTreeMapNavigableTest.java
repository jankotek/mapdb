/*
* Copyright 2012 Luc Peuvrier
* All rights reserved.
*
* This file is a part of JOAFIP.
*
* JOAFIP is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License.
*
* Licensed under the GNU LESSER GENERAL PUBLIC LICENSE
* Licensed under the LGPL License, Version 3, 29 June 2007 (the "LGPL License");
* you may not use this file except in compliance with the "LGPL License" extended with here below additional permissions.
* You may obtain a copy of the "LGPL License" at
*
*    http://www.gnu.org/licenses/lgpl.html
*
* Additional permissions extensions for this file:
*
* Redistribution and use in source and binary forms, with or without modification,
* are permitted under the the Apache License, Version 2.0 (the "Apache License") instead of the "LGPL License"
* and if following conditions are met:
*
* 1. Redistributions of source code must retain the above copyright
* notice, this list of conditions and the following disclaimer.
*
* 2. Redistributions in binary form must reproduce the above copyright
* notice, this list of conditions and the following disclaimer in the
* documentation and/or other materials provided with the distribution.
*
* 3. Neither the name of the copyright holders nor the names of its
* contributors may be used to endorse or promote products derived from
* this software without specific prior written permission.
*
* You may obtain a copy of the "Apache License" at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* JOAFIP is distributed in the hope that it will be useful, but
* unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.mapdb;

import junit.framework.TestCase;

import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.SortedMap;

/**
 * to test {@link java.util.NavigableMap} implementation
 * 
 * @author luc peuvrier
 * 
 */
public class BTreeMapNavigableTest extends TestCase {

	private static final String MUST_NOT_CONTAINS_KD = "must not contains 'kd'";

	private static final String MUST_NOT_CONTAINS_KA = "must not contains 'ka'";

	private static final String BAD_FIRST_ENTRY_KEY = "bad first entry key";

	private static final String MUST_NOT_BE_EMPTY = "must not be empty";

	private static final String BAD_SIZE = "bad size";

	private static final String MUST_CONTAINS_KC = "must contains 'kc'";

	private static final String MUST_CONTAINS_KB = "must contains 'kb'";

	private static final String MUST_CONTAINS_KA = "must contains 'ka'";

	private NavigableMap<String, String> navigableMap =
        DBMaker.newMemoryDB().make().getTreeMap("test");
    


	public void testLowerEntry() {
		navigableMap.put("ka", "xx");
		navigableMap.put("kb", "aa");
		navigableMap.put("kc", "zz");
		final Entry<String, String> lowerEntry = navigableMap.lowerEntry("kb");
		assertEquals("bad lower entry value", "xx", lowerEntry.getValue());
		assertEquals("bad lower entry key", "ka", lowerEntry.getKey());
	}

	public void testLowerKey() {
		navigableMap.put("ka", "xx");
		navigableMap.put("kb", "aa");
		navigableMap.put("kc", "zz");
		assertEquals("bad lower key", "ka", navigableMap.lowerKey("kb"));
	}

	public void testFloorEntry() {
		navigableMap.put("ka", "xx");
		navigableMap.put("kc", "aa");
		navigableMap.put("kd", "zz");
		Entry<String, String> floorEntry = navigableMap.floorEntry("ka");
		assertEquals("bad floor entry value", "xx", floorEntry.getValue());
		assertEquals("bad floor entry key", "ka", floorEntry.getKey());
		floorEntry = navigableMap.floorEntry("kb");
		assertEquals("bad floor entry value", "xx", floorEntry.getValue());
		assertEquals("bad floor entry key", "ka", floorEntry.getKey());
	}

	public void testFloorKey() {
		navigableMap.put("ka", "xx");
		navigableMap.put("kc", "aa");
		navigableMap.put("kd", "zz");
		assertEquals("bad floor key", "ka", navigableMap.floorKey("ka"));
		assertEquals("bad floor key", "ka", navigableMap.floorKey("kb"));
	}

	public void testCeilingEntry() {
		navigableMap.put("ka", "xx");
		navigableMap.put("kb", "aa");
		navigableMap.put("kd", "zz");
		Entry<String, String> ceilingEntry = navigableMap.ceilingEntry("kd");
		assertEquals("bad ceiling entry value", "zz", ceilingEntry.getValue());
		assertEquals("bad ceiling entry key", "kd", ceilingEntry.getKey());
		ceilingEntry = navigableMap.ceilingEntry("kc");
		assertEquals("bad ceiling entry value", "zz", ceilingEntry.getValue());
		assertEquals("bad ceiling entry key", "kd", ceilingEntry.getKey());
	}

	public void testCeilingKey() {
		navigableMap.put("ka", "xx");
		navigableMap.put("kb", "aa");
		navigableMap.put("kd", "zz");
		assertEquals("bad ceiling key", "kd", navigableMap.ceilingKey("kd"));
		assertEquals("bad ceiling key", "kd", navigableMap.ceilingKey("kc"));
	}

	public void testHigherEntry() {
		navigableMap.put("ka", "xx");
		navigableMap.put("kb", "aa");
		navigableMap.put("kc", "zz");
		final Entry<String, String> higherEntry = navigableMap
				.higherEntry("kb");
		assertEquals("bad higher entry value", "zz", higherEntry.getValue());
		assertEquals("bad higher entry key", "kc", higherEntry.getKey());
	}

	public void testHigherKey() {
		navigableMap.put("ka", "xx");
		navigableMap.put("kb", "aa");
		navigableMap.put("kc", "zz");
		assertEquals("bad higher key", "kc", navigableMap.higherKey("kb"));
	}

	public void testFirstEntry() {
		navigableMap.put("ka", "xx");
		navigableMap.put("kb", "aa");
		navigableMap.put("kc", "zz");
		final Entry<String, String> firstEntry = navigableMap.firstEntry();
		assertEquals("bad first entry value", "xx", firstEntry.getValue());
		assertEquals(BAD_FIRST_ENTRY_KEY, "ka", firstEntry.getKey());
	}

	public void testLastEntry() {
		navigableMap.put("ka", "xx");
		navigableMap.put("kb", "aa");
		navigableMap.put("kc", "zz");
		final Entry<String, String> lastEntry = navigableMap.lastEntry();
		assertEquals("bad last entry value", "zz", lastEntry.getValue());
		assertEquals("bad last entry key", "kc", lastEntry.getKey());
	}

	public void testPollFirstEntry() {
		assertNull("must not have first entry", navigableMap.pollFirstEntry());
		navigableMap.put("ka", "xx");
		navigableMap.put("kb", "aa");
		navigableMap.put("kc", "zz");
		assertEquals("must have 3 entries", 3, navigableMap.size());
		final Entry<String, String> firstEntry = navigableMap.pollFirstEntry();
		assertNotNull("must have first entry", firstEntry);
		assertEquals("bad first entry value", "xx", firstEntry.getValue());
		assertEquals(BAD_FIRST_ENTRY_KEY, "ka", firstEntry.getKey());
		assertEquals("must have 2 entries", 2, navigableMap.size());
	}

	public void testPollLastEntry() {
		assertNull("must not have last entry", navigableMap.pollLastEntry());
		navigableMap.put("ka", "xx");
		navigableMap.put("kb", "aa");
		navigableMap.put("kc", "zz");
		assertEquals("must have 3 entries", 3, navigableMap.size());
		final Entry<String, String> lastEntry = navigableMap.pollLastEntry();
		assertNotNull("must have last entry", lastEntry);
		assertEquals("bad last entry value", "zz", lastEntry.getValue());
		assertEquals("bad last entry key", "kc", lastEntry.getKey());
		assertEquals("must have 2 entries", 2, navigableMap.size());
	}

//  TODO implement this
//
//	public void testDescendingMap() {
//		navigableMap.put("ka", "xx");
//		navigableMap.put("kb", "aa");
//		navigableMap.put("kc", "zz");
//		final NavigableMap<String, String> descendingMap = navigableMap
//				.descendingMap();
//
//		assertEquals(BAD_SIZE, 3, descendingMap.size());
//		assertFalse(MUST_NOT_BE_EMPTY, descendingMap.isEmpty());
//
//		final Entry<String, String> firstEntry = descendingMap.firstEntry();
//		assertEquals("bad first entry value", "zz", firstEntry.getValue());
//		assertEquals(BAD_FIRST_ENTRY_KEY, "kc", firstEntry.getKey());
//
//		final Entry<String, String> lastEntry = descendingMap.lastEntry();
//		assertEquals("bad last entry value", "xx", lastEntry.getValue());
//		assertEquals("bad last entry key", "ka", lastEntry.getKey());
//
//		final Set<Entry<String, String>> entrySet = descendingMap.entrySet();
//		final Iterator<Entry<String, String>> iterator = entrySet.iterator();
//		assertTrue("must have first entry", iterator.hasNext());
//		assertEquals(BAD_FIRST_ENTRY_KEY, "kc", iterator.next().getKey());
//		assertTrue("must have second entry", iterator.hasNext());
//		assertEquals("bad second entry key", "kb", iterator.next().getKey());
//		assertTrue("must have third entry", iterator.hasNext());
//		assertEquals("bad third entry key", "ka", iterator.next().getKey());
//		assertFalse("must not have fourth entry", iterator.hasNext());
//
//		descendingMap.remove("kb");
//		assertEquals(BAD_SIZE, 2, descendingMap.size());
//		assertFalse(MUST_NOT_BE_EMPTY, descendingMap.isEmpty());
//
//		assertEquals(BAD_SIZE, 2, navigableMap.size());
//		assertFalse(MUST_NOT_BE_EMPTY, navigableMap.isEmpty());
//		assertTrue("must contains key 'ka'", navigableMap.containsKey("ka"));
//		assertFalse("must not contains key 'kb'", navigableMap
//				.containsKey("kb"));
//		assertTrue("must contains key 'kc'", navigableMap.containsKey("kc"));
//	}

	public void testNavigableKeySet() {
		navigableMap.put("ka", "xx");
		navigableMap.put("kb", "aa");
		navigableMap.put("kc", "zz");
		final NavigableSet<String> navigableSet = navigableMap
				.navigableKeySet();
		assertEquals("bad first element", "ka", navigableSet.first());
		assertEquals("bad last element", "kc", navigableSet.last());
		assertTrue(MUST_CONTAINS_KA, navigableSet.contains("ka"));
		assertTrue(MUST_CONTAINS_KB, navigableSet.contains("kb"));
		assertTrue(MUST_CONTAINS_KC, navigableSet.contains("kc"));

		navigableSet.remove("kb");
		assertEquals(BAD_SIZE, 2, navigableMap.size());
		assertFalse(MUST_NOT_BE_EMPTY, navigableMap.isEmpty());
		assertTrue("must contains key 'ka'", navigableMap.containsKey("ka"));
		assertFalse("must not contains key 'kb'", navigableMap
				.containsKey("kb"));
		assertTrue("must contains key 'kc'", navigableMap.containsKey("kc"));
	}
// TODO implement this
//	public void testDescendingKeySet() {
//		navigableMap.put("ka", "xx");
//		navigableMap.put("kb", "aa");
//		navigableMap.put("kc", "zz");
//		final NavigableSet<String> navigableSet = navigableMap
//				.descendingKeySet();
//		assertEquals("bad first element", "kc", navigableSet.first());
//		assertEquals("bad last element", "ka", navigableSet.last());
//		assertTrue(MUST_CONTAINS_KA, navigableSet.contains("ka"));
//		assertTrue(MUST_CONTAINS_KB, navigableSet.contains("kb"));
//		assertTrue(MUST_CONTAINS_KC, navigableSet.contains("kc"));
//
//		navigableSet.remove("kb");
//		assertEquals(BAD_SIZE, 2, navigableMap.size());
//		assertFalse(MUST_NOT_BE_EMPTY, navigableMap.isEmpty());
//		assertTrue("must contains key 'ka'", navigableMap.containsKey("ka"));
//		assertFalse("must not contains key 'kb'", navigableMap
//				.containsKey("kb"));
//		assertTrue("must contains key 'kc'", navigableMap.containsKey("kc"));
//	}

	public void testSubMap() {
		navigableMap.put("ka", "xx");
		navigableMap.put("kb", "aa");
		navigableMap.put("kc", "zz");
		navigableMap.put("kd", "uu");

		SortedMap<String, String> sortedMap = navigableMap.subMap("kb", "kd");
		assertFalse(MUST_NOT_CONTAINS_KA, sortedMap.containsKey("ka"));
		assertTrue(MUST_CONTAINS_KB, sortedMap.containsKey("kb"));
		assertTrue(MUST_CONTAINS_KC, sortedMap.containsKey("kc"));
		assertFalse(MUST_NOT_CONTAINS_KD, sortedMap.containsKey("kd"));

		sortedMap = navigableMap.subMap("ka", false, "kc", true);
		assertFalse(MUST_NOT_CONTAINS_KA, sortedMap.containsKey("ka"));
		assertTrue(MUST_CONTAINS_KB, sortedMap.containsKey("kb"));
		assertTrue(MUST_CONTAINS_KC, sortedMap.containsKey("kc"));
		assertFalse(MUST_NOT_CONTAINS_KD, sortedMap.containsKey("kd"));
	}

	public void testHeadMap() {
		navigableMap.put("ka", "xx");
		navigableMap.put("kb", "aa");
		navigableMap.put("kc", "zz");
		navigableMap.put("kd", "uu");

		SortedMap<String, String> sortedMap = navigableMap.headMap("kc");
		assertTrue(MUST_CONTAINS_KA, sortedMap.containsKey("ka"));
		assertTrue(MUST_CONTAINS_KB, sortedMap.containsKey("kb"));
		assertFalse("must not contains 'kc'", sortedMap.containsKey("kc"));
		assertFalse(MUST_NOT_CONTAINS_KD, sortedMap.containsKey("kd"));

		sortedMap = navigableMap.headMap("kb", true);
		assertTrue(MUST_CONTAINS_KA, sortedMap.containsKey("ka"));
		assertTrue(MUST_CONTAINS_KB, sortedMap.containsKey("kb"));
		assertFalse("must not contains 'kc'", sortedMap.containsKey("kc"));
		assertFalse(MUST_NOT_CONTAINS_KD, sortedMap.containsKey("kd"));
	}

	public void testTailMap() {
		navigableMap.put("ka", "xx");
		navigableMap.put("kb", "aa");
		navigableMap.put("kc", "zz");
		navigableMap.put("kd", "uu");

		SortedMap<String, String> sortedMap = navigableMap.tailMap("kc");
		assertFalse(MUST_NOT_CONTAINS_KA, sortedMap.containsKey("ka"));
		assertFalse("must not contains 'kb'", sortedMap.containsKey("kb"));
		assertTrue(MUST_CONTAINS_KC, sortedMap.containsKey("kc"));
		assertTrue("must contains 'kd'", sortedMap.containsKey("kd"));

		sortedMap = navigableMap.tailMap("kb", false);
		assertFalse(MUST_NOT_CONTAINS_KA, sortedMap.containsKey("ka"));
		assertFalse("must not contains 'kb'", sortedMap.containsKey("kb"));
		assertTrue(MUST_CONTAINS_KC, sortedMap.containsKey("kc"));
		assertTrue("must contains 'kd'", sortedMap.containsKey("kd"));
	}
}
