package org.mapdb;

import junit.framework.TestCase;

import java.util.AbstractMap;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.SortedMap;

@SuppressWarnings({ "unchecked" })
public  class BTreeMapNavigable2Test extends TestCase
{
	NavigableMap<Integer, String> map = DBMaker.newTempTreeMap();


	@Override
	public void setUp() throws Exception
	{
		map.put(1, "one");
		map.put(2, "two");
		map.put(3, "three");
		map.put(4, "four");
		
		map.put(7, "seven");
		map.put(8, "eight");
		map.put(9, "nine");
		map.put(10, "ten");
	}


	public void testSize()
	{
		int i = 8;
		assertEquals(map.size(), i);
		while (!map.isEmpty())
		{
			map.remove(map.firstKey());
			assertEquals(map.size(), --i);
		}
	}
	
	
	public void testContainsKey()
	{
		assertTrue(map.containsKey(1));
		assertTrue(map.containsKey(2));
		assertTrue(map.containsKey(3));
		assertTrue(map.containsKey(4));
		assertFalse(map.containsKey(5));
		assertFalse(map.containsKey(6));
		assertTrue(map.containsKey(7));
		assertTrue(map.containsKey(8));
		assertTrue(map.containsKey(9));
		assertTrue(map.containsKey(10));
		
		assertFalse(map.containsKey(999));
		assertFalse(map.containsKey(-1));
	}
	
	
	public void testContainsValue()
	{
		assertTrue(map.containsValue("one"));
		assertTrue(map.containsValue("two"));
		assertTrue(map.containsValue("three"));
		assertTrue(map.containsValue("four"));
		assertFalse(map.containsValue("five"));
		assertFalse(map.containsValue("six"));
		assertTrue(map.containsValue("seven"));
		assertTrue(map.containsValue("eight"));
		assertTrue(map.containsValue("nine"));
		assertTrue(map.containsValue("ten"));
		
		assertFalse(map.containsValue("aaaa"));
	}
	
	
	public void testPut()
	{
		assertFalse(map.containsKey(40));
		assertFalse(map.containsValue("forty"));
		map.put(40, "forty");
		assertTrue(map.containsKey(40));
		assertTrue(map.containsValue("forty"));
	}
	
	
	public void testLowerEntry()
	{
		AbstractMap.Entry<Integer,String> e = map.lowerEntry(4);
		assertEquals(e.getKey(), (Integer)3);
	}
	
	
	public void testLowerKey()
	{
		Integer key = map.lowerKey(4);
		assertEquals(key, (Integer)3);
	}
	
	
	public void testFloorEntry()
	{
		AbstractMap.Entry<Integer, String> e = map.floorEntry(6);
		assertEquals(e.getKey(), (Integer)4);
				
		e = map.floorEntry(7);
		assertEquals(e.getKey(), (Integer)7);
	}
	
	
	public void testFloorKey()
	{
		Integer key = map.floorKey(6);
		assertEquals(key, (Integer)4);
		
		key = map.floorKey(7);
		assertEquals(key, (Integer)7);
	}
	
	
	public void testCeilingEntry()
	{
		AbstractMap.Entry<Integer, String> e = map.ceilingEntry(6);
		assertEquals(e.getKey(), (Integer)7);
		
		e = map.ceilingEntry(7);
		assertEquals(e.getKey(), (Integer)7);
	}
	
	
	public void testCeilingKey()
	{
		Integer key = map.ceilingKey(6);
		assertEquals(key, (Integer)7);
		
		key = map.ceilingKey(7);
		assertEquals(key, (Integer)7);
	}
	
	
	public void testHigherEntry()
	{
		AbstractMap.Entry<Integer, String> e = map.higherEntry(4);
		assertEquals(e.getKey(), (Integer)7);
		
		e = map.higherEntry(7);
		assertEquals(e.getKey(), (Integer)8);
	}
	
	
	public void testHigherKey()
	{
		Integer key = map.higherKey(4);
		assertEquals(key, (Integer)7);
		
		key = map.higherKey(7);
		assertEquals(key, (Integer)8);
	}
	
	
	public void testFirstEntry()
	{
		assertEquals(
			map.firstEntry().getKey(),
			(Integer) 1);
	}
	
	
	public void testLastEntry()
	{
		assertEquals(
		    map.lastEntry().getKey(),
		    (Integer) 10);
	}
	
	
	public void testPollFirstEntry()
	{
		int size0 = map.size();
		AbstractMap.Entry<Integer, String> e = map.pollFirstEntry();
		int size1 = map.size();
		assertEquals(size0-1, size1);
		
		assertNull(map.get(1));
		assertEquals(e.getKey(), (Integer)1);
		assertEquals(e.getValue(), "one");
	}
	
	
	public void testPollLastEntry()
	{
		int size0 = map.size();
		AbstractMap.Entry<Integer, String> e = map.pollLastEntry();
		int size1 = map.size();
		assertEquals(size0-1, size1);
		
		assertNull(map.get(10));
		assertEquals(e.getKey(), (Integer)10);
		assertEquals(e.getValue(), "ten");
	}


//	public void testDescendingMap()
//	{
//
//        //TODO desc
//		NavigableMap<Integer, String> desMap = map.descendingMap();
//		Set<AbstractMap.Entry<Integer,String>> entrySet1 = map.entrySet();
//		Set<AbstractMap.Entry<Integer,String>> entrySet2 = desMap.entrySet();
//		AbstractMap.Entry<Integer,String>[] arr1 = entrySet1.toArray(new AbstractMap.Entry[0]);
//		AbstractMap.Entry<Integer,String>[] arr2 = entrySet2.toArray(new AbstractMap.Entry[0]);
//
//		int size = arr1.length;
//		assertEquals(arr1.length, arr2.length);
//		for (int i = 0; i < arr1.length; i++)
//		{
//			assertEquals(arr1[i], arr2[size-1-i]);
//		}
//	}
	
	
	public void testNavigableKeySet()
	{
		int size0 = map.size();
		NavigableSet<Integer> keySet = map.navigableKeySet();
		int size1 = keySet.size();
		assertEquals(size0, size1);
		
		keySet.remove(2);
		size0 = map.size();
		size1 = keySet.size();
		assertEquals(size0, size1);
		assertNull(map.get(2));
	}


//	public void testDescendingKeySet()
//	{
//        //TODO desc
//		Set<Integer> keySet1 = map.keySet();
//		Set<Integer> keySet2 = map.descendingKeySet();
//
//		Integer[] arr1 = keySet1.toArray(new Integer[0]);
//		Integer[] arr2 = keySet2.toArray(new Integer[0]);
//		int size = arr1.length;
//		assertEquals(arr1.length, arr2.length);
//		for (int i = 0; i < size; i++)
//		{
//			assertEquals(arr1[i],arr2[size-1-i]);
//		}
//	}
	
	
	public void testSubMap()
	{
		SortedMap<Integer,String> subMap = map.subMap(3, 8);
		assertNotNull(subMap.get(3));
		assertEquals(subMap.get(3), "three");
		assertEquals(subMap.get(4), "four");
		assertNull(subMap.get(5));
		assertNull(subMap.get(6));
		assertEquals(subMap.get(7), "seven");
		
		assertNull(subMap.get(8));
		assertNull(subMap.get(2));
		assertNull(subMap.get(9));
		try 
		{
			subMap.put(11,"eleven");
			fail("Inserted entry outside of submap range");
		}
		catch (IllegalArgumentException e)
		{
			assertNull(subMap.get(11));
		}
	}
	
	
	public void testSubMap2()
	{
		NavigableMap<Integer, String> subMap = map.subMap(3,true,8,false);
		assertNotNull(subMap.get(3));
		assertEquals(subMap.get(3), "three");
		assertEquals(subMap.get(4), "four");
		assertNull(subMap.get(5));
		assertNull(subMap.get(6));
		assertEquals(subMap.get(7), "seven");
		
		assertNull(subMap.get(8));
		assertNull(subMap.get(2));
		assertNull(subMap.get(9));
		try 
		{
			subMap.put(11,"eleven");
			fail("Inserted entry outside of submap range");
		}
		catch (IllegalArgumentException e)
		{
			assertNull(subMap.get(11));
		}
	}
	
	
	public void testSubMap3()
	{
		NavigableMap<Integer, String> subMap = map.subMap(2, false, 8, false);
		assertNotNull(subMap.get(3));
		assertEquals(subMap.get(3), "three");
		assertEquals(subMap.get(4), "four");
		assertNull(subMap.get(5));
		assertNull(subMap.get(6));
		assertEquals(subMap.get(7), "seven");
		
		assertNull(subMap.get(8));
		assertNull(subMap.get(2));
		assertNull(subMap.get(9));
		try 
		{
			subMap.put(11,"eleven");
			fail("Inserted entry outside of submap range");
		}
		catch (IllegalArgumentException e)
		{
			assertNull(subMap.get(11));
		}
	}
	
	
	public void testSubMap4()
	{
		NavigableMap<Integer, String> subMap = map.subMap(3, true, 7, true);
		assertNotNull(subMap.get(3));
		assertEquals(subMap.get(3), "three");
		assertEquals(subMap.get(4), "four");
		assertNull(subMap.get(5));
		assertNull(subMap.get(6));
		assertEquals(subMap.get(7), "seven");
		
		assertNull(subMap.get(8));
		assertNull(subMap.get(2));
		assertNull(subMap.get(9));
		try 
		{
			subMap.put(11,"eleven");
			fail("Inserted entry outside of submap range");
		}
		catch (IllegalArgumentException e)
		{
			assertNull(subMap.get(11));
		}
	}
	
	
	public void testHeadMap()
	{
		SortedMap<Integer, String> subMap = map.headMap(5);
		assertEquals(subMap.size(), 4);
		assertNull(subMap.get(5));
		assertEquals(subMap.get(1), "one");
		try
		{
			subMap.put(5, "five");
			fail("Inseted data out of bounds of submap.");
		}
		catch (IllegalArgumentException e)
		{
			assertNull(subMap.get(5));
		}
	}
	
	
	public void testHeadMap2()
	{
		NavigableMap<Integer,String> subMap = map.headMap(5, false);
		assertEquals(subMap.size(), 4);
		assertNull(subMap.get(5));
		assertEquals(subMap.get(1), "one");
		try
		{
			subMap.put(5, "five");
			fail("Inseted data out of bounds of submap.");
		}
		catch (IllegalArgumentException e)
		{
			assertNull(subMap.get(5));
		}
	}
	
	
	public void testHeadMap3()
	{
		NavigableMap<Integer,String> subMap = map.headMap(5, true);
		assertEquals(subMap.size(), 4);
		assertNull(subMap.get(5));
		assertEquals(subMap.get(1), "one");
		try
		{
			subMap.put(5, "five");
			assertEquals(subMap.get(5), "five");
		}
		catch (IllegalArgumentException e)
		{
			fail("It was not possible to insert a legal value in a submap.");
		}
	}
	
	
	public void testHeadMap4()
	{
		NavigableMap<Integer,String> subMap = map.headMap(8, true);
		assertEquals(subMap.size(), 6);
		assertEquals(subMap.get(8), "eight");
		assertEquals(subMap.get(1), "one");
		try
		{
			subMap.put(5, "five");
			assertEquals(subMap.get(5), "five");
		}
		catch (IllegalArgumentException e)
		{
			fail("It was not possible to insert a legal value in a submap.");
		}
	}
	
	
	public void testTailMap()
	{
		SortedMap<Integer, String> subMap = map.tailMap(5);
		assertEquals(subMap.size(), 4);
		assertEquals(subMap.firstKey(), (Integer)7);
		assertEquals(subMap.lastKey(), (Integer)10);
	}
	
	
	public void testTailMap2()
	{
		SortedMap<Integer, String> subMap = map.tailMap(7);
		assertEquals(subMap.size(),  4);
		assertEquals(subMap.firstKey(), (Integer)7);
		assertEquals(subMap.lastKey(), (Integer)10);
	}

	
	public void testTailMap3()
	{
		NavigableMap<Integer, String> subMap = map.tailMap(7, false);
		assertEquals(subMap.size(),  3);
		assertEquals(subMap.firstKey(), (Integer)8);
		assertEquals(subMap.lastKey(), (Integer)10);
	}

	
	public void testTailMap4()
	{
		NavigableMap<Integer, String> subMap = map.tailMap(7, true);
		assertEquals(subMap.size(),  4);
		assertEquals(subMap.firstKey(), (Integer)7);
		assertEquals(subMap.lastKey(), (Integer)10);
	}
	
	
	public void testIsEmpty()
	{
		assertFalse(map.isEmpty());
		map.clear();
		assertTrue(map.isEmpty());
	}
	
	
	public void testClearSubmap()
	{
		NavigableMap<Integer, String> subMap = map.subMap(7, true, 9, true);
		subMap.clear();
		assertEquals(subMap.size(), 0);
		assertTrue(map.size()==5);
		assertNull(map.get(7));
		assertNull(map.get(8));
		assertNull(map.get(9));
	}
	
//
//	public void testConcurrentModification()
//	{
//		Set<AbstractMap.Entry<Integer, String>> entrySet = map.entrySet();
//		assertTrue(entrySet.size() > 0);
//        try
//        {
//
//		    for (AbstractMap.Entry<Integer, String> e : entrySet)
//			    entrySet.remove(e);
//
//            fail("No concurrentModificationException was thrown");
//        }
//        catch (ConcurrentModificationException ex){}
//
//
//	}



}
