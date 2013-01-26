package org.mapdb;

import junit.framework.TestCase;

import java.lang.reflect.Field;
import java.util.Set;
import java.util.TreeSet;


public class SerializationHeaderTest extends TestCase {

	@SuppressWarnings({  "rawtypes" })
    public void testUnique() throws IllegalAccessException {
        Class c = SerializationHeader.class;
        Set<Integer> s = new TreeSet<Integer>();
        for (Field f : c.getDeclaredFields()) {
            f.setAccessible(true);
            int value = f.getInt(null);

            assertTrue("Value already used: " + value, !s.contains(value));
            s.add(value);
        }
        assertTrue(!s.isEmpty());
    }
}
