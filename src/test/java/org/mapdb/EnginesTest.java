package org.mapdb;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;
/**
 * Tests contract of various implementations of Engine interface
 */
@RunWith(Parameterized.class)
public class EnginesTest{
    final Engine e;

    public EnginesTest(Engine e) {
        this.e = e;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {DBMaker.newMemoryDB().makeEngine()},
                {DBMaker.newMemoryDB().journalDisable().makeEngine()}
        });
    }

    @Test
    public void casNulls(){
        long recid = e.put(null, Serializer.BASIC_SERIALIZER);

        assertNull(e.get(recid,Serializer.BASIC_SERIALIZER));
        assertFalse(e.compareAndSwap(recid, "aa", null, Serializer.BASIC_SERIALIZER));
        assertNull(e.get(recid, Serializer.BASIC_SERIALIZER));
        assertTrue(e.compareAndSwap(recid, null, "aa", Serializer.BASIC_SERIALIZER));
        assertEquals("aa", e.get(recid, Serializer.BASIC_SERIALIZER));
        assertFalse(e.compareAndSwap(recid, null, "bb", Serializer.BASIC_SERIALIZER));
        assertEquals("aa", e.get(recid, Serializer.BASIC_SERIALIZER));
        assertTrue(e.compareAndSwap(recid, "aa", null, Serializer.BASIC_SERIALIZER));
        assertNull(e.get(recid, Serializer.BASIC_SERIALIZER));
    }

}
