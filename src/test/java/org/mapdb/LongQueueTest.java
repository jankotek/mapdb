package org.mapdb;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LongQueueTest {

    Store.LongQueue m = new Store.LongQueue();

    @Test
    public void basic() {
        assertTrue(m.put(11));
        assertTrue(m.put(12));
        for (long i = 11; i < 100000; i++) {
            assertTrue(m.put(i + 2));
            assertEquals(i, m.take());
        }
    }

    @Test
    public void empty() {
        assertEquals(Long.MIN_VALUE, m.take());

        assertTrue(m.put(11));
        assertTrue(m.put(12));
        assertEquals(11L, m.take());
        assertEquals(12L, m.take());

        assertEquals(Long.MIN_VALUE, m.take());
    }

    @Test
    public void fill_drain() {
        for(int i=0;i<m.size - Store.LongQueue.MAX_PACKED_LEN;i++){
            assertTrue(m.put(1L));
        }
        assertFalse(m.put(1L));
        assertEquals(1L, m.take());
        assertTrue(m.put(1L));
        assertFalse(m.put(1L));

        for(int i=0;i<m.size- Store.LongQueue.MAX_PACKED_LEN;i++){
            assertEquals(1L, m.take());
        }
        assertEquals(Long.MIN_VALUE, m.take());

        assertEquals(m.start,m.end);


    }
}