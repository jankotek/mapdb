package org.mapdb;


import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class StoreDirect2_BaseTest {

    protected abstract StoreDirect2 openStore();

    @Test
    public void long_stack_putGet_all_sizes(){
        for(long masterLinkOffset = StoreDirect2.O_STACK_FREE_RECID;masterLinkOffset<StoreDirect2.HEADER_SIZE;masterLinkOffset+=8) {
            StoreDirect2 s = openStore();
            s.init();
            s.structuralLock.lock();
            assertTrue(masterLinkOffset < StoreDirect2.HEADER_SIZE);
            s.longStackPut(masterLinkOffset, 1111L);
            assertEquals(1111L, s.longStackTake(masterLinkOffset));
            s.close();
        }
    }

}
