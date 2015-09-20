package org.mapdb;


import org.junit.Test;

import static org.junit.Assert.*;

public class StoreWAL_LongStack_Test {

    final long masterLinkOffset = StoreDirect2.O_STACK_FREE_RECID;

    @Test
    public void commit_rollback(){
        StoreWAL2 s = new StoreWAL2(null);
        s.init();
        s.structuralLock.lock();

        s.longStackPut(masterLinkOffset, 10000);
        assertEquals(1, s.longStackPages.size());
        assertEquals(0, s.longStackCommited.size());
        TT.assertZeroes(s.vol, StoreDirect2.HEADER_SIZE, s.vol.length());

        s.structuralLock.unlock();
        s.commit();
        assertEquals(0, s.longStackPages.size());
        assertEquals(1, s.longStackCommited.size());
        TT.assertZeroes(s.vol, 24, s.vol.length());

        s.structuralLock.lock();
        assertEquals(10000, s.longStackTake(masterLinkOffset));
        assertEquals(1, s.longStackPages.size());
        assertEquals(1, s.longStackCommited.size());

        s.structuralLock.unlock();
        s.rollback();
        assertEquals(0, s.longStackPages.size());
        assertEquals(1, s.longStackCommited.size());

        s.structuralLock.lock();
        assertEquals(10000, s.longStackTake(masterLinkOffset));
        assertEquals(1, s.longStackPages.size());
        assertEquals(1, s.longStackCommited.size());

    }


}
