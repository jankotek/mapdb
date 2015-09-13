package org.mapdb;


import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mapdb.DataIO.parity4Set;

public class StoreWAL_LongStack_Test {

    @Test
    public void commit(){
        StoreWAL2 s = new StoreWAL2(null);
        s.init();
        s.structuralLock.lock();
        s.headVol.putLong(16, parity4Set(0));

        s.longStackPut(16, 10000);
        assertEquals(1, s.longStackPages.size);
        assertEquals(0, s.longStackCommited.size);
        TT.assertZeroes(s.vol, 24, s.vol.length());

        s.structuralLock.unlock();
        s.commit();
        assertEquals(0, s.longStackPages.size);
        assertEquals(1, s.longStackCommited.size);
        TT.assertZeroes(s.vol, 24, s.vol.length());

        s.structuralLock.lock();
        assertEquals(10000, s.longStackTake(16));
        assertEquals(1, s.longStackPages.size);
        assertEquals(1, s.longStackCommited.size);

    }

}
