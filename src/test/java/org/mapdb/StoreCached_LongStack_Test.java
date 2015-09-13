package org.mapdb;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mapdb.DataIO.packLongSize;
import static org.mapdb.DataIO.parity4Set;

public class StoreCached_LongStack_Test {

    @Test public void modified(){
        StoreCached2 s = new StoreCached2(null);
        s.structuralLock.lock();
        s.init();
        s.vol.ensureAvailable(1000);
        s.storeSize=16;
        s.headVol.putLong(8, parity4Set(0));
        s.longStackPut(8, 1000);

        // Long Stack Page should be stored in collection
        assertEquals(1, s.longStackPages.size);
        assertEquals(161, s.longStackPages.get(16L).length);
        // original store is not modified
        TT.assertZeroes(s.vol, 0, 1000);
        // and headVol is separate buffer
        long expectedMasterLinkValue = parity4Set(((8L + packLongSize(1000)) << 48) + 16);
        assertEquals(expectedMasterLinkValue, s.headVol.getLong(8));

        s.structuralLock.unlock();
        //commit should flush the collections
        s.commit();
        assertEquals(0, s.longStackPages.size);
        assertEquals(expectedMasterLinkValue, s.headVol.getLong(8));
        assertEquals(16+160,s.storeSize);
    }

    @Test public void put_take(){
        StoreCached2 s = new StoreCached2(null);
        s.structuralLock.lock();
        s.init();
        s.vol.ensureAvailable(1000);
        s.storeSize=16;
        s.headVol.putLong(8, parity4Set(0));

        //put into page, it should end in collection
        s.longStackPut(8, 1000);
        assertEquals(1, s.longStackPages.size);

        //taking it back should remove the given page from collection
        assertEquals(1000, s.longStackTake(8));
        assertEquals(0, s.longStackPages.size);
        assertEquals(parity4Set(0), s.headVol.getLong(8));
        TT.assertZeroes(s.vol, 0, 1000);
    }

    @Test public void put_take_two(){
        StoreCached2 s = new StoreCached2(null);
        s.structuralLock.lock();
        s.init();
        s.vol.ensureAvailable(1000);
        s.storeSize=16;
        s.headVol.putLong(8, parity4Set(0));

        //put into page, it should end in collection
        s.longStackPut(8, 1000);
        s.longStackPut(8, 1200);
        assertEquals(1, s.longStackPages.size);

        //taking it back should remove the given page from collection
        assertEquals(1200, s.longStackTake(8));
        assertEquals(1000, s.longStackTake(8));
        assertEquals(0, s.longStackPages.size);
        assertEquals(parity4Set(0), s.headVol.getLong(8));
        TT.assertZeroes(s.vol, 0, 1000);
    }

}
