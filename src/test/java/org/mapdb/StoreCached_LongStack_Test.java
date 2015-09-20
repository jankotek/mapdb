package org.mapdb;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mapdb.DataIO.packLongSize;
import static org.mapdb.DataIO.parity4Set;

public class StoreCached_LongStack_Test {

    final long masterLinkOffset = StoreDirect2.O_STACK_FREE_RECID;

    @Test public void modified(){
        StoreCached2 s = new StoreCached2(null);
        s.structuralLock.lock();
        s.init();
        s.longStackPut(masterLinkOffset, 1000);

        // Long Stack Page should be stored in collection
        assertEquals(1, s.longStackPages.size());
        assertEquals(161, s.longStackPages.get(StoreDirect2.HEADER_SIZE).length);
        // original store is not modified
        TT.assertZeroes(s.vol, StoreDirect2.HEADER_SIZE , s.vol.length());
        // and headVol is separate buffer
        long expectedMasterLinkValue = parity4Set(((8L + packLongSize(1000)) << 48) + StoreDirect2.HEADER_SIZE);
        assertEquals(expectedMasterLinkValue, s.headVol.getLong(masterLinkOffset));

        s.structuralLock.unlock();
        //commit should flush the collections
        s.commit();
        assertEquals(0, s.longStackPages.size());
        assertEquals(expectedMasterLinkValue, s.headVol.getLong(masterLinkOffset));
        assertEquals(StoreDirect2.HEADER_SIZE+160,s.storeSize);
    }

    @Test public void put_take(){
        StoreCached2 s = new StoreCached2(null);
        s.structuralLock.lock();
        s.init();

        //put into page, it should end in collection
        s.longStackPut(masterLinkOffset, 1000);
        assertEquals(1, s.longStackPages.size());

        //taking it back should remove the given page from collection
        assertEquals(1000, s.longStackTake(masterLinkOffset));
        assertEquals(0, s.longStackPages.size());
        assertEquals(parity4Set(0), s.headVol.getLong(masterLinkOffset));
        TT.assertZeroes(s.vol, 0, s.vol.length());
    }

    @Test public void put_take_two(){
        StoreCached2 s = new StoreCached2(null);
        s.structuralLock.lock();
        s.init();
        s.headVol.putLong(masterLinkOffset, parity4Set(0));

        //put into page, it should end in collection
        s.longStackPut(masterLinkOffset, 1000);
        s.longStackPut(masterLinkOffset, 1200);
        assertEquals(1, s.longStackPages.size());

        //taking it back should remove the given page from collection
        assertEquals(1200, s.longStackTake(masterLinkOffset));
        assertEquals(1000, s.longStackTake(masterLinkOffset));
        assertEquals(0, s.longStackPages.size());
        assertEquals(parity4Set(0), s.headVol.getLong(masterLinkOffset));
        TT.assertZeroes(s.vol, 0, s.vol.length());
    }

}
