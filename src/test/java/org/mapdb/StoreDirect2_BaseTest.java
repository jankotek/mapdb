package org.mapdb;


import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public abstract class StoreDirect2_BaseTest {

    protected abstract StoreDirect2 openStore(String file);

    @Test public void test_init(){
        StoreDirect2 s = openStore(null);
        s.init();
        s.structuralLock.lock();
        assertEquals(StoreDirect2.HEADER_SIZE, s.storeSizeGet());
        for(long masterLinkOffset = StoreDirect2.O_STACK_FREE_RECID;masterLinkOffset<StoreDirect2.HEADER_SIZE;masterLinkOffset+=8) {
            assertEquals(0L, s.longStackTake(masterLinkOffset));
        }
        s.structuralLock.unlock();
        s.close();
    }


    @Test public void test_reopen(){
        File f = TT.tempDbFile();
        StoreDirect2 s = openStore(f.getPath());
        s.init();
        s.structuralLock.lock();
        s.longStackPut(StoreDirect2.O_STACK_FREE_RECID, 111L);
        long storeSize = s.storeSizeGet();
        s.structuralLock.unlock();
        s.commit();
        s.close();
        s = openStore(f.getPath());
        s.init();
        s.structuralLock.lock();
        assertEquals(storeSize, s.storeSizeGet());
        assertEquals(111L, s.longStackTake(StoreDirect2.O_STACK_FREE_RECID));
        s.structuralLock.unlock();
        s.commit();
        s.close();
        f.delete();
    }




    @Test
    public void long_stack_putGet_all_sizes(){
        for(long masterLinkOffset = StoreDirect2.O_STACK_FREE_RECID;masterLinkOffset<StoreDirect2.HEADER_SIZE;masterLinkOffset+=8) {
            StoreDirect2 s = openStore(null);
            s.init();
            s.structuralLock.lock();
            assertTrue(masterLinkOffset < StoreDirect2.HEADER_SIZE);
            s.longStackPut(masterLinkOffset, 1111L);
            assertEquals(1111L, s.longStackTake(masterLinkOffset));
            s.structuralLock.unlock();
            s.commit(); // no warnings
            s.close();
        }
    }

}
