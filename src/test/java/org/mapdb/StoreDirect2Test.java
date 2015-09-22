package org.mapdb;


import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class StoreDirect2Test extends StoreDirect2_BaseTest{

    @Override
    protected StoreDirect2 openStore(String file) {
        return new StoreDirect2(file);
    }

    @Test public void constants(){
        assertEquals(0, StoreDirect2.HEADER_SIZE%16);
    }


    @Test public void longStackDump(){
        StoreDirect2 s = new StoreDirect2(null);
        s.init();
        s.structuralLock.lock();
        int max = 100000;
        for(long i=0;i<max;i++){
            s.longStackPut(StoreDirect2.O_STACK_FREE_RECID,i+1000);
        }

        List<Long> stack = s.longStackDump(StoreDirect2.O_STACK_FREE_RECID);
        Collections.sort(stack);
        assertEquals(max, stack.size());
        for(Long i=0L;i<max;i++){
            assertEquals(new Long(i + 1000), stack.get( i.intValue()));
        }
    }

    @Test public void storeCheck(){
        StoreDirect2 s = new StoreDirect2(null);
        s.init();
        s.storeCheck();

        s.structuralLock.lock();
        s.longStackPut(StoreDirect2.longStackMasterLinkOffset(160), 16L);

        try {
            s.storeCheck();
            throw new RuntimeException();
        }catch(AssertionError e){
            assertEquals("Offset is marked twice: 16",e.getMessage());
        }
    }

}