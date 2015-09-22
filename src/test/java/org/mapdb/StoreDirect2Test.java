package org.mapdb;


import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static org.mapdb.DataIO.*;
import static org.mapdb.StoreDirect2.round16Up;

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

    @Test public void put_delete(){
        StoreDirect2 s = new StoreDirect2(null);
        s.init();


        byte[] b = TT.randomByteArray(100,11);
        long recid = s.put(b, Serializer.BYTE_ARRAY_NOSIZE);

        byte[] b2 = new byte[b.length];
        s.vol.getData(StoreDirect2.PAGE_SIZE, b2, 0, b2.length);
        assertArrayEquals(b, b2);
        assertEquals(StoreDirect2.PAGE_SIZE + round16Up(b.length), parity4Get(s.vol.getLong(StoreDirect2.O_STORE_SIZE)));

        s.delete(recid, Serializer.BYTE_ARRAY_NOSIZE);

        assertEquals(StoreDirect2.PAGE_SIZE + 160, parity4Get(s.vol.getLong(StoreDirect2.O_STORE_SIZE)));
        s.structuralLock.lock();
        assertEquals(recid, s.freeRecidTake());
        assertEquals(StoreDirect2.PAGE_SIZE, parity4Get(s.vol.getLong(StoreDirect2.O_STORE_SIZE)));

    }

}