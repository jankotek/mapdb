package org.mapdb;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;
/**
 * Tests contract of various implementations of Engine interface
 */
public abstract class EngineTest<ENGINE extends Engine>{

    protected abstract ENGINE openEngine();

    void reopen(){
        e.close();
        e=openEngine();
    }

    ENGINE e;
    @Before public void init(){
        e = openEngine();
    }

    @Test public void put_get(){
        Long l = 11231203099090L;
        long recid = e.put(l, Serializer.LONG_SERIALIZER);
        assertEquals(l, e.get(recid, Serializer.LONG_SERIALIZER));
    }

    @Test public void put_reopen_get(){
        Long l = 11231203099090L;
        long recid = e.put(l, Serializer.LONG_SERIALIZER);
        reopen();
        assertEquals(l, e.get(recid, Serializer.LONG_SERIALIZER));
    }

    @Test public void put_get_large(){
        byte[] b = new byte[(int) 1e6];
        Utils.RANDOM.nextBytes(b);
        long recid = e.put(b, StorageTestCase.BYTE_ARRAY_SERIALIZER);
        assertArrayEquals(b, e.get(recid, StorageTestCase.BYTE_ARRAY_SERIALIZER));
    }

    @Test public void put_reopen_get_large(){
        byte[] b = new byte[(int) 1e6];
        Utils.RANDOM.nextBytes(b);
        long recid = e.put(b, StorageTestCase.BYTE_ARRAY_SERIALIZER);
        reopen();
        assertArrayEquals(b, e.get(recid, StorageTestCase.BYTE_ARRAY_SERIALIZER));
    }


    @Test public void first_recid(){
        assertEquals(Engine.LAST_RESERVED_RECID+1, e.put(1,Serializer.INTEGER_SERIALIZER));
    }



}
