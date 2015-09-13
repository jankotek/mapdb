package org.mapdb;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mapdb.DataIO.*;

/**
 * Makes operations on two stores in parallel and verifies they are binary same
 */
@RunWith(Parameterized.class)
public class StoreDirect_BinaryEqual_Test {

    final StoreDirect2[] stores;
    final StoreDirect2 s1,s2;


    public StoreDirect_BinaryEqual_Test(StoreDirect2 s1, StoreDirect2 s2) {
        this.stores = new StoreDirect2[]{s1,s2};
        this.s1 = s1;
        this.s2 = s2;
    }

    @Parameterized.Parameters
    public static Iterable params() throws IOException {
        List ret = new ArrayList();
        ret.add(new Object[]{new StoreDirect2(null), new StoreDirect2(null)});
        ret.add(new Object[]{new StoreDirect2(null), new StoreCached2(null)});
        ret.add(new Object[]{new StoreDirect2(null), new StoreWAL2(null)});
        return ret;
    }

    private void assertBinaryEquals() {
        assertEquals(s1.vol.length(), s2.vol.length());
        long size = s1.vol.length();
        for(long offset=0;offset<size;offset++){
            int b1 = s1.vol.getUnsignedByte(offset);
            int b2 = s2.vol.getUnsignedByte(offset);
            if(b1!=b2)
                throw new AssertionError("Not equal at offset:"+offset+",  "+b1+"!="+b2);
        }
    }

    protected void commit(StoreDirect2 s) {
        if(s instanceof StoreWAL2)
            ((StoreWAL2)s).commitFullReplay();
        else
            s.commit();

    }


    @Test public void init(){
        s1.init();
        s2.init();

        commit(s1);
        commit(s2);
        assertBinaryEquals();
    }



    @Test public void long_stack_put(){
        for(StoreDirect2 s:stores) {
            s.init();
            s.structuralLock.lock();
            s.headVol.putLong(16, parity4Set(0));
            s.longStackPut(16, 1600);
            s.structuralLock.unlock();
            commit(s);
        }
        assertBinaryEquals();
    }

    @Test public void long_stack_put2(){
        for(StoreDirect2 s:stores) {
            s.init();
            s.structuralLock.lock();
            s.headVol.putLong(16, parity4Set(0));
            s.longStackPut(16, 1600);
            s.longStackPut(16, 1900);
            s.structuralLock.unlock();
            commit(s);
        }
        assertBinaryEquals();
    }

    @Test public void long_stack_put_many(){
        for(StoreDirect2 s:stores) {
            s.init();
            s.structuralLock.lock();
            s.headVol.putLong(16, parity4Set(0));
            for(long i=1000;i<2000;i++) {
                s.longStackPut(16, i);
            }
            s.structuralLock.unlock();
            commit(s);
        }
        assertBinaryEquals();
    }

    @Test public void long_stack_put__many(){
        for(StoreDirect2 s:stores) {
            s.init();
            s.structuralLock.lock();
            s.headVol.putLong(16, parity4Set(0));
            for(long i=1000;i<2000;i++) {
                s.longStackPut(16, i);
            }

            s.structuralLock.unlock();
            commit(s);
        }
        assertBinaryEquals();
    }


    @Test public void long_stack_put_take_many(){
        for(StoreDirect2 s:stores) {
            s.init();
            s.structuralLock.lock();
            s.headVol.putLong(16, parity4Set(0));
            for(long i=1000;i<2000;i++) {
                s.longStackPut(16, i);
            }
            for(long i=1000;i<2000;i++) {
                s.longStackTake(16);
            }

            s.structuralLock.unlock();
            commit(s);
        }
        assertBinaryEquals();
    }



}