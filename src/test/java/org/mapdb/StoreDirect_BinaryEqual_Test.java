package org.mapdb;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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
        assertEquals(s1.longStackDumpAll(), s2.longStackDumpAll());

        long s1len = s1.vol.length();
        long s2len = s2.vol.length();
        long maxLen = Math.max(s1len, s2len);
        for(long offset=0;offset<maxLen;offset++){
            int b1 = offset<s1len?s1.vol.getUnsignedByte(offset):0;
            int b2 = offset<s2len?s2.vol.getUnsignedByte(offset):0;
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


    @Test public void mixed(){
        for(StoreDirect2 s:stores) {
            s.init();
            s.structuralLock.lock();
            s.headVol.putLong(16, parity4Set(0));
            s.headVol.putLong(24, parity4Set(0));
            for(long i=1000;i<2000;i++) {
                s.longStackPut(16, i);
            }

            for(long i=1000;i<2000;i++) {
                s.longStackPut(24, i);
            }

            for(long i=1000;i<2000;i++) {
                s.longStackTake(16);
            }

            s.structuralLock.unlock();
            commit(s);
        }
        assertBinaryEquals();

    }

    @Test public void mixed_huge(){
        if(TT.shortTest())
            return;

        long scale = (long) (1e7*TT.scale());

        for(StoreDirect2 s:stores) {
            Random r = new Random(0);
            s.init();
            s.structuralLock.lock();
            for(long offset=0;offset<StoreDirect2.HEADER_SIZE;offset+=8){
                s.headVol.putLong(offset, parity4Set(0));
            }

            for(long i=0;i<scale;i++) {
                long offset = r.nextInt((StoreDirect2.HEADER_SIZE-8) / 8) * 8;
                if (r.nextInt(10) < 3) {
                    s.longStackTake(offset);
                } else {
                    s.longStackPut(offset, 1+r.nextInt((int) 1e7));
                }
            }

            s.structuralLock.unlock();
            commit(s);
        }
        assertBinaryEquals();
    }



}