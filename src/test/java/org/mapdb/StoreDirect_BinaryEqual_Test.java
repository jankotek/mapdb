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

    final long masterLinkOffset = StoreDirect2.O_STACK_FREE_RECID;

    final Fun.Function0<StoreDirect2> s1,s2;


    public StoreDirect_BinaryEqual_Test(Fun.Function0<StoreDirect2> s1, Fun.Function0<StoreDirect2> s2) {
        this.s1 = s1;
        this.s2 = s2;
    }

    @Parameterized.Parameters
    public static Iterable params() throws IOException {
        List ret = new ArrayList();
        Fun.Function0<StoreDirect2> direct = new Fun.Function0<StoreDirect2>() {
            @Override public StoreDirect2 run() {
                return new StoreDirect2(null);
            }
        };
        Fun.Function0<StoreDirect2> cached = new Fun.Function0<StoreDirect2>() {
            @Override public StoreDirect2 run() {
                return new StoreCached2(null);
            }
        };
        Fun.Function0<StoreDirect2> wal = new Fun.Function0<StoreDirect2>() {
            @Override public StoreDirect2 run() {
                return new StoreWAL2(null);
            }
        };
        ret.add(new Object[]{direct, direct});
        ret.add(new Object[]{direct, cached});
        ret.add(new Object[]{direct, wal});
        return ret;
    }

    StoreDirect2[] stores(){
        return new StoreDirect2[]{s1.run(), s2.run()};
    }

    private void assertBinaryEquals(StoreDirect2[] stores) {
        StoreDirect2 s1 = stores[0];
        StoreDirect2 s2 = stores[1];
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
        StoreDirect2[] stores = stores();
        for(StoreDirect2 s:stores){
            s.init();
            commit(s);
        }
        assertBinaryEquals(stores);
    }



    @Test public void long_stack_put(){
        StoreDirect2[] stores = stores();
        for(StoreDirect2 s:stores) {
            s.init();
            s.structuralLock.lock();
            s.longStackPut(masterLinkOffset, 1600);
            s.structuralLock.unlock();
            commit(s);
        }
        assertBinaryEquals(stores);
    }

    @Test public void long_stack_put2(){
        StoreDirect2[] stores = stores();
        for(StoreDirect2 s:stores) {
            s.init();
            s.structuralLock.lock();
            s.longStackPut(masterLinkOffset, 1600);
            s.longStackPut(masterLinkOffset, 1900);
            s.structuralLock.unlock();
            commit(s);
        }
        assertBinaryEquals(stores);
    }

    @Test public void long_stack_put_many(){
        StoreDirect2[] stores = stores();
        for(StoreDirect2 s:stores) {
            s.init();
            s.structuralLock.lock();
            for(long i=1000;i<2000;i++) {
                s.longStackPut(masterLinkOffset, i);
            }
            s.structuralLock.unlock();
            commit(s);
        }
        assertBinaryEquals(stores);
    }


    @Test public void long_stack_put_take_many(){
        StoreDirect2[] stores = stores();
        for(StoreDirect2 s:stores) {
            s.init();
            s.structuralLock.lock();
            s.headVol.putLong(masterLinkOffset, parity4Set(0));
            for(long i=1000;i<2000;i++) {
                s.longStackPut(masterLinkOffset, i);
            }
            for(long i=1000;i<2000;i++) {
                s.longStackTake(masterLinkOffset);
            }

            s.structuralLock.unlock();
            commit(s);
        }
        assertBinaryEquals(stores);
    }


    @Test public void mixed(){
        StoreDirect2[] stores = stores();
        for(StoreDirect2 s:stores) {
            s.init();
            s.structuralLock.lock();
            for(long i=1000;i<2000;i++) {
                s.longStackPut(masterLinkOffset, i);
            }

            for(long i=1000;i<2000;i++) {
                s.longStackPut(masterLinkOffset+8, i);
            }

            for(long i=1000;i<2000;i++) {
                s.longStackTake(masterLinkOffset);
            }

            s.structuralLock.unlock();
            commit(s);
        }
        assertBinaryEquals(stores);

    }

    @Test public void mixed_huge(){
        if(TT.shortTest())
            return;
        StoreDirect2[] stores = stores();
        long scale = (long) (1e7*TT.scale());

        for(StoreDirect2 s:stores) {
            Random r = new Random(0);
            s.init();
            s.structuralLock.lock();

            for(long i=0;i<scale;i++) {
                long offset = StoreDirect2.O_STACK_FREE_RECID;

                if (r.nextInt(100) < 12) {
                    s.longStackTake(offset);
                } else {
                    s.longStackPut(offset, 1+r.nextInt((int) 1e7));
                }
            }

            s.structuralLock.unlock();
            commit(s);
        }
        assertBinaryEquals(stores);
    }



}