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
public class StoreDirectDuplicateTest {

    final StoreDirect2 s1,s2;


    public StoreDirectDuplicateTest(StoreDirect2 s1, StoreDirect2 s2) {
        this.s1 = s1;
        this.s2 = s2;
    }

    @Parameterized.Parameters
    public static Iterable params() throws IOException {
        List ret = new ArrayList();
        ret.add(new Object[]{new StoreDirect2(null), new StoreDirect2(null)});
        return ret;
    }

    private void assertBinaryEquals() {
        assertEquals(s1.vol.length(), s2.vol.length());
        long size = s1.vol.length();
        for(long offset=0;offset<size;offset++){
            assertEquals(s1.vol.getUnsignedByte(offset),s2.vol.getUnsignedByte(offset));
        }

    }

    @Test public void init(){
        s1.init();
        s2.init();

        s1.commit();
        s2.commit();
        assertBinaryEquals();
    }



    @Test public void long_stack_put(){
        s1.init();
        s1.vol.ensureAvailable(24);
        s1.structuralLock.lock();
        s1.headVol.putLong(16, parity4Set(0));
        s1.storeSize=32;
        s2.init();
        s2.vol.ensureAvailable(24);
        s2.structuralLock.lock();
        s2.headVol.putLong(16, parity4Set(0));
        s2.storeSize=32;

        s1.longStackPut(16, 1600);
        s2.longStackPut(16, 1600);


        s1.commit();
        s2.commit();
        assertBinaryEquals();
    }

}