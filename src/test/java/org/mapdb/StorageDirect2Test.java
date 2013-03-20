package org.mapdb;


import org.junit.Test;

import static org.junit.Assert.*;
import static org.mapdb.StorageDirect2.*;

public class StorageDirect2Test extends EngineTest<StorageDirect2>{

    Volume.Factory fac = Volume.memoryFactory(false);

    @Override protected StorageDirect2 openEngine() {
        return new StorageDirect2(fac,false);
    }

    @Test
    public void phys_append_alloc(){
        long[] ret = e.physAllocate(100);
        long expected = 100L<<48 | 8L;
        assertArrayEquals(new long[]{expected}, ret);
    }

    @Test
    public void phys_append_alloc_link2(){
        long[] ret = e.physAllocate(100 + MAX_REC_SIZE);
        long exp1 = MASK_IS_LINKED |((long)MAX_REC_SIZE)<<48 | 8L;
        long exp2 = 112L<<48 | (8L+MAX_REC_SIZE);
        assertArrayEquals(new long[]{exp1, exp2}, ret);
    }

    @Test
    public void phys_append_alloc_link3(){
        long[] ret = e.physAllocate(100 + MAX_REC_SIZE*2);
        long exp1 = MASK_IS_LINKED | ((long)MAX_REC_SIZE)<<48 | 8L;
        long exp2 = MASK_IS_LINKED | ((long)MAX_REC_SIZE)<<48 | (8L+MAX_REC_SIZE);
        long exp3 = ((long)120)<<48 | (8L+MAX_REC_SIZE*2);

        assertArrayEquals(new long[]{exp1, exp2, exp3}, ret);
    }


}
