package org.mapdb;


import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mapdb.DataIO.parity4Set;

public class StoreDirect2Test {

    @Test
    public void header(){
        StoreDirect2 s = new StoreDirect2(null);
        s.init();

        assertTrue(s.vol == s.headVol);

        for(long offset=StoreDirect2.O_STACK_FREE_RECID; offset<StoreDirect2.HEADER_SIZE;offset+=8){
            assertEquals(parity4Set(0), s.headVol.getLong(offset));
        }

    }

}