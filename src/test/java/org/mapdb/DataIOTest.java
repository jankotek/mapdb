package org.mapdb;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.mapdb.DataIO.*;

public class DataIOTest {

    @Test
    public void testPackLongBidi() throws Exception {
        DataOutputByteArray b = new DataOutputByteArray();

        long max = (long) 1e14;
        for(long i=0;i<max;i=i+1 +i/100000){
            b.pos=0;
            long size = packLongBidi(b,i);
            assertTrue(i>100000 || size<6);
            assertEquals(b.pos,size);
            assertEquals(i | (size<<56), unpackLongBidi(b.buf,0));
            assertEquals(i | (size<<56), unpackLongBidiReverse(b.buf, (int) size));
        }
    }
}