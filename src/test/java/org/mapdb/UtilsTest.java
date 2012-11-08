package org.mapdb;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class UtilsTest {


    @Test public void testPackInt() throws Exception {

        DataOutput2 out = new DataOutput2();
        DataInput2 in = new DataInput2(ByteBuffer.wrap(out.buf,0, out.pos),0);
        for(int i = 0;i>-1; i = i + 1 + i/1111){  //overflow is expected
            out.pos = 0;

            Utils.packInt(out, i);
            in.pos = 0;
            in.buf.clear();

            int i2 = Utils.unpackInt(in);

            Assert.assertEquals(i, i2);

        }

    }

    @Test public void testPackLong() throws Exception {

        DataOutput2 out = new DataOutput2();
        DataInput2 in = new DataInput2(ByteBuffer.wrap(out.buf,0, out.pos),0);
        for(long i = 0;i>-1L  ; i=i+1 + i/111){  //overflow is expected
            out.pos = 0;

            Utils.packLong(out, i);
            in.pos = 0;
            in.buf.clear();

            long i2 = Utils.unpackLong(in);
            Assert.assertEquals(i, i2);

        }
    }

    @Test public void testArrayPut(){
        assertEquals(asList(1,2,3,4,5), asList(Utils.arrayPut(new Integer[]{1, 2, 4, 5}, 2, 3)));
        assertEquals(asList(1,2,3,4,5), asList(Utils.arrayPut(new Integer[]{2, 3, 4, 5}, 0, 1)));
        assertEquals(asList(1,2,3,4,5), asList(Utils.arrayPut(new Integer[]{1, 2, 3, 4}, 4, 5)));
    }

    @Test
    public void testNextPowTwo() throws Exception {
        assertEquals(16, Utils.nextPowTwo(9));
        assertEquals(8, Utils.nextPowTwo(8));
    }





}
