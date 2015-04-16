package org.mapdb;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.*;
import static org.mapdb.DataIO.*;

public class DataIOTest {

    @Test public void parity1() {
        assertEquals(Long.parseLong("1", 2), parity1Set(0));
        assertEquals(Long.parseLong("10", 2), parity1Set(2));
        assertEquals(Long.parseLong("111", 2), parity1Set(Long.parseLong("110", 2)));
        assertEquals(Long.parseLong("1110", 2), parity1Set(Long.parseLong("1110", 2)));
        assertEquals(Long.parseLong("1011", 2), parity1Set(Long.parseLong("1010", 2)));
        assertEquals(Long.parseLong("11111", 2), parity1Set(Long.parseLong("11110", 2)));

        assertEquals(0, parity1Get(Long.parseLong("1", 2)));
        try {
            parity1Get(Long.parseLong("0", 2));
            fail();
        }catch(DBException.PointerChecksumBroken e){
            //TODO check mapdb specific error;
        }
        try {
            parity1Get(Long.parseLong("110", 2));
            fail();
        }catch(DBException.PointerChecksumBroken e){
            //TODO check mapdb specific error;
        }
    }

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

    @Test public void parityBasic(){
        for(long i=0;i<Integer.MAX_VALUE;i+= 1 + i/1000000L){
            if(i%2==0)
                assertEquals(i, parity1Get(parity1Set(i)));
            if(i%8==0)
                assertEquals(i, parity3Get(parity3Set(i)));
            if(i%16==0)
                assertEquals(i, parity4Get(parity4Set(i)));
            if((i&0xFFFF)==0)
                assertEquals(i, parity16Get(parity16Set(i)));
        }
    }

    @Test public void testSixLong(){
        byte[] b = new byte[8];
        for(long i=0;i>>>48==0;i=i+1+i/10000){
            DataIO.putSixLong(b,2,i);
            assertEquals(i, DataIO.getSixLong(b,2));
        }
    }

    @Test public void testNextPowTwo(){
        assertEquals(1, DataIO.nextPowTwo(1));
        assertEquals(2, DataIO.nextPowTwo(2));
        assertEquals(4, DataIO.nextPowTwo(3));
        assertEquals(4, DataIO.nextPowTwo(4));

        assertEquals(64, DataIO.nextPowTwo(33));
        assertEquals(64, DataIO.nextPowTwo(61));

        assertEquals(1024, DataIO.nextPowTwo(777));
        assertEquals(1024, DataIO.nextPowTwo(1024));

        assertEquals(1073741824, DataIO.nextPowTwo(1073741824-100));
        assertEquals(1073741824, DataIO.nextPowTwo((int) (1073741824*0.7)));
        assertEquals(1073741824, DataIO.nextPowTwo(1073741824));
    }

    @Test public void testNextPowTwo2(){
        for(int i=1;i<1073750016;i+= 1 + i/100000){
            int pow = nextPowTwo(i);
            assertTrue(pow>=i);
            assertTrue(Integer.bitCount(pow)==1);

        }
    }

    @Test public void packLongCompat() throws IOException {
        DataOutputByteArray b = new DataOutputByteArray();
        b.packLong(2111L);
        b.packLong(100);
        b.packLong(1111L);

        DataInputByteArray b2 = new DataInputByteArray(b.buf);
        assertEquals(2111L, b2.unpackLong());
        assertEquals(100L, b2.unpackLong());
        assertEquals(1111L, b2.unpackLong());

        DataInputByteBuffer b3 = new DataInputByteBuffer(ByteBuffer.wrap(b.buf),0);
        assertEquals(2111L, b3.unpackLong());
        assertEquals(100L, b3.unpackLong());
        assertEquals(1111L, b3.unpackLong());
    }

    @Test public void packIntCompat() throws IOException {
        DataOutputByteArray b = new DataOutputByteArray();
        b.packInt(2111);
        b.packInt(100);
        b.packInt(1111);

        DataInputByteArray b2 = new DataInputByteArray(b.buf);
        assertEquals(2111, b2.unpackInt());
        assertEquals(100, b2.unpackInt());
        assertEquals(1111, b2.unpackInt());

        DataInputByteBuffer b3 = new DataInputByteBuffer(ByteBuffer.wrap(b.buf),0);
        assertEquals(2111, b3.unpackInt());
        assertEquals(100, b3.unpackInt());
        assertEquals(1111, b3.unpackInt());
    }


    @Test public void testHexaConversion(){
        byte[] b = new byte[]{11,112,11,0,39,90};
        assertTrue(Serializer.BYTE_ARRAY.equals(b, DataIO.fromHexa(DataIO.toHexa(b))));
    }
}