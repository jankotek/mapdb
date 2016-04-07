package org.mapdb;

import org.junit.Test;
import org.mapdb.volume.SingleByteArrayVol;
import org.mapdb.volume.Volume;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

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

    @Test public void zeroParity(){
        assertTrue(parity1Set(0)!=0);
        assertTrue(parity3Set(0)!=0);
        assertTrue(parity4Set(0)!=0);
        assertTrue(parity16Set(0)!=0);
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


    @Test public void testNextPowTwoLong(){
        assertEquals(1, DataIO.nextPowTwo(1L));
        assertEquals(2, DataIO.nextPowTwo(2L));
        assertEquals(4, DataIO.nextPowTwo(3L));
        assertEquals(4, DataIO.nextPowTwo(4L));

        assertEquals(64, DataIO.nextPowTwo(33L));
        assertEquals(64, DataIO.nextPowTwo(61L));

        assertEquals(1024, DataIO.nextPowTwo(777L));
        assertEquals(1024, DataIO.nextPowTwo(1024L));

        assertEquals(1073741824, DataIO.nextPowTwo(1073741824L-100));
        assertEquals(1073741824, DataIO.nextPowTwo((long) (1073741824*0.7)));
        assertEquals(1073741824, DataIO.nextPowTwo(1073741824L));
    }

    @Test public void testNextPowTwo2(){
        for(int i=1;i<1073750016;i+= 1 + i/100000){
            int pow = nextPowTwo(i);
            assertTrue(pow>=i);
            assertTrue(pow/2<i);
            assertTrue(Integer.bitCount(pow)==1);

        }
    }


    @Test public void testNextPowTwo2Long(){
        for(long i=1;i<10000L*Integer.MAX_VALUE;i+= 1 + i/100000){
            long pow = nextPowTwo(i);
            assertTrue(pow>=i);
            assertTrue(pow/2<i);
            assertTrue(Long.bitCount(pow)==1);

        }
    }


    @Test public void packLongCompat() throws IOException {
        DataOutput2 b = new DataOutput2();
        b.packLong(2111L);
        b.packLong(100);
        b.packLong(1111L);

        DataInput2.ByteArray b2 = new DataInput2.ByteArray(b.buf);
        assertEquals(2111L, b2.unpackLong());
        assertEquals(100L, b2.unpackLong());
        assertEquals(1111L, b2.unpackLong());

        DataInput2.ByteBuffer b3 = new DataInput2.ByteBuffer(ByteBuffer.wrap(b.buf),0);
        assertEquals(2111L, b3.unpackLong());
        assertEquals(100L, b3.unpackLong());
        assertEquals(1111L, b3.unpackLong());
    }

    @Test public void packIntCompat() throws IOException {
        DataOutput2 b = new DataOutput2();
        b.packInt(2111);
        b.packInt(100);
        b.packInt(1111);

        DataInput2.ByteArray b2 = new DataInput2.ByteArray(b.buf);
        assertEquals(2111, b2.unpackInt());
        assertEquals(100, b2.unpackInt());
        assertEquals(1111, b2.unpackInt());

        DataInput2.ByteBuffer b3 = new DataInput2.ByteBuffer(ByteBuffer.wrap(b.buf),0);
        assertEquals(2111, b3.unpackInt());
        assertEquals(100, b3.unpackInt());
        assertEquals(1111, b3.unpackInt());
    }


    @Test public void testHexaConversion(){
        byte[] b = new byte[]{11,112,11,0,39,90};
        assertTrue(Serializer.BYTE_ARRAY.equals(b, DataIO.fromHexa(DataIO.toHexa(b))));
    }

    @Test public void packLong() throws IOException {
        DataInput2.ByteArray in = new DataInput2.ByteArray(new byte[20]);
        DataOutput2 out = new DataOutput2();
        out.buf = in.buf;
        for (long i = 0; i >0; i = i + 1 + i / 10000) {
            in.pos = 10;
            out.pos = 10;

            DataIO.packLong((DataOutput)out,i);
            long i2 = DataIO.unpackLong(in);

            assertEquals(i,i2);
            assertEquals(in.pos,out.pos);
        }

    }

    @Test public void packInt() throws IOException {
        DataInput2.ByteArray in = new DataInput2.ByteArray(new byte[20]);
        DataOutput2 out = new DataOutput2();
        out.buf = in.buf;
        for (int i = 0; i >0; i = i + 1 + i / 10000) {
            in.pos = 10;
            out.pos = 10;

            DataIO.packInt((DataOutput)out,i);
            long i2 = DataIO.unpackInt(in);

            assertEquals(i,i2);
            assertEquals(in.pos,out.pos);
        }

    }

    @Test public void int2Long(){
        assertEquals(0x7fffffffL, DataIO.intToLong(0x7fffffff));
        assertEquals(0x80000000L, DataIO.intToLong(0x80000000));
        assertTrue(-1L != DataIO.intToLong(-1));
    }

    @Test public void packedLong_volume() throws IOException {
        DataOutput2 out = new DataOutput2();
        DataInput2.ByteArray in = new DataInput2.ByteArray(out.buf);
        Volume v = new SingleByteArrayVol(out.buf);

        for (long i = 0; i < 1e6; i++) {
            Arrays.fill(out.buf, (byte) 0);
            out.pos=10;
            out.packLong(i);
            assertEquals(i, v.getPackedLong(10)& DataIO.PACK_LONG_RESULT_MASK);
            assertEquals(DataIO.packLongSize(i), v.getPackedLong(10)>>>60);

            Arrays.fill(out.buf, (byte) 0);
            out.pos=10;
            out.packInt((int)i);
            assertEquals(i, v.getPackedLong(10)& DataIO.PACK_LONG_RESULT_MASK);
            assertEquals(DataIO.packLongSize(i), v.getPackedLong(10)>>>60);

            Arrays.fill(out.buf, (byte) 0);
            v.putPackedLong(10, i);
            in.pos=10;
            assertEquals(i, in.unpackLong());
            in.pos=10;
            assertEquals(i, in.unpackInt());
        }
    }


    @Test public void shift(){
        for(int i =0; i<30;i++){
            assertEquals(i, DataIO.shift(1<<i));
            assertEquals(i, DataIO.shift(nextPowTwo(1<<i)));
            if(i>2)
                assertEquals(i, DataIO.shift(nextPowTwo((1<<i)-1)));
        }
    }

    @Test public void putLong2(){
        long i = 123901230910290433L;
        byte[] b1 = new byte[10];
        byte[] b2 = new byte[10];

        DataIO.putLong(b1, 2, i);
        DataIO.putLong(b2, 2, i,8);

        assertArrayEquals(b1,b2);
    }

}