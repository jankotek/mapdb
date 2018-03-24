package org.mapdb.io;

import org.junit.Test;
import org.mapdb.DBException;


import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.*;
import static org.mapdb.io.DataIO.*;

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

/* TODO tests
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



    @Test
    public void packedLong_volume() throws IOException {
        if(TT.shortTest())
            return;

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

*/
    @Test public void testHexaConversion(){
        byte[] b = new byte[]{11,112,11,0,39,90};
        assertTrue(Arrays.equals(b, DataIO.fromHexa(DataIO.toHexa(b))));
    }
    @Test public void int2Long(){
        assertEquals(0x7fffffffL, DataIO.intToLong(0x7fffffff));
        assertEquals(0x80000000L, DataIO.intToLong(0x80000000));
        assertTrue(-1L != DataIO.intToLong(-1));
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

    @Test public void testFillLowBits(){
        for (int bitCount = 0; bitCount < 64; bitCount++) {
            assertEquals(
                    "fillLowBits should return a long value with 'bitCount' least significant bits set to one",
                    (1L << bitCount) - 1, DataIO.fillLowBits(bitCount));
        }
    }

    Random random = new Random();

    @Test public void testPutLong() throws IOException {
        for (long valueToPut = 0; valueToPut < Long.MAX_VALUE
                && valueToPut >= 0; valueToPut = random.nextInt(2) + valueToPut * 2) {
            byte[] buffer = new byte[20];
            DataIO.putLong(buffer, 2, valueToPut);
            long returned = DataIO.getLong(buffer, 2);
            assertEquals("The value that was put and the value returned from getLong do not match", valueToPut, returned);
            DataIO.putLong(buffer, 2, -valueToPut);
            returned = DataIO.getLong(buffer, 2);
            assertEquals("The value that was put and the value returned from getLong do not match", -valueToPut, returned);
        }
    }


    @Test(expected = EOFException.class)
    public void testReadFully_throws_exception_if_not_enough_data() throws IOException {
        InputStream inputStream = new ByteArrayInputStream(new byte[0]);
        DataIO.readFully(inputStream, new byte[1]);
        fail("An EOFException should have occurred by now since there are not enough bytes to read from the InputStream");
    }

    @Test public void testReadFully_with_too_much_data() throws IOException {
        byte[] inputBuffer = new byte[] { 1, 2, 3, 4 };
        InputStream in = new ByteArrayInputStream(inputBuffer);
        byte[] outputBuffer = new byte[3];
        DataIO.readFully(in, outputBuffer);
        byte[] expected = new byte[] { 1, 2, 3 };
        assertArrayEquals("The passed buffer should be filled with the first three bytes read from the InputStream",
                expected, outputBuffer);
    }

    @Test public void testReadFully_with_data_length_same_as_buffer_length() throws IOException {
        byte[] inputBuffer = new byte[] { 1, 2, 3, 4 };
        InputStream in = new ByteArrayInputStream(inputBuffer);
        byte[] outputBuffer = new byte[4];
        DataIO.readFully(in, outputBuffer);
        assertArrayEquals("The passed buffer should be filled with the whole content of the InputStream"
                + " since the buffer length is exactly same as the data length", inputBuffer, outputBuffer);
    }


    @Test public void testPackLong_WithStreams() throws IOException{
        for (long valueToPack = 0; valueToPack < Long.MAX_VALUE
                && valueToPack >= 0; valueToPack = random.nextInt(2) + valueToPack * 2) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            DataIO.packLong(outputStream, valueToPack);
            DataIO.packLong(outputStream, -valueToPack);
            ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
            long unpackedLong = DataIO.unpackLong(inputStream);
            assertEquals("Packed and unpacked values do not match", valueToPack, unpackedLong);
            unpackedLong = DataIO.unpackLong(inputStream);
            assertEquals("Packed and unpacked values do not match", -valueToPack, unpackedLong);
        }
    }

    @Test(expected = EOFException.class)
    public void testUnpackLong_withInputStream_throws_exception_when_stream_is_empty() throws IOException {
        DataIO.unpackLong(new ByteArrayInputStream(new byte[0]));
        fail("An EOFException should have occurred by now since there are no bytes to read from the InputStream");
    }

    @Test public void testPackLongSize() {
        assertEquals("packLongSize should have returned 1 since number 1 can be represented using 1 byte when packed",
                1, DataIO.packLongSize(1));
        assertEquals("packLongSize should have returned 2 since 1 << 7 can be represented using 2 bytes when packed", 2,
                DataIO.packLongSize(1 << 7));
        assertEquals("packLongSize should have returned 10 since 1 << 63 can be represented using 10 bytes when packed", 10,
                DataIO.packLongSize(1 << 63));
    }

}