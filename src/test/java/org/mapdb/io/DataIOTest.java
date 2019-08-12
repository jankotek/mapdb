package org.mapdb.io;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mapdb.DBException;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.*;
import static org.mapdb.io.DataIO.*;
import static org.mockito.Matchers.anyString;

@RunWith(PowerMockRunner.class)
public class DataIOTest {

    @Rule public final ExpectedException thrown = ExpectedException.none();

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

    @Test public void testUnpackInt() throws IOException {
        Assert.assertEquals(126, DataIO.unpackInt(
                new ByteArrayInputStream(new byte[]{-2, -15, -128})));
        Assert.assertEquals(0, DataIO.unpackInt((DataInput) new
                DataInputStream(new ByteArrayInputStream(new byte[]{-128}))));

        Assert.assertEquals(0, DataIO.unpackInt(new byte[]{-128}, 0));
        Assert.assertEquals(0, DataIO.unpackInt(new byte[]{0, -128}, 0));
    }

    @Test public void testUnpackIntThrowsException1() throws IOException {
        thrown.expect(EOFException.class);
        DataIO.unpackInt((DataInput)new DataInputStream(
                new ByteArrayInputStream(new byte[]{0})));
    }

    @Test public void testUnpackIntThrowsException2() throws IOException {
        thrown.expect(EOFException.class);
        DataIO.unpackInt(new ByteArrayInputStream(new byte[]{0}));
    }

    @Test public void testUnpackLong() throws IOException {
        Assert.assertEquals(0L, DataIO.unpackLong(
                new ByteArrayInputStream(new byte[]{0, -128, -127})));
        Assert.assertEquals(0L, DataIO.unpackLong(
                new ByteArrayInputStream(new byte[]{-128, -127})));
        Assert.assertEquals(0, DataIO.unpackLong((DataInput) new
                DataInputStream(new ByteArrayInputStream(new byte[]{-128}))));

        Assert.assertEquals(0L, DataIO.unpackLong(new byte[]{-128}, 0));
        Assert.assertEquals(0L, DataIO.unpackLong(new byte[]{0, -128}, 0));
    }

    @Test public void testUnpackLongThrowsException() throws IOException {
        thrown.expect(EOFException.class);
        DataIO.unpackLong(new ByteArrayInputStream(new byte[]{0}));
    }

    @Test public void testPackLong() {
        Assert.assertArrayEquals(new byte[2], new byte[2]);
        Assert.assertArrayEquals(new byte[9], new byte[9]);

        Assert.assertEquals(1, DataIO.packLong(new byte[2], 1, 0L));
        Assert.assertEquals(2, DataIO.packLong(new byte[9], 3, 2048L));
    }

    @Test public void testPackLongSize2() {
        Assert.assertEquals(1, DataIO.packLongSize(0L));
        Assert.assertEquals(2, DataIO.packLongSize(2048L));
    }

    @Test public void testUnpackRecid() throws Exception {
        final DataInput2 in = PowerMockito.mock(DataInput2.class);
        PowerMockito.when(in.readPackedLong()).thenReturn(4610L);

        Assert.assertEquals(2305L, DataIO.unpackRecid(in));
    }

    @Test public void testLongHash() {
        Assert.assertEquals(0, DataIO.longHash(0L));
    }

    @Test public void testIntHash() {
        Assert.assertEquals(0, DataIO.intHash(0));
    }

    @Test public void getInt() {
        Assert.assertEquals(
                66051, DataIO.getInt(new byte[] {0, 1, 2, 3, 4, 5}, 0));
        Assert.assertEquals(
                16909060, DataIO.getInt(new byte[] {0, 1, 2, 3, 4, 5}, 1));
    }

    @Test public void getLong() {
        Assert.assertEquals(283686952306183l,
                DataIO.getLong(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8}, 0));
        Assert.assertEquals(72623859790382856l,
                DataIO.getLong(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8}, 1));
    }

    @Test public void testPackInt() {
        Assert.assertArrayEquals(new byte[6], new byte[6]);
        Assert.assertArrayEquals(new byte[8], new byte[8]);

        Assert.assertEquals(2, DataIO.packInt(new byte[6], 2, 128));
        Assert.assertEquals(1, DataIO.packInt(new byte[8], 1, 0));
    }

    @Test public void getSixLong() {
        Assert.assertEquals(4328719365l,
                DataIO.getSixLong(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8}, 0));
        Assert.assertEquals(1108152157446l,
                DataIO.getSixLong(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8}, 1));
    }

    @Test public void testNextPowTwo3() {
        Assert.assertEquals(1L, DataIO.nextPowTwo(0L));
        Assert.assertEquals(1, DataIO.nextPowTwo(0));
    }

    @Test public void testWriteFully1() throws Exception {
        final Path path = Files.createTempFile("write-fully", ".tmp");
        try {
            try(final FileOutputStream os = new FileOutputStream(path.toString());
                final FileChannel channel = os.getChannel()) {
                DataIO.writeFully(channel, ByteBuffer.allocate(0));
            }
            Assert.assertEquals(0, Files.readAllBytes(path).length);
        } finally {
            Files.delete(path);
        }
    }

    @Test public void testWriteFully2() throws Exception {
        final ByteBuffer buf = ByteBuffer.allocate(3);
        buf.put(new byte[]{'a', 'b', 'c'});
        buf.flip();

        final Path path = Files.createTempFile("write-fully", ".tmp");
        try {
            try(final FileOutputStream os = new FileOutputStream(path.toString());
                final FileChannel channel = os.getChannel()) {
                DataIO.writeFully(channel, buf);
            }
            final byte[] actual = Files.readAllBytes(path);
            Assert.assertArrayEquals(buf.array(), actual);
        } finally {
            Files.delete(path);
        }
    }

    @Test public void testSkipFully() throws IOException {
        final ByteArrayInputStream in = new ByteArrayInputStream(new byte[0]);
        DataIO.skipFully(in, new byte[0].length);
        Assert.assertEquals(0, in.available());
    }

    @Test public void testParity1Set() {
        Assert.assertEquals(1, DataIO.parity1Set(1));
        Assert.assertEquals(1L, DataIO.parity1Set(1L));
    }

    @Test public void testParity1Get() {
        Assert.assertEquals(-2147483648, DataIO.parity1Get(-2147483648));
        Assert.assertEquals(-9223372036854775808L, DataIO.parity1Get(-9223372036854775808L));
    }

    @Test public void testParity1GetThrowsException() {
        thrown.expect(DBException.PointerChecksumBroken.class);
        DataIO.parity1Get(2148);
    }

    @Test public void testParity3Set() {
        Assert.assertEquals(1L, DataIO.parity3Set(0L));
    }

    @Test public void testParity3Get() {
        Assert.assertEquals(0L, DataIO.parity3Get(1L));
    }

    @Test public void testParity3GetThrowsException() {
        thrown.expect(DBException.PointerChecksumBroken.class);
        DataIO.parity3Get(2149);
    }

    @Test public void testParity4Set() {
        Assert.assertEquals(1L, DataIO.parity4Set(0L));
    }

    @Test public void testParity4Get() {
        Assert.assertEquals(0L, DataIO.parity4Get(1L));
    }

    @Test public void testParity4GetThrowsException() {
        thrown.expect(DBException.PointerChecksumBroken.class);
        DataIO.parity4Get(2149);
    }

    @Test public void testParity16Set() {
        Assert.assertEquals(24431, DataIO.parity16Set(1390L));
    }

    @Test public void testParity16Get() {
        Assert.assertEquals(0L, DataIO.parity16Get(58_577L));
    }

    @Test public void testParity16GetThrowsException() {
        thrown.expect(DBException.PointerChecksumBroken.class);
        DataIO.parity16Get(2148);
    }

    @Test public void testToHexa() {
        Assert.assertEquals("", DataIO.toHexa(new byte[]{}));
        Assert.assertEquals("98", DataIO.toHexa(new byte[]{-104}));
    }

    @Test public void testArrayPut() {
        Assert.assertArrayEquals(new Integer[] {1},
                DataIO.arrayPut(new Integer[] {}, 0, 1));
        Assert.assertArrayEquals(new Integer[] {1, 2, 3},
                DataIO.arrayPut(new Integer[] {1, 3}, 1, 2));
    }

    @Test public void testArrayDeleteObjects() {
        Assert.assertArrayEquals(new Integer[]{},
                DataIO.arrayDelete(new Integer[]{}, 0, 0));
        Assert.assertArrayEquals(new Integer[]{},
                DataIO.arrayDelete(new Integer[]{1}, 1, 1));
        Assert.assertArrayEquals(new Integer[]{1, 4},
                DataIO.arrayDelete(new Integer[]{1, 2, 3, 4}, 3, 2));
    }

    @Test public void testArrayDeleteLongs() {
        Assert.assertArrayEquals(new long[] {},
                DataIO.arrayDelete(new long[] {}, 0, 0));
        Assert.assertArrayEquals(new long[] {},
                DataIO.arrayDelete(new long[] {1l}, 1, 1));
        Assert.assertArrayEquals(new long[] {1l, 4l},
                DataIO.arrayDelete(new long[] {1l, 2l, 3l, 4l}, 3, 2));
    }

    @Test public void testIntToLong() {
        Assert.assertEquals(0L, DataIO.intToLong(0));
    }

    @Test public void testRoundUp() {
        Assert.assertEquals(0, DataIO.roundUp(0, 1));
        Assert.assertEquals(0L, DataIO.roundUp(0L, 1L));
    }

    @Test public void testRoundDown() {
        Assert.assertEquals(0, DataIO.roundDown(0, 1));
        Assert.assertEquals(0L, DataIO.roundDown(0L, 1L));
    }

    @Test public void testShift() {
        Assert.assertEquals(0, DataIO.shift(1));
    }

    @PrepareForTest({DataIO.class, System.class})
    @Test public void testIsWindows() {
        PowerMockito.mockStatic(System.class);
        PowerMockito.when(System.getProperty(anyString()))
                .thenReturn("Linux");

        Assert.assertFalse(DataIO.isWindows());
    }

    @Test public void testJVMSupportsLargeMappedFiles() {
        final String osArchOriginal = System.getProperty("os.arch");
        final String osNameOriginal = System.getProperty("os.name");

        System.setProperty("os.arch", "x86_32");
        Assert.assertFalse(DataIO.JVMSupportsLargeMappedFiles());

        System.setProperty("os.arch", "x86_64");
        System.setProperty("os.name", "Mac OS X");
        Assert.assertTrue(DataIO.JVMSupportsLargeMappedFiles());

        System.setProperty("os.name", "Windows 10");
        Assert.assertFalse(DataIO.JVMSupportsLargeMappedFiles());

        System.setProperty("os.arch", osArchOriginal);
        System.setProperty("os.name", osNameOriginal);
    }
}
