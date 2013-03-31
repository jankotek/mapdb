package org.mapdb;


import org.junit.Test;

import java.io.EOFException;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;


public abstract class VolumeTest {


    public static class MemoryVolumeTest extends VolumeTest{
        @Override Volume getVolume() {
            return new Volume.MemoryVol(false);
        }
    }

    public static class MappedFileVolumeTest extends VolumeTest{
        @Override Volume getVolume() {
            return new Volume.MappedFileVol(Utils.tempDbFile(), false);
        }
    }

    public static class RandomAccessVolumeTest extends VolumeTest{
        @Override Volume getVolume() {
            return new Volume.RandomAccessFileVol(Utils.tempDbFile(), false);
        }
    }



    abstract Volume getVolume();

    final int beyondInc = (int) (1e7);

    Volume b = getVolume();
    {
        b.ensureAvailable(32*1024);
    }

    @Test
    public void testEnsureAvailable() throws Exception {
        try{
            b.putLong(beyondInc,111L);
            if(b.isSliced())
                fail("Should throw exception");
        }catch(Exception e){
            //ignore
        }
        b.ensureAvailable(beyondInc+8);
        b.putLong(beyondInc,111L);
        assertEquals(111L, b.getLong(beyondInc));

    }

    @Test
    public void testPutLong() throws Exception {
        b.putLong(1000, 111L);
        assertEquals(111L, b.getLong(1000));
    }

    @Test
    public void testPutUnsignedByte() throws Exception {
        b.putUnsignedByte(1000, (byte) 11);
        assertEquals(11, b.getUnsignedByte(1000));
        b.putUnsignedByte(1000, (byte) 126);
        assertEquals(126, b.getUnsignedByte(1000));
        b.putUnsignedByte(1000, (byte) 130);
        assertEquals(130, b.getUnsignedByte(1000));
        b.putUnsignedByte(1000, (byte) 255);
        assertEquals(255, b.getUnsignedByte(1000));
        b.putUnsignedByte(1000, (byte) 0);
        assertEquals(0, b.getUnsignedByte(1000));
    }

    @Test
    public void testPutData() throws Exception {
        DataOutput2 out = new DataOutput2();
        out.writeInt(11);
        out.writeLong(1111L);
        b.putData(111L, out.buf, 0, out.pos);

        DataInput2 in = b.getDataInput(111L, out.pos);
        assertEquals(11, in.readInt());
        assertEquals(1111L, in.readLong());
    }

    @Test public void unsignedShort() throws IOException {
        b.putUnsignedShort(1000, 0);
        assertEquals(0, b.getUnsignedShort(1000));
        b.putUnsignedShort(1000, 100);
        assertEquals(100, b.getUnsignedShort(1000));
        b.putUnsignedShort(1000, 32000);
        assertEquals(32000, b.getUnsignedShort(1000));

        b.putUnsignedShort(1000, 35000);
        assertEquals(35000, b.getUnsignedShort(1000));

        b.putUnsignedShort(1000, 65000);
        assertEquals(65000, b.getUnsignedShort(1000));

    }


    @Test public void testConstants(){
        assertEquals(0, Volume.BUF_SIZE% Volume.MappedFileVol.BUF_SIZE_INC);
        assertEquals(0, Volume.BUF_SIZE%8);
        assertEquals(0, Volume.MappedFileVol.BUF_SIZE_INC%8);
        assertTrue(Volume.MappedFileVol.BUF_SIZE_INC> StoreDirect.MAX_REC_SIZE);
    }

    @Test public void RAF_bytes(){
        File f = Utils.tempDbFile();
        Volume v = new Volume.RandomAccessFileVol(f, false);
        v.ensureAvailable(100);
        v.putByte(1, (byte)(-120));
        assertEquals((byte)(-120), v.getByte(1));
        v.putByte(1, (byte)120);
        assertEquals((byte)120, v.getByte(1));

    }


    @Test
    public void read_beyond_end_raf_long(){
        try{
            Volume v = new Volume.RandomAccessFileVol(Utils.tempDbFile(), false);
            v.getLong(1000000);
            fail();
        }catch(IOError e){
            assertTrue(e.getCause() instanceof  EOFException);
        }
    }

    @Test
    public void read_beyond_end_raf_byte(){
        try{
            Volume v = new Volume.RandomAccessFileVol(Utils.tempDbFile(), false);
            v.getByte(1000000);
            fail();
        }catch(IOError e){
            assertTrue(e.getCause() instanceof  EOFException);
        }
    }

    @Test
    public void read_beyond_end_mapped_long(){
        try{
            Volume v = new Volume.MappedFileVol(Utils.tempDbFile(), false);
            v.ensureAvailable(10);
            v.getLong(1000000);
            fail();
        }catch(IOError e){
            assertTrue(e.getCause() instanceof  EOFException);
        }
    }

    @Test
    public void read_beyond_end_mapped_byte(){
        try{
            Volume v = new Volume.MappedFileVol(Utils.tempDbFile(), false);
            v.ensureAvailable(10);
            v.getByte(1000000);
            fail();
        }catch(IOError e){
            assertTrue(e.getCause() instanceof  EOFException);
        }
    }


    @Test public void concurrent_write() throws InterruptedException {
        final Volume v = getVolume();
        final long max = 1024*8;
        v.ensureAvailable(max);
        ExecutorService s = Executors.newCachedThreadPool();
        for(int i=0;i<8;i++){
            final int threadNum = i;
            s.execute(new Runnable() {
                @Override public void run() {
                    for(long offset=threadNum*4;offset<max;offset+=4*8){
                        v.putInt(offset,111);
                        if(offset!=0)
                            if(v.getInt(offset-4)==-111)throw new InternalError();
                    }
                }
            });
        }
        s.shutdown();
        s.awaitTermination(111, TimeUnit.DAYS);
        for(long offset=0;offset<max;offset+=4){
            assertEquals("offset:"+offset, 111,v.getInt(offset));
        }
    }


    @Test public void sixLong(){
        long[] d = {
                1,2,3, 665,  0, 199012, 0x222222, 0x0000FFFFFFFFFFFFL
        };

        Volume v = getVolume();
        v.ensureAvailable(16+6);
        for(long l:d){
            v.putSixLong(16, l);
            assertEquals(l, v.getSixLong(16));
        }
    }

}
