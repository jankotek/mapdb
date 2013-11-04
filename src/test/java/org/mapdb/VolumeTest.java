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


    public static class MemoryVolumeFullChunkTest extends VolumeTest{
        @Override Volume getVolume() {
            return new Volume.MemoryVol(false, 0L,true);
        }


        @Override public void testEnsureAvailable() throws Exception {
            Volume.MemoryVol v = (Volume.MemoryVol) getVolume();
            v.ensureAvailable(11);
            assertEquals(Volume.CHUNK_SIZE, v.chunks[0].capacity());
        }

    }

    public static class MemoryVolumeTest extends VolumeTest{
        @Override Volume getVolume() {
            return new Volume.MemoryVol(false, 0L,false);
        }

        @Test public void transfer(){
            long max = (long) (Volume.CHUNK_SIZE *1.5);
            Volume from = new Volume.MemoryVol(true,0,false);
            for(long i=0;i<max;i+=8){
                from.ensureAvailable(i+8);
                from.putLong(i,i);
            }
            Volume to = new Volume.MemoryVol(true,0,false);
            Volume.volumeTransfer(max,from,to);
            for(long i=0;i<max;i+=8){
                assertEquals(i, to.getLong(i));
            }
        }


    }

    public static class MappedFileFullChunkVolumeTest extends VolumeTest{
        @Override Volume getVolume() {
            return new Volume.MappedFileVol(Utils.tempDbFile(), false, 0L, true);
        }

        @Override public void testEnsureAvailable() throws Exception {
            Volume.MappedFileVol v = (Volume.MappedFileVol) getVolume();
            v.ensureAvailable(11);
            assertEquals(Volume.CHUNK_SIZE, v.chunks[0].capacity());
        }

    }

    public static class MappedFileVolumeTest extends VolumeTest{
        @Override Volume getVolume() {
            return new Volume.MappedFileVol(Utils.tempDbFile(), false, 0L, false);
        }

        /** this test fails on Windows */
        @Test public void simple_grow() throws Exception {
            File f = Utils.tempDbFile();
            f.deleteOnExit();

            final Volume.MappedFileVol vol = new Volume.MappedFileVol(f, false,0, false);
            final long max = 256 * 1024 * 1024;

            for(long i=0;i<max;i=i+8){
                vol.ensureAvailable(i+8);
                vol.putLong(i,i);
            }

            vol.sync();
            vol.close();
        }

    }


    public static class RandomAccessFullChunkVolumeTest extends VolumeTest{
        @Override Volume getVolume() {
            return new Volume.FileChannelVol(Utils.tempDbFile(), false, 0L, true);
        }

        @Override public void testEnsureAvailable() throws Exception {
            Volume.FileChannelVol v = (Volume.FileChannelVol) getVolume();
            v.ensureAvailable(11);
            assertEquals(Volume.CHUNK_SIZE, v.size);
        }

    }

    public static class RandomAccessVolumeTest extends VolumeTest{
        @Override Volume getVolume() {
            return new Volume.FileChannelVol(Utils.tempDbFile(), false, 0L, false);
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

    void testSizeLimit(Volume vol){
        assertTrue(vol.tryAvailable(999));
        assertFalse(vol.tryAvailable(1001));
        try{
            vol.ensureAvailable(1001); //throws
            fail();
        }catch (IOError e){}
    }

    @Test
    public void testMappedSizeLimit(){
        testSizeLimit(new Volume.FileChannelVol(Utils.tempDbFile(), false, 1000, false));
    }

    @Test
    public void testRAFSizeLimit(){
        testSizeLimit( new Volume.FileChannelVol(Utils.tempDbFile(), true, 1000, false));
    }


    @Test
    public void testMemorySizeLimit(){
        testSizeLimit(new Volume.MemoryVol(false, 1000L,false));
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
        assertEquals(0, Volume.CHUNK_SIZE %8);
    }

    @Test public void RAF_bytes(){
        File f = Utils.tempDbFile();
        Volume v = new Volume.FileChannelVol(f, false, 0L, false);
        v.ensureAvailable(100);
        v.putByte(1, (byte)(-120));
        assertEquals((byte)(-120), v.getByte(1));
        v.putByte(1, (byte)120);
        assertEquals((byte)120, v.getByte(1));

    }


    @Test
    public void read_beyond_end_raf_long(){
        try{
            Volume v = new Volume.FileChannelVol(Utils.tempDbFile(), false, 0L, false);
            v.getLong(1000000);
            fail();
        }catch(IOError e){
            assertTrue(e.getCause() instanceof  EOFException);
        }
    }

    @Test
    public void read_beyond_end_raf_byte(){
        try{
            Volume v = new Volume.FileChannelVol(Utils.tempDbFile(), false, 0L, false);
            v.getByte(1000000);
            fail();
        }catch(IOError e){
            assertTrue(e.getCause() instanceof  EOFException);
        }
    }

    @Test
    public void read_beyond_end_mapped_long(){
        try{
            Volume v = new Volume.MappedFileVol(Utils.tempDbFile(), false, 0L, false);
            v.ensureAvailable(10);
            v.getLong(1000000);
            fail();
        }catch(IndexOutOfBoundsException e){

        }
    }

    @Test
    public void read_beyond_end_mapped_byte(){
        try{
            Volume v = new Volume.MappedFileVol(Utils.tempDbFile(), false, 0L, false);
            v.ensureAvailable(10);
            v.getByte(1000000);
            fail();
        }catch(IndexOutOfBoundsException e){

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


    @Test public void writeBeyond2GB(){
        long max = (long) (1.5* Integer.MAX_VALUE);

        Volume v = getVolume();
        for(long i=0;i<max;i+=8){
            v.ensureAvailable(i+8);
            v.putLong(i,i);
        }
        for(long i=0;i<max;i+=8){
            assertEquals(i, v.getLong(i));
        }

    }
}
