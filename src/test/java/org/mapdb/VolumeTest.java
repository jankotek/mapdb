package org.mapdb;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

public class VolumeTest {

    static final int scale = TT.scale();
    static final long sub = (long) Math.pow(10, 5 + scale);

    public static final Fun.Function1<Volume, String>[] VOL_FABS = new Fun.Function1[]{

            new Fun.Function1<Volume, String>() {
                @Override
                public Volume run(String file) {
                    return new Volume.ByteArrayVol(CC.VOLUME_PAGE_SHIFT,0L);
                }
            },
            new Fun.Function1<Volume, String>() {
                @Override
                public Volume run(String file) {
                    return new Volume.SingleByteArrayVol((int) 4e7);
                }
            },
            new Fun.Function1<Volume, String>() {
                @Override
                public Volume run(String file) {
                    return new Volume.MemoryVol(true, CC.VOLUME_PAGE_SHIFT, false,0L);
                }
            },
            new Fun.Function1<Volume, String>() {
                @Override
                public Volume run(String file) {
                    return new Volume.MemoryVol(false, CC.VOLUME_PAGE_SHIFT, false,0L);
                }
            },
            new Fun.Function1<Volume, String>() {
                @Override
                public Volume run(String file) {
                    return Volume.UNSAFE_VOL_FACTORY.makeVolume(null, false, false, CC.VOLUME_PAGE_SHIFT, 0, false);
                }
            },
            new Fun.Function1<Volume, String>() {
                @Override
                public Volume run(String file) {
                    return new Volume.FileChannelVol(new File(file), false, false, CC.VOLUME_PAGE_SHIFT,0L);
                }
            },
            new Fun.Function1<Volume, String>() {
                @Override
                public Volume run(String file) {
                    return new Volume.RandomAccessFileVol(new File(file), false, false,0L);
                }
            },
            new Fun.Function1<Volume, String>() {
                @Override
                public Volume run(String file) {
                    return new Volume.MappedFileVol(new File(file), false, false, CC.VOLUME_PAGE_SHIFT, false, 0L,false);
                }
            },
            new Fun.Function1<Volume, String>() {
                @Override
                public Volume run(String file) {
                    return new Volume.MappedFileVolSingle(new File(file), false, false, (long) 4e7, false);
                }
            },
            new Fun.Function1<Volume, String>() {
                @Override
                public Volume run(String file) {
                    return new Volume.MemoryVolSingle(false, (long) 4e7, false);
                }
            },
    };


    @RunWith(Parameterized.class)
    public static class IndividualTest {
        final Fun.Function1<Volume, String> fab;

        public IndividualTest(Fun.Function1<Volume, String> fab) {
            this.fab = fab;
        }

        @Parameterized.Parameters
        public static Iterable params() throws IOException {
            List ret = new ArrayList();
            if (TT.shortTest()){
                ret.add(new Object[]{VOL_FABS[0]});
                return ret;
            }

            for (Object o : VOL_FABS) {
                ret.add(new Object[]{o});
            }

            return ret;
        }

        @Test
        public void testPackLongBidi() throws Exception {
            Volume v = fab.run(TT.tempDbFile().getPath());

            v.ensureAvailable(10000);

            long max = (long) 1e14;
            for (long i = 0; i < max; i = i + 1 + i / sub) {
                v.clear(0, 20);
                long size = v.putLongPackBidi(10, i);
                assertTrue(i > 100000 || size < 6);

                assertEquals(i | (size << 60), v.getLongPackBidi(10));
                assertEquals(i | (size << 60), v.getLongPackBidiReverse(10 + size,10));
            }
            v.close();
        }


        @Test
        public void testPackLong() throws Exception {
            Volume v = fab.run(TT.tempDbFile().getPath());

            v.ensureAvailable(10000);

            for (long i = 0; i < DataIO.PACK_LONG_RESULT_MASK; i = i + 1 + i / 1000) {
                v.clear(0, 20);
                long size = v.putPackedLong(10, i);
                assertTrue(i > 100000 || size < 6);

                assertEquals(i | (size << 60), v.getPackedLong(10));
            }
            v.close();
        }


        @Test
        public void overlap() throws Throwable {
            Volume v = fab.run(TT.tempDbFile().getPath());

            putGetOverlap(v, 100, 1000);
            putGetOverlap(v, StoreDirect.PAGE_SIZE - 500, 1000);
            putGetOverlap(v, (long) 2e7 + 2000, (int) 1e7);
            putGetOverlapUnalligned(v);

            v.close();

        }

        @Test public void hash(){
            byte[] b = new byte[11111];
            new Random().nextBytes(b);
            Volume v = fab.run(TT.tempDbFile().getPath());
            v.ensureAvailable(b.length);
            v.putData(0,b,0,b.length);

            assertEquals(DataIO.hash(b,0,b.length,11), v.hash(0,b.length,11));

            v.close();
        }

        @Test public void clear(){
            long offset = 7339936;
            long size = 96;
            Volume v = fab.run(TT.tempDbFile().getPath());
            v.ensureAvailable(offset + 10000);
            for(long o=0;o<offset+10000;o++){
                v.putUnsignedByte(o,11);
            }
            v.clear(offset,offset+size);

            for(long o=0;o<offset+10000;o++){
                int b = v.getUnsignedByte(o);
                int expected = 11;
                if(o>=offset && o<offset+size)
                    expected=0;
                assertEquals(expected,b);
            }
        }

        void putGetOverlap(Volume vol, long offset, int size) throws IOException {
            byte[] b = TT.randomByteArray(size);

            vol.ensureAvailable(offset + size);
            vol.putDataOverlap(offset, b, 0, b.length);

            byte[] b2 = new byte[size];
            vol.getDataInputOverlap(offset, size).readFully(b2, 0, size);

            assertTrue(Serializer.BYTE_ARRAY.equals(b, b2));
        }


        void putGetOverlapUnalligned(Volume vol) throws IOException {
            int size = (int) 1e7;
            long offset = (long) (2e6 + 2000);
            vol.ensureAvailable(offset + size);

            byte[] b = TT.randomByteArray(size);

            byte[] b2 = new byte[size + 2000];

            System.arraycopy(b, 0, b2, 1000, size);

            vol.putDataOverlap(offset, b2, 1000, size);

            byte[] b3 = new byte[size + 200];
            vol.getDataInputOverlap(offset, size).readFully(b3, 100, size);


            for (int i = 0; i < size; i++) {
                assertEquals(b2[i + 1000], b3[i + 100]);
            }
        }
    }


    @RunWith(Parameterized.class)
    public static class DoubleTest {
        final Fun.Function1<Volume, String> fab1;
        final Fun.Function1<Volume, String> fab2;

        public DoubleTest(Fun.Function1<Volume, String> fab1, Fun.Function1<Volume, String> fab2) {
            this.fab1 = fab1;
            this.fab2 = fab2;
        }

        @Parameterized.Parameters
        public static Iterable params() throws IOException {
            List ret = new ArrayList();
            if (TT.shortTest()) {
                ret.add(new Object[]{
                    new Fun.Function1<Volume, String>() {
                        @Override
                        public Volume run(String file) {
                            return new Volume.ByteArrayVol(CC.VOLUME_PAGE_SHIFT,0L);
                        }
                    },
                    new Fun.Function1<Volume, String>() {
                        @Override
                        public Volume run(String file) {
                            return new Volume.FileChannelVol(new File(file), false, false, CC.VOLUME_PAGE_SHIFT,0L);
                        }
                    }
                });
                return ret;
            }
            for (Object o : VOL_FABS) {
                for (Object o2 : VOL_FABS) {
                    ret.add(new Object[]{o, o2});
                }
            }

            return ret;
        }

        @Test
        public void unsignedShort_compatible() {
            Volume v1 = fab1.run(TT.tempDbFile().getPath());
            Volume v2 = fab2.run(TT.tempDbFile().getPath());

            v1.ensureAvailable(16);
            v2.ensureAvailable(16);
            byte[] b = new byte[8];

            for (int i = Character.MIN_VALUE; i <= Character.MAX_VALUE; i++) {
                v1.putUnsignedShort(7, i);
                v1.getData(7, b, 0, 8);
                v2.putData(7, b, 0, 8);
                assertEquals(i, v2.getUnsignedShort(7));
            }

            v1.close();
            v2.close();
        }


        @Test
        public void unsignedByte_compatible() {
            Volume v1 = fab1.run(TT.tempDbFile().getPath());
            Volume v2 = fab2.run(TT.tempDbFile().getPath());

            v1.ensureAvailable(16);
            v2.ensureAvailable(16);
            byte[] b = new byte[8];

            for (int i = 0; i <= 255; i++) {
                v1.putUnsignedByte(7, i);
                v1.getData(7, b, 0, 8);
                v2.putData(7, b, 0, 8);
                assertEquals(i, v2.getUnsignedByte(7));
            }

            v1.close();
            v2.close();
        }


        @Test
        public void long_compatible() {
            Volume v1 = fab1.run(TT.tempDbFile().getPath());
            Volume v2 = fab2.run(TT.tempDbFile().getPath());

            v1.ensureAvailable(16);
            v2.ensureAvailable(16);
            byte[] b = new byte[8];

            for (long i : new long[]{1L, 2L, Integer.MAX_VALUE, Integer.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE,
                    -1, 0x982e923e8989229L, -2338998239922323233L,
                    0xFFF8FFL, -0xFFF8FFL, 0xFFL, -0xFFL,
                    0xFFFFFFFFFF0000L, -0xFFFFFFFFFF0000L}) {
                v1.putLong(7, i);
                v1.getData(7, b, 0, 8);
                v2.putData(7, b, 0, 8);
                assertEquals(i, v2.getLong(7));
            }

            v1.close();
            v2.close();
        }


        @Test
        public void long_pack_bidi() {
            Volume v1 = fab1.run(TT.tempDbFile().getPath());
            Volume v2 = fab2.run(TT.tempDbFile().getPath());

            v1.ensureAvailable(16);
            v2.ensureAvailable(16);
            byte[] b = new byte[9];

            for (long i = 0; i > 0; i = i + 1 + i / 1000) {
                v1.putLongPackBidi(7, i);
                v1.getData(7, b, 0, 8);
                v2.putData(7, b, 0, 8);
                assertEquals(i, v2.getLongPackBidi(7));
            }

            v1.close();
            v2.close();
        }

        @Test
        public void long_pack() {
            Volume v1 = fab1.run(TT.tempDbFile().getPath());
            Volume v2 = fab2.run(TT.tempDbFile().getPath());

            v1.ensureAvailable(21);
            v2.ensureAvailable(20);
            byte[] b = new byte[12];

            for (long i = 0; i < DataIO.PACK_LONG_RESULT_MASK; i = i + 1 + i / sub) {
                long len = v1.putPackedLong(7, i);
                v1.getData(7, b, 0, 12);
                v2.putData(7, b, 0, 12);
                assertTrue(len <= 10);
                assertEquals((len << 60) | i, v2.getPackedLong(7));
            }

            v1.close();
            v2.close();
        }


        @Test
        public void long_six_compatible() {
            Volume v1 = fab1.run(TT.tempDbFile().getPath());
            Volume v2 = fab2.run(TT.tempDbFile().getPath());

            v1.ensureAvailable(16);
            v2.ensureAvailable(16);
            byte[] b = new byte[9];

            for (long i = 0; i >> 48 == 0; i = i + 1 + i / sub) {
                v1.putSixLong(7, i);
                v1.getData(7, b, 0, 8);
                v2.putData(7, b, 0, 8);
                assertEquals(i, v2.getSixLong(7));
            }

            v1.close();
            v2.close();
        }

        @Test
        public void int_compatible() {
            Volume v1 = fab1.run(TT.tempDbFile().getPath());
            Volume v2 = fab2.run(TT.tempDbFile().getPath());

            v1.ensureAvailable(16);
            v2.ensureAvailable(16);
            byte[] b = new byte[8];

            for (int i : new int[]{1, 2, Integer.MAX_VALUE, Integer.MIN_VALUE,
                    -1, 0x982e9229, -233899233,
                    0xFFF8FF, -0xFFF8FF, 0xFF, -0xFF,
                    0xFFFF000, -0xFFFFF00}) {
                v1.putInt(7, i);
                v1.getData(7, b, 0, 8);
                v2.putData(7, b, 0, 8);
                assertEquals(i, v2.getInt(7));
            }

            v1.close();
            v2.close();
        }


        @Test
        public void byte_compatible() {
            Volume v1 = fab1.run(TT.tempDbFile().getPath());
            Volume v2 = fab2.run(TT.tempDbFile().getPath());

            v1.ensureAvailable(16);
            v2.ensureAvailable(16);
            byte[] b = new byte[8];

            for (byte i = Byte.MIN_VALUE; i < Byte.MAX_VALUE - 1; i++) {
                v1.putByte(7, i);
                v1.getData(7, b, 0, 8);
                v2.putData(7, b, 0, 8);
                assertEquals(i, v2.getByte(7));
            }


            for (int i = 0; i < 256; i++) {
                v1.putUnsignedByte(7, i);
                v1.getData(7, b, 0, 8);
                v2.putData(7, b, 0, 8);
                assertEquals(i, v2.getUnsignedByte(7));
            }


            v1.close();
            v2.close();
        }
    }



    @Test public void direct_bb_overallocate(){
        if(TT.shortTest())
            return;

        Volume vol = new Volume.MemoryVol(true, CC.VOLUME_PAGE_SHIFT,false, 0L);
        try {
            vol.ensureAvailable((long) 1e10);
        }catch(DBException.OutOfMemory e){
            assertTrue(e.getMessage().contains("-XX:MaxDirectMemorySize"));
        }
        vol.close();
    }

    @Test public void byte_overallocate(){
        if(TT.shortTest())
            return;

        Volume vol = new Volume.ByteArrayVol(CC.VOLUME_PAGE_SHIFT,0L);
        try {
            vol.ensureAvailable((long) 1e10);
        }catch(DBException.OutOfMemory e){
            assertFalse(e.getMessage().contains("-XX:MaxDirectMemorySize"));
        }
        vol.close();
    }

    @Test
    public void mmap_init_size() throws IOException {
        //test if mmaping file size repeatably increases file
        File f = File.createTempFile("mapdbTest","mapdb");

        long chunkSize = 1<<CC.VOLUME_PAGE_SHIFT;
        long add = 100000;

        //open file channel and write some size
        RandomAccessFile raf = new RandomAccessFile(f,"rw");
        raf.seek(add);
        raf.writeInt(11);
        raf.close();

        //open mmap file, size should grow to multiple of chunk size
        Volume.MappedFileVol m = new Volume.MappedFileVol(f, false,false, CC.VOLUME_PAGE_SHIFT,true, 0L, false);
        assertEquals(1, m.slices.length);
        m.sync();
        m.close();
        assertEquals(chunkSize, f.length());

        //open mmap file, size should grow to multiple of chunk size
        m = new Volume.MappedFileVol(f, false,false, CC.VOLUME_PAGE_SHIFT,true, 0L, false);
        assertEquals(1, m.slices.length);
        m.ensureAvailable(add + 4);
        assertEquals(11, m.getInt(add));
        m.sync();
        m.close();
        assertEquals(chunkSize, f.length());

        raf = new RandomAccessFile(f,"rw");
        raf.seek(chunkSize + add);
        raf.writeInt(11);
        raf.close();

        m = new Volume.MappedFileVol(f, false,false, CC.VOLUME_PAGE_SHIFT,true, 0L, false);
        assertEquals(2, m.slices.length);
        m.sync();
        m.ensureAvailable(chunkSize + add + 4);
        assertEquals(chunkSize * 2, f.length());
        assertEquals(11, m.getInt(chunkSize + add));
        m.sync();
        m.close();
        assertEquals(chunkSize * 2, f.length());

        m = new Volume.MappedFileVol(f, false,false, CC.VOLUME_PAGE_SHIFT,true, 0L, false) ;
        m.sync();
        assertEquals(chunkSize * 2, f.length());
        m.ensureAvailable(chunkSize + add + 4);
        assertEquals(11, m.getInt(chunkSize + add));
        m.sync();
        assertEquals(chunkSize * 2, f.length());

        m.ensureAvailable(chunkSize * 2 + add + 4);
        m.putInt(chunkSize * 2 + add, 11);
        assertEquals(11, m.getInt(chunkSize * 2 + add));
        m.sync();
        assertEquals(3, m.slices.length);
        assertEquals(chunkSize * 3, f.length());

        m.close();
        f.delete();
    }

    @Test public void small_mmap_file_single() throws IOException {
        File f = File.createTempFile("mapdbTest","mapdb");
        RandomAccessFile raf = new RandomAccessFile(f,"rw");
        int len = 10000000;
        raf.setLength(len);
        raf.close();
        assertEquals(len, f.length());

        Volume v = Volume.MappedFileVol.FACTORY.makeVolume(f.getPath(), true);

        assertTrue(v instanceof Volume.MappedFileVolSingle);
        ByteBuffer b = ((Volume.MappedFileVolSingle)v).buffer;
        assertEquals(len, b.limit());
    }

    @Test public void single_mmap_grow() throws IOException {
        File f = File.createTempFile("mapdbTest","mapdb");
        RandomAccessFile raf = new RandomAccessFile(f,"rw");
        raf.seek(0);
        raf.writeLong(112314123);
        raf.close();
        assertEquals(8, f.length());

        Volume.MappedFileVolSingle v = new Volume.MappedFileVolSingle(f,false,false, 1000,false);
        assertEquals(1000, f.length());
        assertEquals(112314123, v.getLong(0));
        v.close();
    }

    @Test
    public void lock_double_open() throws IOException {
        File f = File.createTempFile("mapdbTest","mapdb");
        Volume.RandomAccessFileVol v = new Volume.RandomAccessFileVol(f,false,false,0L);
        v.ensureAvailable(8);
        v.putLong(0, 111L);

        //second open should fail, since locks are enabled
        assertTrue(v.getFileLocked());

        try {
            Volume.RandomAccessFileVol v2 = new Volume.RandomAccessFileVol(f, false, false,0L);
            fail();
        }catch(DBException.FileLocked l){
            //ignored
        }
        v.close();
        Volume.RandomAccessFileVol v2 = new Volume.RandomAccessFileVol(f, false, false,0L);

        assertEquals(111L, v2.getLong(0));
    }

    @Test public void initSize(){
        if(TT.shortTest())
            return;

        Volume.VolumeFactory[] factories = new Volume.VolumeFactory[]{
                CC.DEFAULT_FILE_VOLUME_FACTORY,
                CC.DEFAULT_MEMORY_VOLUME_FACTORY,
                Volume.ByteArrayVol.FACTORY,
                Volume.FileChannelVol.FACTORY,
                Volume.MappedFileVol.FACTORY,
                Volume.MappedFileVol.FACTORY,
                Volume.MemoryVol.FACTORY,
                Volume.MemoryVol.FACTORY_WITH_CLEANER_HACK,
                Volume.RandomAccessFileVol.FACTORY,
                Volume.SingleByteArrayVol.FACTORY,
                Volume.MappedFileVolSingle.FACTORY,
                Volume.MappedFileVolSingle.FACTORY_WITH_CLEANER_HACK,
                Volume.UNSAFE_VOL_FACTORY,
        };

        for(Volume.VolumeFactory fac:factories){
            File f = TT.tempDbFile();
            long initSize = 20*1024*1024;
            Volume vol = fac.makeVolume(f.getPath(),false,true,CC.VOLUME_PAGE_SHIFT,initSize,false);
            assertEquals(vol.getClass().getName(), initSize, vol.length());
            vol.close();
            f.delete();
        }
    }

    @Test public void hash(){
        Random r = new Random();
        for(int i=0;i<100;i++){
            int len = 100+r.nextInt(1999);
            byte[] b = new byte[len];
            r.nextBytes(b);

            Volume vol = new Volume.SingleByteArrayVol(len);
            vol.putData(0, b,0,b.length);

            assertEquals(
                    DataIO.hash(b,0,b.length,0),
                    vol.hash(0,b.length,0)
                    );

        }
    }

    @Test public void clearOverlap(){
        if(TT.scale()<100)
            return;

        Volume.ByteArrayVol v = new Volume.ByteArrayVol();
        v.ensureAvailable(5 * 1024 * 1024);
        long vLength = v.length();
        byte[] ones = new byte[1024];
        Arrays.fill(ones, (byte) 1);

        for(long size : new long[]{100, 1024*1024, 3*1024*1024, 3*1024*1024+6000}){
            for(long startPos=0;startPos<vLength-size;startPos++){
                //fill with ones
                for(long pos=0;pos<vLength;pos+=ones.length){
                    v.putData(pos,ones,0,ones.length);
                }

                //clear section of the volume
                v.clearOverlap(startPos, startPos+size);
                //ensure zeroes
                v.assertZeroes(startPos,startPos+size);

                //ensure ones before
                for(long pos=0;pos<startPos;pos++){
                    if(v.getByte(pos)!=1)
                        throw new AssertionError();
                }

                //ensure ones after
                for(long pos=startPos+size;pos<vLength;pos++){
                    if(v.getByte(pos)!=1)
                        throw new AssertionError();
                }
            }
        }
    }

    @Test
    public void testClearOverlap2(){
        clearOverlap(0, 1000);
        clearOverlap(0,10000000);
        clearOverlap(100,10000000);
        clearOverlap(StoreDirect.PAGE_SIZE,10000000);
        clearOverlap(StoreDirect.PAGE_SIZE-1,StoreDirect.PAGE_SIZE*3);
        clearOverlap(StoreDirect.PAGE_SIZE+1,StoreDirect.PAGE_SIZE*3);
    }

    void clearOverlap(long startPos, long size){
        Volume.ByteArrayVol v = new Volume.ByteArrayVol();
        v.ensureAvailable(startPos+size+10000);
        byte[] ones = new byte[1024];
        Arrays.fill(ones, (byte) 1);
        long vLength = v.length();

        //fill with ones
        for(long pos=0;pos<vLength;pos+=ones.length){
            v.putData(pos,ones,0,ones.length);
        }

        //clear section of the volume
        v.clearOverlap(startPos, startPos+size);
        //ensure zeroes
        v.assertZeroes(startPos,startPos+size);

        //ensure ones before
        for(long pos=0;pos<startPos;pos++){
            if(v.getByte(pos)!=1)
                throw new AssertionError();
        }

        //ensure ones after
        for(long pos=startPos+size;pos<vLength;pos++){
            if(v.getByte(pos)!=1)
                throw new AssertionError();
        }
    }

}
