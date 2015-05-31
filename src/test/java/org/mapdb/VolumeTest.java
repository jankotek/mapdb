package org.mapdb;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class VolumeTest {

    public static final Fun.Function1<Volume,String>[] VOL_FABS = new Fun.Function1[] {

                    new Fun.Function1<Volume,String>() {
                        @Override
                        public Volume run(String file) {
                            return new Volume.ByteArrayVol(CC.VOLUME_PAGE_SHIFT);
                        }
                    },
                    new Fun.Function1<Volume,String>() {
                        @Override
                        public Volume run(String file) {
                            return new Volume.SingleByteArrayVol((int) 4e7);
                        }
                    },
                    new Fun.Function1<Volume,String>() {
                        @Override
                        public Volume run(String file) {
                            return new Volume.MemoryVol(true, CC.VOLUME_PAGE_SHIFT);
                        }
                    },
                    new Fun.Function1<Volume,String>() {
                        @Override
                        public Volume run(String file) {
                            return new Volume.MemoryVol(false, CC.VOLUME_PAGE_SHIFT);
                        }
                    },
                    new Fun.Function1<Volume,String>() {
                        @Override
                        public Volume run(String file) {
                            return Volume.UNSAFE_VOL_FACTORY.makeVolume(null, false, CC.VOLUME_PAGE_SHIFT, 0, false);
                        }
                    },
                    new Fun.Function1<Volume,String>() {
                        @Override
                        public Volume run(String file) {
                            return new Volume.FileChannelVol(new File(file), false, CC.VOLUME_PAGE_SHIFT);
                        }
                    },
                    new Fun.Function1<Volume,String>() {
                        @Override
                        public Volume run(String file) {
                            return new Volume.RandomAccessFileVol(new File(file), false);
                        }
                    },
                    new Fun.Function1<Volume,String>() {
                        @Override
                        public Volume run(String file) {
                            return new Volume.MappedFileVol(new File(file), false, CC.VOLUME_PAGE_SHIFT);
                        }
                    }
    };

    @Test
    public void all() throws Throwable {
        System.out.println("Run volume tests. Free space: "+File.createTempFile("mapdb","mapdb").getFreeSpace());


        for (Fun.Function1<Volume,String> fab1 : VOL_FABS) {

            Volume v = fab1.run(UtilsTest.tempDbFile().getPath());
            System.out.println(" "+v);
            testPackLongBidi(v);

            putGetOverlap(fab1.run(UtilsTest.tempDbFile().getPath()), 100, 1000);
            putGetOverlap(fab1.run(UtilsTest.tempDbFile().getPath()), StoreDirect.PAGE_SIZE - 500, 1000);
            putGetOverlap(fab1.run(UtilsTest.tempDbFile().getPath()), (long) 2e7 + 2000, (int) 1e7);
            putGetOverlapUnalligned(fab1.run(UtilsTest.tempDbFile().getPath()));

            for (Fun.Function1<Volume,String> fab2 : VOL_FABS) try{
                long_compatible(fab1.run(UtilsTest.tempDbFile().getPath()), fab2.run(UtilsTest.tempDbFile().getPath()));
                long_six_compatible(fab1.run(UtilsTest.tempDbFile().getPath()), fab2.run(UtilsTest.tempDbFile().getPath()));
                long_pack_bidi(fab1.run(UtilsTest.tempDbFile().getPath()), fab2.run(UtilsTest.tempDbFile().getPath()));
                int_compatible(fab1.run(UtilsTest.tempDbFile().getPath()), fab2.run(UtilsTest.tempDbFile().getPath()));
                byte_compatible(fab1.run(UtilsTest.tempDbFile().getPath()), fab2.run(UtilsTest.tempDbFile().getPath()));
                unsignedShort_compatible(fab1.run(UtilsTest.tempDbFile().getPath()), fab2.run(UtilsTest.tempDbFile().getPath()));
                unsignedByte_compatible(fab1.run(UtilsTest.tempDbFile().getPath()), fab2.run(UtilsTest.tempDbFile().getPath()));
            }catch(Throwable e){
                System.err.println("test failed: \n"+
                        fab1.run(UtilsTest.tempDbFile().getPath()).getClass().getName()+"\n"+
                        fab2.run(UtilsTest.tempDbFile().getPath()).getClass().getName());
                throw e;
            }
        }
    }

    void unsignedShort_compatible(Volume v1, Volume v2) {
        v1.ensureAvailable(16);
        v2.ensureAvailable(16);
        byte[] b = new byte[8];

        for (int i =Character.MIN_VALUE;i<=Character.MAX_VALUE; i++) {
            v1.putUnsignedShort(7,i);
            v1.getData(7, b, 0, 8);
            v2.putData(7, b, 0, 8);
            assertEquals(i, v2.getUnsignedShort(7));
        }

        v1.close();
        v2.close();
    }


    void unsignedByte_compatible(Volume v1, Volume v2) {
        v1.ensureAvailable(16);
        v2.ensureAvailable(16);
        byte[] b = new byte[8];

        for (int i =0;i<=255; i++) {
            v1.putUnsignedByte(7, i);
            v1.getData(7, b, 0, 8);
            v2.putData(7, b, 0, 8);
            assertEquals(i, v2.getUnsignedByte(7));
        }

        v1.close();
        v2.close();
    }


    void testPackLongBidi(Volume v) throws Exception {
        v.ensureAvailable(10000);

        long max = (long) 1e14;
        for (long i = 0; i < max; i = i + 1 + i / 1000) {
            v.clear(0, 20);
            long size = v.putLongPackBidi(10, i);
            assertTrue(i > 100000 || size < 6);

            assertEquals(i | (size << 56), v.getLongPackBidi(10));
            assertEquals(i | (size << 56), v.getLongPackBidiReverse(10 + size));
        }
        v.close();
    }

    void long_compatible(Volume v1, Volume v2) {
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


    void long_pack_bidi(Volume v1, Volume v2) {
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


    void long_six_compatible(Volume v1, Volume v2) {
        v1.ensureAvailable(16);
        v2.ensureAvailable(16);
        byte[] b = new byte[9];

        for (long i = 0; i >> 48 == 0; i = i + 1 + i / 1000) {
            v1.putSixLong(7, i);
            v1.getData(7, b, 0, 8);
            v2.putData(7, b, 0, 8);
            assertEquals(i, v2.getSixLong(7));
        }

        v1.close();
        v2.close();
    }

    void int_compatible(Volume v1, Volume v2) {
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


    void byte_compatible(Volume v1, Volume v2) {
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


    void putGetOverlap(Volume vol, long offset, int size) throws IOException {
        byte[] b = UtilsTest.randomByteArray(size);

        vol.ensureAvailable(offset+size);
        vol.putDataOverlap(offset, b, 0, b.length);

        byte[] b2 = new byte[size];
        vol.getDataInputOverlap(offset, size).readFully(b2, 0, size);

        assertTrue(Serializer.BYTE_ARRAY.equals(b, b2));
        vol.close();
    }



    void putGetOverlapUnalligned(Volume vol) throws IOException {
        int size = (int) 1e7;
        long offset = (long) (2e6 + 2000);
        vol.ensureAvailable(offset+size);

        byte[] b = UtilsTest.randomByteArray(size);

        byte[] b2 = new byte[size + 2000];

        System.arraycopy(b, 0, b2, 1000, size);

        vol.putDataOverlap(offset, b2, 1000, size);

        byte[] b3 = new byte[size + 200];
        vol.getDataInputOverlap(offset, size).readFully(b3, 100, size);


        for (int i = 0; i < size; i++) {
            assertEquals(b2[i + 1000], b3[i + 100]);
        }
        vol.close();
    }

    /* TODO move this to burn tests
    @Test public void direct_bb_overallocate(){
        Volume vol = new Volume.MemoryVol(true, CC.VOLUME_PAGE_SHIFT);
        try {
            vol.ensureAvailable((long) 1e10);
        }catch(DBException.OutOfMemory e){
            assertTrue(e.getMessage().contains("-XX:MaxDirectMemorySize"));
        }
        vol.close();
    }

    @Test public void byte_overallocate(){
        Volume vol = new Volume.ByteArrayVol(CC.VOLUME_PAGE_SHIFT);
        try {
            vol.ensureAvailable((long) 1e10);
        }catch(DBException.OutOfMemory e){
            assertFalse(e.getMessage().contains("-XX:MaxDirectMemorySize"));
        }
        vol.close();
    }
    */

}
