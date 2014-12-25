package org.mapdb;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mapdb.DataIO.packLongBidi;
import static org.mapdb.DataIO.unpackLongBidi;
import static org.mapdb.DataIO.unpackLongBidiReverse;

public class VolumeTest {

    @Test
    public void interrupt_raf_file_exception() throws IOException, InterruptedException {
        // when IO thread is interrupted, channel gets closed and it throws  ClosedByInterruptException
        final Volume.FileChannelVol v = new Volume.FileChannelVol(File.createTempFile("mapdb", "mapdb"), false, 0, 0);
        final AtomicReference ref = new AtomicReference();
        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    long pos = 0;
                    while (true) {
                        v.ensureAvailable(pos++);
                        v.putByte(pos - 1, (byte) 1);
                    }
                }catch(Throwable e){
                    ref.set(e);
                }
            }
        };
        t.start();
        Thread.sleep(100);
        t.interrupt();
        Thread.sleep(100);
        assertTrue(ref.get() instanceof DBException.VolumeClosedByInterrupt);
        //now channel should be closed
        assertFalse(v.channel.isOpen());
        try {
            v.putLong(0, 1000);
            fail();
        }catch(DBException e){
            assertTrue(e instanceof DBException.VolumeClosed);
        }
    }


    @Test
    public void testPackLongBidi() throws Exception {
        Volume v = new Volume.ByteArrayVol(CC.VOLUME_PAGE_SHIFT);
        v.ensureAvailable(10000);

        long max = (long) 1e14;
        for(long i=0;i<max;i=i+1 +i/100000){
            v.clear(0, 20);
            long size = v.putLongPackBidi(10,i);
            assertTrue(i>100000 || size<6);

            assertEquals(i | (size<<56), v.getLongPackBidi(10));
            assertEquals(i | (size<<56), v.getLongPackBidiReverse(10+size));
        }
    }
}