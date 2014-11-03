package org.mapdb;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

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
        assertEquals(DBException.Code.VOLUME_CLOSED_BY_INTERRUPT, ((DBException)ref.get()).getCode());
        //now channel should be closed
        assertFalse(v.channel.isOpen());
        try {
            v.putLong(0, 1000);
            fail();
        }catch(DBException e){
            assertEquals(DBException.Code.VOLUME_CLOSED, e.getCode());
        }
    }
}