package org.mapdb;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import static org.junit.Assert.*;

public class VolumeTest {

    @Test
    public void mmap_init_size() throws IOException {
        //test if mmaping file size repeatably increases file
        File f = File.createTempFile("mapdb","mapdb");

        long chunkSize = 1<<CC.VOLUME_CHUNK_SHIFT;
        long add = 100000;

        //open file channel and write some size
        RandomAccessFile raf = new RandomAccessFile(f,"rw");
        raf.seek(add);
        raf.writeInt(11);
        raf.close();

        //open mmap file, size should grow to multiple of chunk size
        Volume.MappedFileVol m = new Volume.MappedFileVol(f, false,0,CC.VOLUME_CHUNK_SHIFT,0);
        assertEquals(1, m.chunks.length);
        m.ensureAvailable(add + 4);
        assertEquals(11, m.getInt(add));
        m.sync();
        m.close();
        assertEquals(chunkSize, f.length());

        raf = new RandomAccessFile(f,"rw");
        raf.seek(chunkSize + add);
        raf.writeInt(11);
        raf.close();

        m = new Volume.MappedFileVol(f, false,0,CC.VOLUME_CHUNK_SHIFT,0);
        assertEquals(2, m.chunks.length);
        m.sync();
        m.ensureAvailable(chunkSize + add + 4);
        assertEquals(chunkSize * 2, f.length());
        assertEquals(11, m.getInt(chunkSize + add));
        m.sync();
        m.close();
        assertEquals(chunkSize * 2, f.length());

        m = new Volume.MappedFileVol(f, false,0,CC.VOLUME_CHUNK_SHIFT,0);
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
        assertEquals(3, m.chunks.length);
        assertEquals(chunkSize * 3, f.length());

        m.close();
        f.delete();
    }

}
