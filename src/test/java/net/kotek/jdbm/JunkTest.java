package net.kotek.jdbm;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;



public class JunkTest  extends JdbmTestCase{


    @Test
    public void test_mapped_byte_buffer_reopen() throws IOException {


        File f = File.createTempFile("whatever","ads");
        f.deleteOnExit();
        FileChannel c = new RandomAccessFile(f,"rw").getChannel();

        MappedByteBuffer b = c.map(FileChannel.MapMode.READ_WRITE, 0, 8);
        b.putLong(0, 111L);

        c.close();

        c = new RandomAccessFile(f,"rw").getChannel();
        b = c.map(FileChannel.MapMode.READ_WRITE, 0, 8);
        long l = b.getLong(0);

        Assert.assertEquals(111L, l);

    }






}
