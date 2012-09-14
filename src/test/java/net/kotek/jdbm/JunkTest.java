package net.kotek.jdbm;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;


@SuppressWarnings("unchecked")
public class JunkTest  extends JdbmTestCase{


    @Test
    public void test_mapped_byte_buffer_reopen() throws IOException {

        Assume.assumeTrue(false);

        File f = File.createTempFile("whatever","ads");
        FileChannel c = new RandomAccessFile(f,"rw").getChannel();

        MappedByteBuffer b = c.map(FileChannel.MapMode.READ_WRITE, 0, 8);
        b.putLong(0, 111L);

        c.close();

        c = new RandomAccessFile(f,"rw").getChannel();
        b = c.map(FileChannel.MapMode.READ_WRITE, 0, 8);
        long l = b.getLong(0);

        Assert.assertEquals(111L, l);

    }

    @Test public void test2(){

        RecordStore db = new RecordStoreAsyncWrite("filename",true);
        HTreeMap map = new HTreeMap(db, true);
        map.put(11,222);
//do something with map
        db.close();

        long rootRecid = map.rootRecid; //save this number somewhere
//restart JVM or whatever, and latter reopen map:
        db = new RecordStoreAsyncWrite("filename",true);
        map = new HTreeMap(db,rootRecid);
        System.out.println(map.get(11));
//do something with map, it is populated with previous data
        db.close();
    }




}
