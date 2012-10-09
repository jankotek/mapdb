package net.kotek.jdbm;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class CompressTest{

    DB db = DBMaker
            .newMemoryDB()
            .cacheDisable()
            .asyncWriteDisable()
            .compressionEnable()
            .make();


    @Test
    public void check_instance() throws Exception {
        assertTrue(db.recman instanceof ByteTransformWrapper);
        assertTrue(((ByteTransformWrapper)db.recman).blockSerializer instanceof  CompressLZFSerializer);
    }


    @Test
    public void check_null() throws Exception {
        long recid = db.recman.recordPut(null,Serializer.NULL_SERIALIZER);
        assertTrue(recid!=0);
        assertNull(db.recman.recordGet(recid, Serializer.BASIC_SERIALIZER));
    }

    @Test
    public void put_get_update() throws Exception {
        long recid = db.recman.recordPut("aaaa",Serializer.STRING_SERIALIZER);
        assertEquals("aaaa",db.recman.recordGet(recid, Serializer.STRING_SERIALIZER));
        db.recman.recordUpdate(recid, "bbbb",Serializer.STRING_SERIALIZER);
        assertEquals("bbbb",db.recman.recordGet(recid, Serializer.STRING_SERIALIZER));
        db.recman.recordDelete(recid);
        assertEquals(null,db.recman.recordGet(recid, Serializer.STRING_SERIALIZER));

    }

    @Test
    public void short_compression() throws Exception {
        CompressLZFSerializer ser = new CompressLZFSerializer();
        byte[] b = new byte[]{1,2,3,4,5,33,3};
        byte[] b2 = JdbmUtil.clone(b, ser);
        assertArrayEquals(b,b2);
    }

    @Test public void large_compression() throws IOException {
        CompressLZFSerializer ser = new CompressLZFSerializer();
        byte[]  b = new byte[1024];
        b[0] = 1;
        b[4] = 5;
        b[1000] = 1;

        assertArrayEquals(b,JdbmUtil.clone(b, ser));

        //check compressed size is actually smaller
        DataOutput2 out = new DataOutput2();
        ser.serialize(out,b);
        assertTrue(out.pos<100);
    }


}
