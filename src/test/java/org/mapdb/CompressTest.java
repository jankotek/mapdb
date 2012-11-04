package org.mapdb;

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
        assertTrue(db.engine instanceof ByteTransformEngine);
        assertTrue(((ByteTransformEngine)db.engine).blockSerializer instanceof  CompressLZFSerializer);
    }


    @Test
    public void check_null() throws Exception {
        long recid = db.engine.recordPut(null,Serializer.NULL_SERIALIZER);
        assertTrue(recid!=0);
        assertNull(db.engine.recordGet(recid, Serializer.BASIC_SERIALIZER));
    }

    @Test
    public void put_get_update() throws Exception {
        long recid = db.engine.recordPut("aaaa",Serializer.STRING_SERIALIZER);
        assertEquals("aaaa",db.engine.recordGet(recid, Serializer.STRING_SERIALIZER));
        db.engine.recordUpdate(recid, "bbbb",Serializer.STRING_SERIALIZER);
        assertEquals("bbbb",db.engine.recordGet(recid, Serializer.STRING_SERIALIZER));
        db.engine.recordDelete(recid);
        assertEquals(null,db.engine.recordGet(recid, Serializer.STRING_SERIALIZER));

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
