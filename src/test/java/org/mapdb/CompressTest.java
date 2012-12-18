package org.mapdb;

import org.junit.Test;

import java.io.IOException;

import org.mapdb.EngineWrapper.*;
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
        assertTrue(((ByteTransformEngine)db.engine).blockSerializer ==  CompressLZF.SERIALIZER);
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
        byte[] b = new byte[]{1,2,3,4,5,33,3};
        byte[] b2 = Utils.clone(b, CompressLZF.SERIALIZER);
        assertArrayEquals(b,b2);
    }

    @Test public void large_compression() throws IOException {
        byte[]  b = new byte[1024];
        b[0] = 1;
        b[4] = 5;
        b[1000] = 1;

        assertArrayEquals(b, Utils.clone(b, CompressLZF.SERIALIZER));

        //check compressed size is actually smaller
        DataOutput2 out = new DataOutput2();
        CompressLZF.SERIALIZER.serialize(out,b);
        assertTrue(out.pos<100);
    }


}
