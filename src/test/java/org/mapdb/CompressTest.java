package org.mapdb;

import org.junit.Test;
import org.mapdb.EngineWrapper.ByteTransformEngine;

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
        ByteTransformEngine e = (ByteTransformEngine) ((EngineWrapper)db.engine).getWrappedEngine();
        assertTrue(e.blockSerializer ==  CompressLZF.SERIALIZER);
    }


    @Test
    public void put_get_update() throws Exception {
        long recid = db.engine.put("aaaa", Serializer.STRING_SERIALIZER);
        assertEquals("aaaa",db.engine.get(recid, Serializer.STRING_SERIALIZER));
        db.engine.update(recid, "bbbb", Serializer.STRING_SERIALIZER);
        assertEquals("bbbb",db.engine.get(recid, Serializer.STRING_SERIALIZER));
        db.engine.delete(recid,Serializer.STRING_SERIALIZER);
        assertEquals(null,db.engine.get(recid, Serializer.STRING_SERIALIZER));

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
