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
        Store s = Pump.storeForDB(db);
        assertTrue(s.compress);
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

// TODO not supported at the moment
//    @Test
//    public void short_compression() throws Exception {
//        byte[] b = new byte[]{1,2,3,4,5,33,3};
//        byte[] b2 = UtilsTest.clone(new ByteTransformEngine.ByteArrayWrapper(b), CompressLZF.SERIALIZER).b;
//        assertArrayEquals(b,b2);
//    }
//
//    @Test public void large_compression() throws IOException {
//        byte[]  b = new byte[1024];
//        b[0] = 1;
//        b[4] = 5;
//        b[1000] = 1;
//
//        assertArrayEquals(b, UtilsTest.clone(new ByteTransformEngine.ByteArrayWrapper(b), CompressLZF.SERIALIZER).b);
//
//        //check compressed size is actually smaller
//        DataOutput2 out = new DataOutput2();
//        CompressLZF.SERIALIZER.serialize(out,new ByteTransformEngine.ByteArrayWrapper(b));
//        assertTrue(out.pos<100);
//    }


}
