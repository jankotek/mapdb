package org.mapdb;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class EncryptionXTEATest {


    byte[] pass = new byte[]{1,2,3,4,5};
    EncryptionXTEA xtea = new EncryptionXTEA(pass);

    @Test public void test_clone() throws Exception{
        byte[] b = new byte[]{1,2,3,4,5};
        assertArrayEquals(b, Utils.clone(b, xtea));
    }

    @Test public void put_read() throws Exception{
        DB d = DBMaker
                .newMemoryDB()
                .encryptionEnable("test")
                .cacheDisable()
                .asyncWriteDisable()
                .make();
        long recid = d.engine.put("testar", Serializer.STRING_SERIALIZER);
        assertEquals("testar",d.engine.get(recid, Serializer.STRING_SERIALIZER));
    }
}
