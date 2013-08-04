package org.mapdb;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class ChecksumCRC32SerializerTest {

    @Test public void testSimple() throws Exception {

        byte[] b = "wwefwaefw;ef;lkwef".getBytes("UTF-8");
        assertArrayEquals(b, UtilsTest.clone(new EngineWrapper.ByteTransformEngine.ByteArrayWrapper(b), Serializer.CRC32_CHECKSUM).b);

    }

    @Test public void testCorrupt() throws Exception {


        byte[] b = "wwefwaefw;ef;lkwef".getBytes("UTF-8");

        DataOutput2 out = new DataOutput2();
        Serializer.CRC32_CHECKSUM.serialize(out,new EngineWrapper.ByteTransformEngine.ByteArrayWrapper(b));
        byte[] b2 = out.copyBytes();
        assertEquals(b.length+4, b2.length);

        //make string corrupted
        b2[1] = (byte) (b2[1]+1);

        DataInput2 in = new DataInput2(b2);
        try{
            Serializer.CRC32_CHECKSUM.deserialize(in, b2.length);
            fail();
        }catch (IOException e){
            assertTrue(e.getMessage().contains("CRC32"));
        }

    }

}
