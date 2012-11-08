package org.mapdb;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class ChecksumCRC32SerializerTest {

    @Test public void testSimple() throws Exception {
        ChecksumCRC32Serializer s = new ChecksumCRC32Serializer();

        byte[] b = "wwefwaefw;ef;lkwef".getBytes("UTF-8");
        assertArrayEquals(b, Utils.clone(b, s));

    }

    @Test public void testCorrupt() throws Exception {
        ChecksumCRC32Serializer s = new ChecksumCRC32Serializer();

        byte[] b = "wwefwaefw;ef;lkwef".getBytes("UTF-8");

        DataOutput2 out = new DataOutput2();
        s.serialize(out,b);
        byte[] b2 = out.copyBytes();
        assertEquals(b.length+4, b2.length);

        //make string corrupted
        b2[1] = (byte) (b2[1]+1);

        DataInput2 in = new DataInput2(b2);
        try{
            s.deserialize(in, b2.length);
            fail();
        }catch (IOException e){
            assertTrue(e.getMessage().contains("CRC32"));
        }

    }

}
