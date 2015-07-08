package org.mapdb10;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.*;

@SuppressWarnings({"rawtypes","unchecked"})
public class SerializerTest {

    @Test public void UUID2(){
        UUID u = UUID.randomUUID();
        assertEquals(u, SerializerBaseTest.clone2(u,Serializer.UUID));
    }

    @Test public void string_ascii(){
        String s = "adas9 asd9009asd";
        assertEquals(s,SerializerBaseTest.clone2(s,Serializer.STRING_ASCII));
        s = "";
        assertEquals(s, SerializerBaseTest.clone2(s,Serializer.STRING_ASCII));
        s = "    ";
        assertEquals(s, SerializerBaseTest.clone2(s,Serializer.STRING_ASCII));
    }

    @Test public void compression_wrapper() throws IOException {
        byte[] b = new byte[100];
        new Random().nextBytes(b);
        Serializer<byte[]> ser = new Serializer.CompressionWrapper(Serializer.BYTE_ARRAY);
        assertArrayEquals(b, SerializerBaseTest.clone2(b, ser));

        b = Arrays.copyOf(b, 10000);
        assertArrayEquals(b, SerializerBaseTest.clone2(b, ser));

        DataOutput2 out = new DataOutput2();
        ser.serialize(out,b);
        assertTrue(out.pos < 1000);
    }

    @Test public void java_serializer_issue536(){
        Long l = 1111L;
        assertEquals(l, SerializerBaseTest.clone2(l, Serializer.JAVA));
    }


    @Test public void java_serializer_issue536_with_engine(){
        DB db = DBMaker.newMemoryDB().transactionDisable().cacheDisable().make();
        Long l = 1111L;
        long recid = db.engine.put(l,Serializer.JAVA);
        assertEquals(l, db.engine.get(recid, Serializer.JAVA));
    }


    @Test public void java_serializer_issue536_with_map(){
        DB db = DBMaker.newMemoryDB().transactionDisable().cacheDisable().make();
        Map m = db.createHashMap("map")
                .keySerializer(Serializer.JAVA)
                .make();
        Long l = 1111L;
        m.put(l,l);
        assertEquals(l, m.get(l));
    }
}
