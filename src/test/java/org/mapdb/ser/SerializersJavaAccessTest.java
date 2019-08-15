package org.mapdb.ser;

import org.junit.Test;
import org.mapdb.io.DataOutput2ByteArray;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SerializersJavaAccessTest {

    @Test
    public void check_accessible() {
        assertTrue(Serializers.INTEGER instanceof Serializer);

        assertTrue(Serializers.BYTE_ARRAY instanceof Serializer);
    }

    @Test
    public void integer() throws IOException {
        DataOutput2ByteArray out = new DataOutput2ByteArray();

        Integer i = new Integer(10);
        Serializers.INTEGER.serialize(out, i);
        assertEquals(out.pos, 4);
    }


}

