package org.mapdb.serializer;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class SerializersJavaAccessTest {

    @Test
    public void check_accessible() {
        assertTrue(Serializers.INTEGER instanceof Serializer);

        assertTrue(Serializers.BYTE_ARRAY instanceof Serializer);
    }
}
