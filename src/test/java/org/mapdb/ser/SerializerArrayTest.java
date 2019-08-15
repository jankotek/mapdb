package org.mapdb.ser;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertTrue;

public class SerializerArrayTest {

    @Test
    public void subtype() throws IOException {
        String[] s = new String[]{"aa","bb"};
        Serializer<String[]> ser = new ArraySerializer(Serializers.STRING, String.class);

        String[] s2 = ser.clone(s);
        assertTrue(Arrays.equals(s,s2));
    }

}
