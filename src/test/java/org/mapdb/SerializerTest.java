package org.mapdb;

import org.junit.Test;
import java.util.*;
import static org.junit.Assert.*;

public class SerializerTest {

    @Test public void UUID2(){
        UUID u = UUID.randomUUID();
        assertEquals(u, Utils.clone(u,Serializer.UUID));
    }

    @Test public void string_ascii(){
        String s = "adas9 asd9009asd";
        assertEquals(s, Utils.clone(s,Serializer.STRING_ASCII));
        s = "";
        assertEquals(s, Utils.clone(s,Serializer.STRING_ASCII));
        s = "    ";
        assertEquals(s, Utils.clone(s,Serializer.STRING_ASCII));
    }

}
