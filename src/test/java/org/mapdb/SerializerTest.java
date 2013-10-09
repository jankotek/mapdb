package org.mapdb;

import org.junit.Test;
import java.util.*;
import static org.junit.Assert.*;

public class SerializerTest {

    @Test public void UUID2(){
        UUID u = UUID.randomUUID();
        assertEquals(u, Utils.clone(u,Serializer.UUID));
    }
}
