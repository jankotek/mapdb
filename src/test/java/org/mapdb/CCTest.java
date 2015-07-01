package org.mapdb;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CCTest {

    @Test public void  concurency(){
        assertEquals(CC.DEFAULT_LOCK_SCALE, DataIO.nextPowTwo(CC.DEFAULT_LOCK_SCALE));
    }
}
