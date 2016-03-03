package org.mapdb

import org.junit.Test
import org.junit.Assert.assertEquals

class CCTest{
    @Test fun constants(){
        assertEquals(CC.PAGE_SIZE, 1L shl CC.PAGE_SHIFT)
    }
}