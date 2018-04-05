package org.mapdb

import org.junit.Test

class DBTest{

    @Test
    fun works(){
        val db = DB.newOnHeapDB().make()
    }

}