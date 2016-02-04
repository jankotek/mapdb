package org.mapdb

import org.junit.Test
import org.junit.Assert.*

class SortedTableMapTest{

    @Test fun import6(){
        test(6)
    }

    @Test fun import40(){
        test(40)
    }


    @Test fun import100(){
        test(100)
    }

    @Test fun import1000(){
        test(1000)
    }

    @Test fun importMega(){
        test(1000000)
    }



    fun test(size:Int){
        val consumer = SortedTableMap.import(
                keySerializer = Serializer.INTEGER,
                valueSerializer = Serializer.INTEGER,
                volume = CC.DEFAULT_MEMORY_VOLUME_FACTORY.makeVolume(null, false)
        )
        for(i in 100 until 100+size){
            consumer.take(Pair(i, i*2))
        }

        val map = consumer.finish()

        if(size<10000)
            assertArrayEquals(arrayOf(100), map.keySerializer.valueArrayToArray(map.pageKeys))
        assertEquals(size, map.size)
        for(i in 100 until 100+size) {
            assertEquals(i*2, map[i])
        }
    }



}