package org.mapdb

import org.junit.Test
import org.junit.Assert.*
import java.util.*
import kotlin.test.assertFailsWith

class SortedTableMapTest{

    @Test fun import0(){
        test(0)
    }
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

        if(size!=0 && size<10000)
            assertArrayEquals(arrayOf(100), map.keySerializer.valueArrayToArray(map.pageKeys))
        assertEquals(size, map.size)

        val keyIter = map.keyIterator()
        val valueIter = map.valueIterator()
        val entryIter = map.entryIterator()

        for(i in 100 until 100+size) {
            assertEquals(i*2, map[i])

            assertTrue(keyIter.hasNext())
            assertEquals(i, keyIter.next())

            assertTrue(valueIter.hasNext())
            assertEquals(i*2, valueIter.next())

            assertTrue(entryIter.hasNext())
            val node = entryIter.next()
            assertEquals(i, node.key)
            assertEquals(i*2, node.value)
        }
        assertFalse(keyIter.hasNext())
        assertFailsWith(NoSuchElementException::class.java){
            keyIter.next()
        }
        assertFalse(valueIter.hasNext())
        assertFailsWith(NoSuchElementException::class.java){
            valueIter.next()
        }
        assertFalse(entryIter.hasNext())
        assertFailsWith(NoSuchElementException::class.java){
            entryIter.next()
        }

    }



}