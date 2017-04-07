package org.mapdb

import org.fest.reflect.core.Reflection
import org.junit.Assert.*
import org.junit.Test
import org.mapdb.TT.assertFailsWith
import org.mapdb.volume.ByteArrayVol
import org.mapdb.volume.RandomAccessFileVol
import java.io.RandomAccessFile
import java.math.BigInteger
import java.util.*

class SortedTableMapTest{

    val SortedTableMap<*,*>.pageKeys:  Any
        get() = Reflection.method("getPageKeys").`in`(this).invoke()


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


    @Test fun header(){
        val volume = CC.DEFAULT_MEMORY_VOLUME_FACTORY.makeVolume(null, false)
        val consumer = SortedTableMap.createFromSink(
                keySerializer = Serializer.INTEGER,
                valueSerializer = Serializer.INTEGER,
                volume = volume
        )
        consumer.put(1,1)
        consumer.create()
        assertEquals(CC.FILE_HEADER, volume.getUnsignedByte(0).toLong())
        assertEquals(CC.FILE_TYPE_SORTED_SINGLE, volume.getUnsignedByte(1).toLong())
    }

    fun test(size:Int){
        val consumer = SortedTableMap.createFromSink(
                keySerializer = Serializer.INTEGER,
                valueSerializer = Serializer.INTEGER,
                volume = CC.DEFAULT_MEMORY_VOLUME_FACTORY.makeVolume(null, false)
        )
        for(i in 0 until size*3 step 3){
            consumer.put(Pair(i, i*2))
        }

        val map = consumer.create()

        if(size!=0 && size<10000)
            assertArrayEquals(arrayOf(0), map.keySerializer.valueArrayToArray(map.pageKeys))
        assertEquals(size, map.size)

        var keyIter = map.keyIterator()
        var valueIter = map.valueIterator()
        var entryIter = map.entryIterator()

        for(i in 0 until size*3 step 3) {
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


        //test lower, higher etc
        val notEmpty = map.isEmpty().not()
        for(i in -2 until size*3+2){
            val notin = i%3!=0 || i<0 || i>=size*3
            val expected = if(notin) null else  i*2
            assertEquals(expected, map[i] )
            val maxKey = size*3-3
            assertEquals(if(i>0 && notEmpty) Math.min(maxKey,((i-1)/3)*3) else null , map.lowerKey(i))
            assertEquals(if(i>=0 && notEmpty) Math.min(maxKey,(i/3)*3) else null , map.floorKey(i))
            assertEquals(if(i<maxKey && notEmpty) Math.max(0, DataIO.roundUp(i+1,3)) else null , map.higherKey(i))
            assertEquals(if(i<=maxKey && notEmpty) Math.max(0, DataIO.roundUp(i,3)) else null , map.ceilingKey(i))
        }

        //do reverse iterators
        keyIter = map.descendingKeyIterator()
        valueIter = map.descendingValueIterator()
        entryIter = map.descendingEntryIterator()

        for(i in size*3-3 downTo 0 step 3) {
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


    @Test fun entry_iterator_values_issue685(){
        val consumer = SortedTableMap.createFromSink(
                keySerializer = Serializer.INTEGER,
                valueSerializer = Serializer.INTEGER,
                volume = CC.DEFAULT_MEMORY_VOLUME_FACTORY.makeVolume(null, false)
        )
        val size = 1e6.toInt()
        for(i in 0 until size){
            consumer.put(Pair(i, i*2))
        }

        val map = consumer.create()


        var iter = map.iterator()
        var count = 0;
        while(iter.hasNext()){
            val next = iter.next()
            assertEquals(count, next.key)
            assertEquals(count*2, next.value)
            count++
        }

        iter = map.descendingMap().iterator()
        while(iter.hasNext()){
            count--
            val next = iter.next()
            assertEquals(count, next.key)
            assertEquals(count*2, next.value)
        }

        iter = map.tailMap(Integer.MIN_VALUE).iterator()
        count = 0;
        while(iter.hasNext()){
            val next = iter.next()
            assertEquals(count, next.key)
            assertEquals(count*2, next.value)
            count++
        }

        iter = map.tailMap(Integer.MIN_VALUE).descendingMap().iterator()
        while(iter.hasNext()){
            count--
            val next = iter.next()
            assertEquals(count, next.key)
            assertEquals(count*2, next.value)
        }
    }

    @Test
    fun issue695(){
        var volume = ByteArrayVol.FACTORY.makeVolume(null,false)
        val sink = SortedTableMap.create(
                volume,
                Serializer.BYTE_ARRAY,
                Serializer.STRING).createFromSink()
        TT.assertFailsWith(DBException.NotSorted::class.java) {
            for (key in 120L..131) {
                sink.put(BigInteger.valueOf(key).toByteArray(), "value" + key)
            }
            sink.create()
        }
    }

    @Test fun headers2(){
        val f = TT.tempFile()
        val vol = RandomAccessFileVol.FACTORY.makeVolume(f.path, false)
        val s =  SortedTableMap.create(vol, Serializer.LONG, Serializer.LONG).createFrom(HashMap())
        s.close()
        val raf = RandomAccessFile(f.path, "rw");
        raf.seek(0)
        assertEquals(CC.FILE_HEADER.toInt(), raf.readUnsignedByte())
        assertEquals(CC.FILE_TYPE_SORTED_SINGLE.toInt(), raf.readUnsignedByte())
        assertEquals(0, raf.readChar().toInt())
        raf.seek(3)
        raf.writeByte(1)
        raf.close()
        TT.assertFailsWith(DBException.NewMapDBFormat::class.java) {
            val vol2 = RandomAccessFileVol.FACTORY.makeVolume(f.path, false)
            SortedTableMap.open(vol2, Serializer.LONG, Serializer.LONG)
        }
        f.delete()
    }

}