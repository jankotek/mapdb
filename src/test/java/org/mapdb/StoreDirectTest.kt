package org.mapdb

import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet
import org.junit.Assert.*
import org.junit.Test
import org.mapdb.DataIO.*
import org.mapdb.StoreAccess.*
import org.mapdb.StoreDirectJava.*
import org.mapdb.volume.*
import java.io.File
import java.io.RandomAccessFile
import java.util.*

class StoreDirectTest:StoreDirectAbstractTest(){


    override fun openStore(file: File): StoreDirect {
        return StoreDirect.make(file.path)
    }

    override fun openStore(): StoreDirect {
        return StoreDirect.make()
    }

    @Test fun constants(){
        assertEquals(0, MAX_RECORD_SIZE%16)
        assertEquals(3*8, DATA_TAIL_OFFSET)
        assertEquals(4*8, INDEX_TAIL_OFFSET)
        assertEquals(5*8, FILE_TAIL_OFFSET)
        assertEquals(8*8, RECID_LONG_STACK)
        assertEquals(8*(8+4095+1), UNUSED1_LONG_STACK)

        assertEquals(8*(8+4095+4+1), HEAD_END)
    }


    @Test fun linked_getSet(){
        fun test(size:Int) {
            val b = TT.randomByteArray(size, 1)
            val s = openStore()
            val indexVal = s.linkedRecordPut(b, b.size)
            assertTrue(s.indexValFlagLinked(indexVal))
            assertTrue(indexValToSize(indexVal) > 0)
            assertTrue(indexValToOffset(indexVal) != 0L)

            val b2 = s.linkedRecordGet(indexVal)
            assertArrayEquals(b, b2)
        }
        test(100000)
        test(1000000)
        test(10000000)
    }

    @Test fun freeSpace(){
        val count = 100000
        val arraySize = 1024
        val div = count * arraySize / 100

        val s = openStore()
        val recids = LongHashSet()
        for(i in 0..count){
            val recid = s.put(ByteArray(arraySize), Serializer.BYTE_ARRAY_NOSIZE)
            recids.add(recid)
        }

        recids.forEach { recid->
            s.delete(recid, Serializer.BYTE_ARRAY_NOSIZE)
        }

        assertTrue( Math.abs(count*arraySize - s.getFreeSize())<div)
        assertEquals(s.getFreeSize(), s.calculateFreeSize())
    }


    @Test fun freeSpace2() {
        val count = 100000
        val arraySize = 1024
        val div = count * arraySize / 100

        val s = openStore()
        val recids = LongHashSet()
        s.getFreeSize()
        for (i in 0..count) {
            val recid = s.put(ByteArray(arraySize), Serializer.BYTE_ARRAY_NOSIZE)
            recids.add(recid)
        }

        recids.forEach { recid ->
            s.delete(recid, Serializer.BYTE_ARRAY_NOSIZE)
        }

        assertTrue(Math.abs(count * arraySize - s.getFreeSize()) < div)
        assertEquals(s.getFreeSize(), s.calculateFreeSize())
    }

    @Test fun no_head_checksum(){
        var store = StoreDirect.make(checksumHeader = false)
        assertEquals(0, store.volume.getInt(16)) //features
        assertEquals(0, store.volume.getInt(20)) //checksum

        store = StoreDirect.make(checksumHeader = true)
        assertEquals(1, store.volume.getInt(16)) //features
        assertNotEquals(0, store.volume.getInt(20)) //checksum

    }



    @Test fun checksum(){
        val vol = SingleByteArrayVol(1024*1024*2)
        val store = StoreDirect.make(volumeFactory = VolumeFactory.wrap(vol,false), checksum=false)
        store.put(11, Serializer.INTEGER)
        store.commit()
        store.close()

        //checksum is not enabled
        assertEquals(1L, vol.getLong(8))
        val i = vol.getInt(4)
        assertEquals(0, i.ushr(CC.FEAT_CHECKSUM_SHIFT) and CC.FEAT_CHECKSUM_MASK)

        //store reopen should not fail
        StoreDirect.make(volumeFactory = VolumeFactory.wrap(vol,true), checksum=false).close()

        //this fails because store has different configuration
        TT.assertFailsWith(DBException.WrongConfiguration::class.java){
            StoreDirect.make(volumeFactory = VolumeFactory.wrap(vol,true), checksum=true).close()
        }

        //set non zero checksum, it should fail to reopen
        vol.putLong(8,11)
        TT.assertFailsWith(DBException.DataCorruption::class.java){
            StoreDirect.make(volumeFactory = VolumeFactory.wrap(vol,true), checksum=false)
        }

    }


    @Test fun checksum_enable(){
        val vol = SingleByteArrayVol(1024*1024*2)
        val store = StoreDirect.make(volumeFactory = VolumeFactory.wrap(vol,false), checksum=true)
        store.put(11, Serializer.LONG)
        store.commit()
        store.close()
        //checksum is not enabled
        val checksum = vol.hash(16, 1024*1024*2-16, 0)+vol.getLong(0)
        assertEquals(checksum, vol.getLong(8))
        val i = vol.getInt(4)
        assertEquals(1, i.ushr(CC.FEAT_CHECKSUM_SHIFT) and CC.FEAT_CHECKSUM_MASK)

        //store reopen should not fail
        StoreDirect.make(volumeFactory = VolumeFactory.wrap(vol,true), checksum=true).close()

        //this fails because store has different configuration
        TT.assertFailsWith(DBException.WrongConfiguration::class.java){
            StoreDirect.make(volumeFactory = VolumeFactory.wrap(vol,true), checksum=false).close()
        }

        //set zero checksum, it should fail to reopen
        vol.putLong(8,0)
        TT.assertFailsWith(DBException.DataCorruption::class.java){
            StoreDirect.make(volumeFactory = VolumeFactory.wrap(vol,true), checksum=true)
        }

        //set wrong checksum, it should fail to reopen
        vol.putLong(8,11)
        TT.assertFailsWith(DBException.DataCorruption::class.java){
            StoreDirect.make(volumeFactory = VolumeFactory.wrap(vol,true), checksum=true)
        }
    }

    @Test fun longStackForeach(){
        var store = StoreDirect.make(checksumHeader = false)
        store.structuralLock!!.lock()
        val longStack = StoreDirectJava.UNUSED1_LONG_STACK

        val count = 1600
        val maxVal = Integer.MAX_VALUE
        val r = Random()
        val values = LongArrayList()
        for(v in 0 until count){
            val value = DataIO.parity1Set(r.nextInt(maxVal).toLong().shl(1))
            values.add(value)

            store._longStackPut(longStack, value, false)

            //check all existing values in long stack are matching values
            val fromStack = LongArrayList()
            store._longStackForEach(longStack,{fromStack.add(it)})

            assertEquals(values.toSortedList(), fromStack.toSortedList())

        }

    }

}

abstract class StoreDirectAbstractTest:StoreReopenTest() {
    abstract override fun openStore(file: File): StoreDirectAbstract

    abstract override fun openStore(): StoreDirectAbstract

    override val headerType: Long = CC.FILE_TYPE_STOREDIRECT


    @Test fun init_values(){
        val s = openStore()
        assertEquals(CC.PAGE_SIZE, s.fileTail)
        assertEquals(0L, s.maxRecid)
        assertEquals(0L, s.dataTail)
        assertEquals(CC.PAGE_SIZE, s.volume.length())

        for(masterLinkOffset in RECID_LONG_STACK until HEAD_END step 8){
            assertEquals(0L, parity4Get(s.volume.getLong(masterLinkOffset)))
        }

        //zero index page is set to zero
        assertEquals(0L, parity16Get(s.volume.getLong(HEAD_END)))
    }


    @Test fun prealloc1(){
        val s = openStore()
        val recid = s.preallocate()
        assertEquals(1L, recid)
        assertTrue(s.indexPages.isEmpty)

        assertEquals(1, s.maxRecid)
        assertEquals(0L, s.dataTail)
        assertEquals(1L * CC.PAGE_SIZE, s.volume.length())
        s.verify()
        s.locks!!.lockReadAll()
        assertEquals(
                s.indexValCompose(size=NULL_RECORD_SIZE, offset=0L, linked=0, archive=1, unused=1),
                s.getIndexVal(1L))
    }

    @Test fun indexValCompose(){
        val s = openStore()
        assertEquals(32L.shl(48), s.indexValCompose(size=32L, offset=0L, linked=0, unused=0, archive=0))
        assertEquals(1024, s.indexValCompose(size=0, offset=1024L, linked=0, unused=0, archive=0))
        assertEquals(1.shl(3), s.indexValCompose(size=0L, offset=0L, linked=1, unused=0, archive=0))
        assertEquals(1.shl(1), s.indexValCompose(size=0L, offset=0L, linked=0, unused=0, archive=1))
        assertEquals(1.shl(2), s.indexValCompose(size=0L, offset=0L, linked=0, unused=1, archive=0))
    }

    @Test fun allocate_new_page(){
        val s = openStore()
        s.structuralLock?.lock()
        for(i in 1L until 16) {
            assertEquals(i * CC.PAGE_SIZE, s.volume.length())
            assertEquals(i * CC.PAGE_SIZE, s.allocateNewPage())
            s.commit()
        }
    }

    @Test fun recidToOffset_convert(){
        val s = openStore()
        s.structuralLock?.lock()
        s.allocateNewIndexPage();
        s.allocateNewIndexPage();
        s.allocateNewIndexPage();
        var oldOffset = 0L
        for(recid in 1L .. 3*1024*1024/10){
            val offset = s.recidToOffset(recid)
            assertTrue(offset%CC.PAGE_SIZE>=16)
            assertTrue(offset>oldOffset)
            oldOffset=offset
        }
    }

    @Test fun recid2Offset() {
        val e = openStore()

        //create 2 fake index pages
        e.volume.ensureAvailable(CC.PAGE_SIZE * 12)
        e.indexPages.add(CC.PAGE_SIZE * 3)
        e.indexPages.add(CC.PAGE_SIZE * 6)
        e.indexPages.add(CC.PAGE_SIZE * 11)


        //control bitset with expected recid layout
        val b = BitSet((CC.PAGE_SIZE * 7).toInt())
        //fill bitset at places where recids should be
        b.set(HEAD_END.toInt() + 16, CC.PAGE_SIZE.toInt())
        b.set(CC.PAGE_SIZE.toInt() * 3 + 16, CC.PAGE_SIZE.toInt() * 4)
        b.set(CC.PAGE_SIZE.toInt() * 6 + 16, CC.PAGE_SIZE.toInt() * 7)
        b.set(CC.PAGE_SIZE.toInt() * 11 + 16, CC.PAGE_SIZE.toInt() * 12)

        //bitset with recid layout generated by recid2Offset
        val b2 = BitSet((CC.PAGE_SIZE * 7).toInt())
        var oldOffset: Long = 0
        var recid: Long = 1
        recidLoop@ while (true) {
            val offset = e.recidToOffset(recid)

            assertTrue(b.get(offset.toInt()))
            assertTrue(oldOffset < offset)
            oldOffset = offset
            b2.set(offset.toInt(), offset.toInt() + 8)
            if (offset == CC.PAGE_SIZE * 12 - 8)
                break@recidLoop
            recid++
        }

        for (offset in 0..b.length() - 1) {
            if (b.get(offset) != b2.get(offset))
                throw AssertionError("error at offset " + offset)
        }
    }


    @Test fun allocate_index_page(){
        val s = openStore()
        s.structuralLock?.lock()
        val c = LongArrayList()
        var prevOffset = ZERO_PAGE_LINK
        assertEquals(0, parity16Get(s.volume.getLong(prevOffset)))
        for(i in 1L until 16) {
            assertEquals(c, s.indexPages)
            assertEquals(i * CC.PAGE_SIZE, s.volume.length())
            val indexPage = s.allocateNewIndexPage();
            s.commit()
            assertEquals(i * CC.PAGE_SIZE, indexPage)
            c.add(indexPage)
            assertEquals(c, s.indexPages)
            assertEquals(indexPage, parity16Get(s.volume.getLong(prevOffset)))
            prevOffset = indexPage+8
            assertEquals(0, parity16Get(s.volume.getLong(prevOffset)))
        }
        s.commit()

        //open new engine over the same volune, check it has the same index pages
        val s2 = StoreDirect.make(volumeFactory = VolumeFactory.wrap(s.volume,true))
        assertEquals(c, s2.indexPages)
    }

    @Test fun allocate_recid(){
        val s = openStore()
        s.structuralLock?.lock()
        for(recid in 1L .. 100000L){
            assertEquals(recid, s.allocateRecid())
        }
    }

    @Test fun alloc_data(){
        val s = openStore()
        s.structuralLock?.lock()
        for(offset in CC.PAGE_SIZE until CC.PAGE_SIZE*2 step 1024){
            assertEquals(offset, s.allocateData(1024,false))
            var expectedTail = offset+1024
            if(expectedTail == CC.PAGE_SIZE*2)
                expectedTail = 0L
            assertEquals(expectedTail, s.dataTail)
            assertEquals(CC.PAGE_SIZE*2, s.fileTail)
        }
        assertEquals(CC.PAGE_SIZE*2, s.fileTail)
    }

    @Test fun alloc_data_overflow(){
        val s = openStore()
        s.structuralLock?.lock()
        //fill until rim
        for(offset in CC.PAGE_SIZE until CC.PAGE_SIZE*2-1024 step 1024){
            assertEquals(offset, s.allocateData(1024, false))
        }
        assertEquals(CC.PAGE_SIZE*2, s.fileTail)

        //allocate smaller
        assertEquals(CC.PAGE_SIZE*2-1024, s.allocateData(16, false))
        assertEquals(CC.PAGE_SIZE*2-1024+16, s.dataTail)

        //allocate which will cause overflow
        assertEquals(CC.PAGE_SIZE*2+LONG_STACK_PREF_SIZE, s.allocateData(1024,false))
        assertEquals(CC.PAGE_SIZE*2+1024+LONG_STACK_PREF_SIZE, s.dataTail)
        assertEquals(CC.PAGE_SIZE*3, s.fileTail)
        //TODO once free space works, make sure that `CC.PAGE_SIZE*2-1024+16` is free
    }


    @Test fun longStack_putTake(){
        val s = openStore()
        s.structuralLock?.lock()
        assertEquals(0, s._longStackTake(UNUSED1_LONG_STACK,false))
        s._longStackPut(UNUSED1_LONG_STACK, parity1Set(160L),false)
        assertEquals(parity1Set(160L), s._longStackTake(UNUSED1_LONG_STACK,false))
        assertEquals(0, s._longStackTake(UNUSED1_LONG_STACK,false))
    }

    @Test fun longStack_putTake2(){
        val s = openStore()
        s.structuralLock?.lock()
        assertEquals(0, s._longStackTake(UNUSED1_LONG_STACK,false))
        s._longStackPut(UNUSED1_LONG_STACK, parity1Set(160L),false)
        s._longStackPut(UNUSED1_LONG_STACK, parity1Set(320L),false)
        assertEquals(parity1Set(320L), s._longStackTake(UNUSED1_LONG_STACK,false))
        assertEquals(parity1Set(160L), s._longStackTake(UNUSED1_LONG_STACK,false))
        assertEquals(0, s._longStackTake(UNUSED1_LONG_STACK,false))
    }

    @Test fun longStack_putTake_many() {
        val max2 = 10000L
        val min2 = if(TT.shortTest()) max2 else 1
        val s = openStore()
        s.structuralLock?.lock()
        for(a in 1 .. 10) {
            for(max in min2..max2) {
                for (i in 1L..max) {
                    s._longStackPut(UNUSED1_LONG_STACK, parity1Set(i * 16), false)
                }
                for (i in max downTo  1L) {
                    val t = s._longStackTake(UNUSED1_LONG_STACK, false)
                    assertEquals(i * 16, parity1Get(t))
                }
                assertEquals(0L, s._longStackTake(UNUSED1_LONG_STACK, false))
            }
        }
    }

    @Test fun longStack_triple(){
        val vals = longArrayOf(16L, 160L, 32000L).map{parity1Set(it)} //various packed sizes
        val s = openStore()
        s.structuralLock?.lock()

        for(v1 in vals) for (v2 in vals) for(v3 in vals){
            s._longStackPut(UNUSED1_LONG_STACK, v1, false)
            s._longStackPut(UNUSED1_LONG_STACK, v2, false)
            s._longStackPut(UNUSED1_LONG_STACK, v3, false)
            assertEquals(v3, s._longStackTake(UNUSED1_LONG_STACK, false))
            assertEquals(v2, s._longStackTake(UNUSED1_LONG_STACK, false))
            assertEquals(v1, s._longStackTake(UNUSED1_LONG_STACK, false))
            assertEquals(0L, s._longStackTake(UNUSED1_LONG_STACK, false))
        }
    }




    @Test fun freeSpace3(){
        val db = DBMaker.memoryDB().make()
        val store = db.store as StoreDirect
        val map = db.hashMap("map",Serializer.LONG, Serializer.BYTE_ARRAY).create()

        for(i in 0..10) for(key in 1L .. 10000){
            map.put(key, ByteArray(800))
            assertEquals( store.calculateFreeSize(), store.getFreeSize() )
        }
    }

    @Test fun compact(){
        val store = openStore();

        val ref = LongObjectHashMap<ByteArray>()
        //insert random records
        val random = Random()
        for(i in 1..1000){
            val string = TT.randomByteArray(size = random.nextInt(100000), seed=random.nextInt())
            val recid = store.put(string, Serializer.BYTE_ARRAY_NOSIZE)
            ref.put(recid,string)
        }
        val nullRecid = store.put(null, Serializer.BYTE_ARRAY_NOSIZE);

        store.compact()
        store.verify()

        assertEquals(ref.size()+1, store.getAllRecids().asSequence().count())
        store.getAllRecids().asSequence().forEach { recid->
            assertTrue(ref.containsKey(recid)|| recid==nullRecid)
        }

        ref.forEachKeyValue { key, value ->
            val value2 = store.get(key, Serializer.BYTE_ARRAY_NOSIZE)
            assertTrue(Arrays.equals(value,value2))
        }

        assertNull(store.get(nullRecid,Serializer.BYTE_ARRAY_NOSIZE))
    }

    @Test open fun delete_after_close(){
        val dir = TT.tempDir()
        val store = StoreDirect.make(dir.path+"/aa", fileDeleteAfterClose = true)
        store.put(11, Serializer.INTEGER)
        store.commit()
        store.put(11, Serializer.INTEGER)
        store.commit()
        assertNotEquals(0, dir.listFiles().size)
        store.close()
        assertEquals(0, dir.listFiles().size)
    }

    @Test fun firstSize(){
        val store = openStore()

        assertEquals(CC.PAGE_SIZE, store.volume.length())
        store.put("aa", Serializer.STRING)
        store.commit()
        assertEquals(1*CC.PAGE_SIZE, store.volume.length())
        store.put("aaaaaaaaaa", Serializer.STRING)
        store.commit()
        assertEquals(2*CC.PAGE_SIZE, store.volume.length())
        store.close()
    }

    @Test fun header_checksum(){
        val f = TT.tempFile()
        val store = openStore(f)

        fun check(){
            val raf = RandomAccessFile(f,"r")

            var c = StoreDirectJava.HEAD_CHECKSUM_SEED
            for(offset in 24 until StoreDirectJava.HEAD_END step 4) {
                raf.seek(offset)
                c += raf.readInt()
            }
            raf.seek(20)
            assertEquals(c, raf.readInt())
        }

        check()

        store.put(1, Serializer.INTEGER)
        store.commit()

        check()

        //corrupt it and reopen
        store.close()

        val raf = RandomAccessFile(f,"rw")
        raf.seek(20)
        raf.writeInt(111)
        raf.close()
        TT.assertFailsWith(DBException.DataCorruption::class.java){
            openStore(f)
        }

        f.delete()
    }

    @Test fun small_ser_size(){
        val f  = TT.tempFile()
        for(size in 1 .. 20){
            var store = openStore(f)

            var b = TT.randomByteArray(size,1)
            val recid = store.put(b, Serializer.BYTE_ARRAY_NOSIZE)
            assertArrayEquals(b, store.get(recid, Serializer.BYTE_ARRAY_NOSIZE))

            store.commit()
            assertArrayEquals(b, store.get(recid, Serializer.BYTE_ARRAY_NOSIZE))
            store.verify()

            //update same size
            b = TT.randomByteArray(size,2)
            store.update(recid, b, Serializer.BYTE_ARRAY_NOSIZE)
            assertArrayEquals(b, store.get(recid, Serializer.BYTE_ARRAY_NOSIZE))
            store.commit()
            assertArrayEquals(b, store.get(recid, Serializer.BYTE_ARRAY_NOSIZE))
            store.verify()


            //read after reopen
            store.close()
            store = openStore(f)
            assertArrayEquals(b, store.get(recid, Serializer.BYTE_ARRAY_NOSIZE))
            store.verify()

            //CAS the same size
            val b2 = TT.randomByteArray(size,3)
            assertFalse(store.compareAndSwap(recid, b2, TT.randomByteArray(2,4), Serializer.BYTE_ARRAY_NOSIZE))
            assertTrue(store.compareAndSwap(recid, b, b2, Serializer.BYTE_ARRAY_NOSIZE))
            assertArrayEquals(b2, store.get(recid, Serializer.BYTE_ARRAY_NOSIZE))
            store.verify()

            store.close()
            f.delete()
        }
    }

    @Test fun test_sizes(){
        if(TT.shortTest())
            return

        val sizes = TreeSet<Int>()
        (0..20).forEach { sizes.add(it)}
        intArrayOf(400,4000,65000, 100000, 1000000).forEach { sizes.add(it) }
        (StoreDirectJava.MAX_RECORD_SIZE-20 .. StoreDirectJava.MAX_RECORD_SIZE+20).forEach { sizes.add(it.toInt()) }

        val arrays = sizes.map{TT.randomByteArray(it)}


        for(a1 in arrays) for(a2 in arrays){
            val store = openStore()
            var recid = store.put(a1, Serializer.BYTE_ARRAY_NOSIZE)

            fun eq(b:ByteArray) {
                assertTrue(Arrays.equals(b, store.get(recid, Serializer.BYTE_ARRAY_NOSIZE)))
                store.verify()
                store.commit()
                assertTrue(Arrays.equals(b, store.get(recid, Serializer.BYTE_ARRAY_NOSIZE)))
                store.verify()
            }
            eq(a1)

            store.update(recid, a2, Serializer.BYTE_ARRAY_NOSIZE)
            eq(a2)

            assertTrue(store.compareAndSwap(recid, a2, a1, Serializer.BYTE_ARRAY_NOSIZE))
            eq(a1)

            store.delete(recid, Serializer.BYTE_ARRAY_NOSIZE)
            TT.assertFailsWith(DBException.GetVoid::class.java){
                    store.get(recid, Serializer.BYTE_ARRAY_NOSIZE)
            }
            store.verify()
            store.commit()
            TT.assertFailsWith(DBException.GetVoid::class.java){
                store.get(recid, Serializer.BYTE_ARRAY_NOSIZE)
            }
            store.verify()

            //update from preallocation
            recid = store.preallocate()
            assertNull(store.get(recid, Serializer.BYTE_ARRAY_NOSIZE))
            store.update(recid, a1, Serializer.BYTE_ARRAY_NOSIZE)
            eq(a1)

            //cas from preallication
            recid = store.preallocate()
            assertNull(store.get(recid, Serializer.BYTE_ARRAY_NOSIZE))
            assertTrue(store.compareAndSwap(recid, null, a1, Serializer.BYTE_ARRAY_NOSIZE))
            eq(a1)
            store.close()
        }
    }

    @Test fun head_feat_bits(){
        val firstUnknownBit = 3
        for(bitPos in firstUnknownBit until 32) {
            val file = TT.tempFile()
            val store = openStore(file)
            store.close()
            //change one bit
            val r = RandomAccessFileVol.FACTORY.makeVolume(file.path, false)
            val features = r.getInt(4)
            assertEquals(0, features.ushr(bitPos) and 1)
            r.putInt(4, features + 1.shl(bitPos))
            r.close()
            TT.assertFailsWith(DBException.NewMapDBFormat::class.java){
                openStore(file)
            }

            file.delete()
        }
    }


    @Test fun direct_feat_bits(){
        val firstUnknownBit = 1
        for(bitPos in firstUnknownBit until 32) {
            val file = TT.tempFile()
            val store = openStore(file)
            store.close()
            //change one bit
            val r = RandomAccessFileVol.FACTORY.makeVolume(file.path, false)
            val features = r.getInt(16)
            assertEquals(0, features.ushr(bitPos) and 1)
            r.putInt(16, features + 1.shl(bitPos))

            //update header checksum
            var c = StoreDirectJava.HEAD_CHECKSUM_SEED
            for(offset in 24 until StoreDirectJava.HEAD_END step 4) {
                c += r.getInt(offset)
            }
            r.putInt(16, c)

            r.close()
            TT.assertFailsWith(DBException.NewMapDBFormat::class.java){
                openStore(file)
            }

            file.delete()
        }
    }


    @Test fun store_header_checksum(){
        var store = openStore(file)
        store.close()
        val r = RandomAccessFileVol.FACTORY.makeVolume(file.path, false)
        assertNotEquals(0, r.getInt(20))
        r.putInt(20, 0)
        r.close()
        TT.assertFailsWith(DBException.DataCorruption::class.java) {
            openStore(file)
        }
    }


    @Test fun header_checksum_bypass(){
        val vol = ByteArrayVol()
        val store = StoreDirect.make(volumeFactory = VolumeFactory.wrap(vol, false), checksumHeaderBypass = false)
        store.put(111, Serializer.INTEGER)
        store.commit()

        //corrupt header
        vol.putInt(20, 0)
        //and reopen
        StoreDirect.make(volumeFactory = VolumeFactory.wrap(vol, true), checksumHeaderBypass = true)
        TT.assertFailsWith(DBException.DataCorruption::class.java){
            StoreDirect.make(volumeFactory = VolumeFactory.wrap(vol, true), checksumHeaderBypass = false)
        }
    }

    @Test fun headers(){
        val f = TT.tempFile()
        val store = openStore(f)
        store.put(TT.randomByteArray(1000000),Serializer.BYTE_ARRAY)

        val raf = RandomAccessFile(f.path, "r");
        raf.seek(0)
        assertEquals(CC.FILE_HEADER.toInt(), raf.readUnsignedByte())
        assertEquals(CC.FILE_TYPE_STOREDIRECT.toInt(), raf.readUnsignedByte())
        assertEquals(0, raf.readChar().toInt())
        raf.close()
        f.delete()
    }


    @Test fun version_fail2(){
        val f = TT.tempFile()
        val store = openStore(f)
        store.close()
        val wal = RandomAccessFile(f.path , "rw");
        wal.seek(3)
        wal.writeByte(1)
        wal.close()
        TT.assertFailsWith(DBException.NewMapDBFormat::class.java) {
            openStore(f)
        }

        f.delete()
    }

    @Test fun fileLoad(){
        val f = TT.tempFile()
        val store = StoreDirect.make(file=f.path, volumeFactory = MappedFileVol.FACTORY)
        assertTrue(store.fileLoad())
        f.delete()
    }

}
