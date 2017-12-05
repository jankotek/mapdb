package org.mapdb.issues

import org.junit.Assert.assertEquals
import org.junit.Test
import org.mapdb.*
import org.mapdb.serializer.Serializers
import org.mapdb.store.*
import org.mapdb.util.DataIO
import org.mapdb.volume.RandomAccessFileVol
import java.util.concurrent.atomic.AtomicBoolean


class Issues760_compact_thread_safe {

    @Test fun compactShouldBeThreadSafe1() = compactShouldBeThreadSafe(false,false)
    @Test fun compactShouldBeThreadSafeWhenUsedByDB1() = compactShouldBeThreadSafeWhenUsedByDB(false,false)


    @Test fun compactShouldBeThreadSafe2() = compactShouldBeThreadSafe(false,true)
    @Test fun compactShouldBeThreadSafeWhenUsedByDB2() = compactShouldBeThreadSafeWhenUsedByDB(false,true)


    @Test fun compactShouldBeThreadSafe3() = compactShouldBeThreadSafe(true,false)
    @Test fun compactShouldBeThreadSafeWhenUsedByDB3() = compactShouldBeThreadSafeWhenUsedByDB(true,false)

    @Test fun compactShouldBeThreadSafe4() = compactShouldBeThreadSafe(true,true)
    @Test fun compactShouldBeThreadSafeWhenUsedByDB4() = compactShouldBeThreadSafeWhenUsedByDB(true,true)


    fun compactShouldBeThreadSafe(tx:Boolean, withCompactThread:Boolean) {
        if(TT.shortTest())
            return
        val entries = 10000
        val initValue = 10000
        val updateCount = 1000
        val end = AtomicBoolean(withCompactThread)

        val db1 = TT.tempFile()


        //val store = DBMaker.fileDB(db1).closeOnJvmShutdown().make().getStore()
        val store =  if(!tx) StoreDirect.make(file = db1.absolutePath, volumeFactory = RandomAccessFileVol.FACTORY,
                fileLockWait = 0,
                allocateIncrement = 0,
                allocateStartSize = 0,
                isReadOnly = false,
                fileDeleteAfterClose = false,
                fileDeleteAfterOpen = false,
                concShift = DataIO.shift(DataIO.nextPowTwo(8)),
                checksum = false,
                isThreadSafe = true ,
                checksumHeaderBypass = false)
            else
                StoreWAL.make(file = db1.absolutePath, volumeFactory = RandomAccessFileVol.FACTORY,
                    fileLockWait = 0,
                    allocateIncrement = 0,
                    allocateStartSize = 0,
                    fileDeleteAfterClose = false,
                    concShift = DataIO.shift(DataIO.nextPowTwo(8)),
                    checksum = false,
                    isThreadSafe = true ,
                    checksumHeaderBypass = false)
        try {
            //compact continuous loop
            val compact = Thread({
                while (end.get()) {
                    store.compact()
                    Thread.sleep(10)
                }
            })
            compact.isDaemon=true
            compact.start()

            //init entries
            val recids = LongArray(entries)
            for (i in 0 until entries) {
                recids[i] = store.put(initValue, Serializers.INTEGER)
            }
            assertEquals(recids.toSet().size, recids.size)


            //update 5 times each entry
            //increment each numbers based on previous saved value
            for (k in 0 until updateCount) {
                for (recid in recids) {
                    //get -> increment -> store
                    store.update(recid, store.get(recid, Serializers.INTEGER)!!+1, Serializers.INTEGER)
                }
            }
            end.set(false)

            // verify
            for (recid in recids) {
                assertEquals(initValue + updateCount, store.get(recid, Serializers.INTEGER)!!)
            }
        } finally {
            store.close()
            if (db1.exists()) {
                db1.delete()
            }
        }
    }

    //will most likely fail, during read or write operation
    //due to compact thread loop

    fun compactShouldBeThreadSafeWhenUsedByDB(tx:Boolean, withCompactThread:Boolean) {
        if(TT.shortTest())
            return

        val db1 = TT.tempFile()

        val entries = 10000
        val initValue = 10000
        val expectedFinalValue = initValue + 5
        val end = AtomicBoolean(withCompactThread)

        val make = if(!tx) DBMaker.fileDB(db1).closeOnJvmShutdown().make()
                    else DBMaker.fileDB(db1).closeOnJvmShutdown().transactionEnable().make()
        try {
            val open = make.hashMap("aaaaa", Serializers.INTEGER, Serializers.INTEGER).createOrOpen()
            //compact continuous loop
            val compact = Thread({
                while (end.get()) {
                    make.store.compact()
                    Thread.sleep(10)
                }
            })
            compact.isDaemon=true
            compact.start()

            //init entries
            for (i in 0..entries) {
                open.put(i, initValue)
            }

            //update 5 times each entry
            //increment each numbers based on previous saved value
            for (k in 0..4) {
                for (i in 0..entries) {
                    //get -> increment -> store
                    open.put(i, (open[i] ?: 0) + 1)
                }
            }
            end.set(false)

            // verify
            for (i in 0..entries) {
                assertEquals(expectedFinalValue, open[i] ?: 0)
            }
        } finally {
            make.close()
            if (db1.exists()) {
                db1.delete()
            }
        }
    }
}