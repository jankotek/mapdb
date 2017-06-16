package org.mapdb.volume

import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet
import org.junit.Assert.*
import org.junit.Test
import org.mapdb.*
import org.mapdb.crash.CrashJVM
import org.mapdb.util.DataIO
import java.io.*
import java.util.*


class VolumeCrashTest(): CrashJVM(){
    val fabs = mapOf<String,Function1<String, Volume>>(
            Pair("fileChannel",{file -> FileChannelVol(File(file), false, 0L, CC.PAGE_SHIFT, 0L)}),
            Pair("raf",{file -> RandomAccessFileVol(File(file), false, 0L, 0L) }),
            Pair("mapped",{file -> MappedFileVol(File(file), false, 0L, CC.PAGE_SHIFT, false, 0L, false) }),
            Pair("mappedSingle",{file -> MappedFileVolSingle(File(file), false, 0L, 4e7.toLong(), false) })
    )

    val max = 4*1024*1024
    val count = 100;
    fun fileForSeed(seed:Long) = getTestDir().toString()+"/"+seed;

    override fun doInJVM(startSeed: Long, params: String) {
        var seed = startSeed
        while (true) {
            seed++
            val file = fileForSeed(seed)
            val v = fabs[params]!!(file);
            v.ensureAvailable(max.toLong())

            val random = Random(seed)
            val alreadyWritten = LongHashSet();
            for(i in 0 until count) {
                val offset = DataIO.roundDown(random.nextInt(max-8).toLong(),8)
                if(!alreadyWritten.add(offset))
                    continue
                v.putLong(offset, random.nextLong())
            }
            v.sync()
            v.close()
            commitSeed(seed)
            //delete prev file to keep disk space usage low
            File(fileForSeed(seed - 1)).delete()

        }
    }

    override fun verifySeed(startSeed: Long, endSeed: Long, params: String): Long {
        val file = fileForSeed(endSeed)
        val raf = RandomAccessFile(file, "r")
        assertTrue(raf.length()>=8)
        val random = Random(endSeed)
        val alreadyWritten = LongHashSet();
        for(i in 0 until count) {
            val offset = DataIO.roundDown(random.nextInt(max-8).toLong(),8)
            if(!alreadyWritten.add(offset))
                continue
            raf.seek(offset)
            assertEquals(random.nextLong(), raf.readLong())
        }


        raf.close()
        return endSeed+10
    }

    @Test
    fun fileChannel() {
        if (TT.shortTest())
            return

        run(this, time = TT.testRuntime(10), params="fileChannel")
    }

    @Test
    fun raf() {
        if (TT.shortTest())
            return

        run(this, time = TT.testRuntime(10), params="raf")
    }

    @Test
    fun mapped() {
        if (TT.shortTest())
            return

        run(this, time = TT.testRuntime(10), params="mapped")
    }

    @Test
    fun mappedSingle() {
        if (TT.shortTest())
            return

        run(this, time = TT.testRuntime(10), params="mappedSingle")
    }
}