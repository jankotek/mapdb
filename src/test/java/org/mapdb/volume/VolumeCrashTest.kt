package org.mapdb.volume

import org.junit.Test
import java.io.File
import java.io.RandomAccessFile
import java.util.*
import org.junit.Assert.*
import org.mapdb.CC
import org.mapdb.CrashJVM
import org.mapdb.TT


class VolumeCrashTest(): CrashJVM(){
    val fabs = mapOf<String,Function1<String, Volume>>(
            Pair("fileChannel",{file -> FileChannelVol(File(file), false, false, CC.PAGE_SHIFT, 0L)}),
            Pair("raf",{file -> RandomAccessFileVol(File(file), false, false, 0L) }),
            Pair("mapped",{file -> MappedFileVol(File(file), false, false, CC.PAGE_SHIFT, false, 0L, false) }),
            Pair("mappedSingle",{file -> MappedFileVolSingle(File(file), false, false, 4e7.toLong(), false) })
    )

    val max = 8L//4L*1024*1024
    val count = 100;
    fun fileForSeed(seed:Long) = getTestDir().toString()+"/"+seed;

    override fun doInJVM(startSeed: Long, params: String) {
        var seed = startSeed
        while (true) {
            seed++
            val file = fileForSeed(seed)
            val v = fabs[params]!!(file);
            v.ensureAvailable(8)

            val random = Random(seed)
            for(i in 0 until count) {
                //raf.seek(random.nextInt(max.toInt() - 8).toLong())
                v.putLong(0L, random.nextLong())
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
        for(i in 0 until count-1) {
            random.nextLong()
//            raf.seek(random.nextInt(max.toInt() - 8).toLong())
        }
        assertEquals(random.nextLong(), raf.readLong())

        raf.close()
        return endSeed+10
    }

    @Test
    fun fileChannel() {
        run(this, time = TT.testRuntime(10), params="fileChannel")
    }

    @Test
    fun raf() {
        run(this, time = TT.testRuntime(10), params="raf")
    }

    @Test
    fun mapped() {
        run(this, time = TT.testRuntime(10), params="mapped")
    }

    @Test
    fun mappedSingle() {
        run(this, time = TT.testRuntime(10), params="mappedSingle")
    }
}