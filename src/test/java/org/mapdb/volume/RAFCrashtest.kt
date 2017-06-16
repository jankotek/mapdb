package org.mapdb.volume

import org.junit.Assert.assertEquals
import org.junit.Test
import org.mapdb.TT
import org.mapdb.crash.CrashJVM
import java.io.*
import java.util.*


class RAFCrashtest: CrashJVM(){

    val max = 8L//4L*1024*1024
    val count = 100;
    fun fileForSeed(seed:Long) = getTestDir().toString()+"/"+seed;

    override fun doInJVM(startSeed: Long, params: String) {
        var seed = startSeed
        while (true) {
            seed++
            val file = fileForSeed(seed)
            val raf = RandomAccessFile(file, "rw")
            raf.setLength(max)

            val random = Random(seed)
            for(i in 0 until count) {
                //raf.seek(random.nextInt(max.toInt() - 8).toLong())
                raf.seek(0)
                raf.writeLong(random.nextLong())
            }
            raf.fd.sync()
            raf.close()
            commitSeed(seed)
            //delete prev file to keep disk space usage low
            File(fileForSeed(seed - 1)).delete()

        }
    }

    override fun verifySeed(startSeed: Long, endSeed: Long, params: String): Long {
        val file = fileForSeed(endSeed)
        val raf = RandomAccessFile(file, "r")
        assertEquals(max, raf.length())
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
    fun run() {
        if (TT.shortTest())
            return

        run(this, time = TT.testRuntime(10))
    }
}