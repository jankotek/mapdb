package org.mapdb.volume

import org.junit.Assert.*
import org.junit.Test
import org.mapdb.TT
import org.mapdb.crash.CrashJVM
import java.io.*
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption.*


class FileChannelCrashTest: CrashJVM(){

    override fun verifySeed(startSeed: Long, endSeed: Long, params:String): Long {
        println("verify")
        val seed = endSeed
        assertTrue(File(getTestDir(), "" + seed+"aa").exists())
        val r = RandomAccessFile(getTestDir().path + "/" + seed+"aa","r")
        r.seek(0)
        val v = r.readLong()
        assertEquals(seed, v)
        r.close()


        return Math.max(startSeed,endSeed)+1;
    }

    override fun doInJVM(startSeed: Long, params:String) {
        var seed = startSeed;

        while(true){
            seed++
            startSeed(seed)
            val bb = ByteBuffer.allocate(8);
            bb.putLong(seed)

            val f = File(getTestDir(), "/" + seed+"aa")
            val c = FileChannel.open(f.toPath(),
                    CREATE, READ, WRITE)
            var pos = 0;
            while(pos!=8) {
                pos+=c.write(bb, 0L)
            }
            c.force(false)
            c.close()
            assertEquals(8, f.length())
            commitSeed(seed)
        }
    }

    @Test
    fun test(){
        if (TT.shortTest())
            return

        val runtime = 4000L + TT.testScale()*60*1000;
        val start = System.currentTimeMillis()
        Companion.run(this, time=runtime, killDelay = 200)
        assertTrue(System.currentTimeMillis() - start >= runtime)
    }
}
