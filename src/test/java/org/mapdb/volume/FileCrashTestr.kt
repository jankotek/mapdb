package org.mapdb.volume

import org.junit.Assert
import org.junit.Test
import org.mapdb.CrashJVM
import org.mapdb.TT
import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Created by jan on 3/10/16.
 */
class FileCrashTestr: CrashJVM(){

    override fun createParams() = ""

    override fun verifySeed(startSeed: Long, endSeed: Long, params:String): Long {
        val seed = endSeed
        Assert.assertTrue(File(getTestDir(), "" + seed).exists())
        val f = File(getTestDir(), "/" + seed)
        assertTrue(f.exists())

        return Math.max(startSeed,endSeed)+1;
    }

    override fun doInJVM(startSeed: Long, params:String) {
        var seed = startSeed;

        while(true){
            seed++
            startSeed(seed)

            val f = File(getTestDir(), "/" + seed)
            f.createNewFile()
            commitSeed(seed)
        }
    }

    @Test fun test(){
        val runtime = 4000L + TT.testScale()*60*1000;
        val start = System.currentTimeMillis()
        Companion.run(this, time=runtime, killDelay = 200)
        Assert.assertTrue(System.currentTimeMillis() - start >= runtime)
    }
}
