package org.mapdb.volume

import org.junit.Assert.assertTrue
import org.junit.Test
import org.mapdb.TT
import org.mapdb.crash.CrashJVM
import java.io.File

/**
 * Created by jan on 3/10/16.
 */
class FileCrashTestr: CrashJVM(){


    override fun verifySeed(startSeed: Long, endSeed: Long, params:String): Long {
        val seed = endSeed
        assertTrue(File(getTestDir(), "" + seed).exists())
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
