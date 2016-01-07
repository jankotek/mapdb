package org.mapdb

import org.junit.Test
import java.io.File
import org.junit.Assert.*

/**
 * Check of commits are durable and survive JVM crash (kill PID -9)
 */
abstract class StoreCrashTest:CrashJVM(){
    abstract fun openStore(file: File):Store;

    override fun createParams():String{
        val store = openStore(File(getTestDir(),"store"))
        val recid = store.put(0L, Serializer.LONG)
        store.commit()
        store.close()
        return recid.toString()
    }


    override fun doInJVM(startSeed: Long, params:String) {
        val store = openStore(File(getTestDir(), "store"))

        val recid = params.toLong()
        var seed = startSeed;
        while (true) {
            seed++;
            startSeed(seed)
            store.update(recid, seed, Serializer.LONG)
            store.commit()
            commitSeed(seed)
        }
    }

    override fun verifySeed(startSeed: Long, endSeed: Long, params:String): Long {
        val recid = params.toLong()
        val store = openStore(File(getTestDir(), "store"))
        val seed = store.get(recid, Serializer.LONG)!!
        store.close()
        assertTrue(seed<=startSeed)
        assertTrue(endSeed==-1L || seed>=endSeed);

        return seed;
    }

    @Test fun crashTest(){
        CrashJVM.run(this, time = TT.testRuntime(6))
    }
}

class StoreTrivialCrashTest:StoreCrashTest(){

    override fun openStore(file: File):Store {
        return StoreTrivialTx(file);
    }



}
