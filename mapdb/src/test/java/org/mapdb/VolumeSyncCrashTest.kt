package org.mapdb

import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.io.File
import java.util.*
import org.junit.Assert.*

/**
 * Checks if [Volume.sync()] really flushes disk cache, it should survive JVM crash...
 */
abstract class VolumeSyncCrashTest(val volfab:Volume.VolumeFactory) :CrashJVM(){

    class RAF       :VolumeSyncCrashTest(Volume.RandomAccessFileVol.FACTORY)
    class FileChan  :VolumeSyncCrashTest(Volume.FileChannelVol.FACTORY)
    class MMAP      :VolumeSyncCrashTest(Volume.MappedFileVol.FACTORY)

    val fileSize = 4 * 1024*1024
    val writeValues = 100;

    override fun createParams(): String {
        return ""
    }

    fun fileForSeed(seed:Long) = getTestDir().toString()+"/"+seed;

    override fun doInJVM(startSeed: Long, params: String) {
        var seed = startSeed
        while(true){
            seed++
            val vol = volfab.makeVolume(fileForSeed(seed), false)
            vol.ensureAvailable(fileSize.toLong())
            startSeed(seed)
            val random = Random(seed)
            for(i in 0 until writeValues){
                val offset = random.nextInt(fileSize - 8 ).toLong()
                val value = random.nextLong();
                vol.putLong(offset, value);
            }
            vol.sync()
            commitSeed(seed)
            //delete prev file to keep disk space usage low
            File(fileForSeed(seed - 1)).delete()
        }
    }

    override fun verifySeed(startSeed: Long, endSeed: Long, params: String): Long {
        if(endSeed==-1L)
            return startSeed+10;

        val file = fileForSeed(endSeed);
        val vol = volfab.makeVolume(file, true)

        val random = Random(endSeed)

        for(i in 0 until writeValues){
            val offset = random.nextInt(fileSize - 8 ).toLong()
            val value = random.nextLong();
            assertEquals(value, vol.getLong(offset));
        }

        vol.close()

        //delete old data
        getTestDir().listFiles().filter{ it.isFile }.forEach { it.delete() }

        return endSeed+10
    }

    @Test @Ignore //TODO crash tests
    fun run(){
        CrashJVM.run(this, time = TT.testRuntime(10))
    }
}