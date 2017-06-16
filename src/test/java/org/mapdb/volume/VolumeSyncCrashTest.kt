package org.mapdb.volume

import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet
import org.junit.Assert.assertEquals
import org.junit.Test
import org.mapdb.TT
import org.mapdb.crash.CrashJVM
import org.mapdb.util.DataIO
import java.io.File
import java.util.*

/**
 * Checks if [Volume.sync()] really flushes disk cache, it should survive JVM crash...
 */
abstract class VolumeSyncCrashTest(val volfab: VolumeFactory) : CrashJVM(){

    class RAF       : VolumeSyncCrashTest(RandomAccessFileVol.FACTORY)
    class FileChan  : VolumeSyncCrashTest(FileChannelVol.FACTORY)
    class MMAP      : VolumeSyncCrashTest(MappedFileVol.FACTORY)

    val fileSize = 4 * 1024*1024
    val writeValues = 100;


    fun fileForSeed(seed:Long) = getTestDir().toString()+"/"+seed;

    override fun doInJVM(startSeed: Long, params: String) {
        var seed = startSeed
        while(true){
            seed++
            val vol = volfab.makeVolume(fileForSeed(seed), false)
            vol.ensureAvailable(fileSize.toLong())
            startSeed(seed)
            val random = Random(seed)
            val used = LongHashSet();
            for(i in 0 until writeValues){
                val offset = DataIO.roundDown(random.nextInt(fileSize - 8 ),8).toLong()

                if(!used.add(offset))
                    continue;
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
        val used = LongHashSet();
        for(i in 0 until writeValues){
            val offset = DataIO.roundDown(random.nextInt(fileSize - 8 ),8).toLong()
            if(!used.add(offset))
                continue;
            val value = random.nextLong();
            assertEquals(value, vol.getLong(offset));
        }

        vol.close()

        //delete old data
        getTestDir().listFiles().filter{ it.isFile }.forEach { it.delete() }

        return endSeed+10
    }

    @Test
    fun run(){
        if (TT.shortTest())
            return

        CrashJVM.Companion.run(this, time = org.mapdb.TT.testRuntime(10))
    }
}