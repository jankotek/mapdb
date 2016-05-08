package org.mapdb.volume

import org.mapdb.*
import org.junit.Assert.*
import org.junit.Test

class FileLockTest{

    @Test fun lock_disable(){
        val f = TT.tempFile()
        val c = FileChannelVol.FACTORY.makeVolume(f.path, false)
        val c2 = FileChannelVol.FACTORY.makeVolume(f.path, false, -1)
        f.delete()
    }


    @Test(timeout=10000L)
    fun lock_wait(){
        val f = TT.tempFile()
        val c = FileChannelVol.FACTORY.makeVolume(f.path, false)
        TT.fork{
            Thread.sleep(2000)
            c.close()
        }
        val c2 = FileChannelVol.FACTORY.makeVolume(f.path, false, Long.MAX_VALUE)
        c2.close()
        f.delete()
    }
}
