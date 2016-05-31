package org.mapdb.crash

import org.mapdb.volume.Volume
import org.junit.Assert.*
import org.junit.Test
import org.mapdb.TT
import org.mapdb.*

class WALCrashTest: CrashJVM(){

    override fun doInJVM(startSeed: Long, params: String) {
        val file = getTestDir().path+"/wal"
        val wal = WriteAheadLog(file, CC.DEFAULT_FILE_VOLUME_FACTORY, 0L, false)
        var seed = startSeed;
        while(true){
            seed++
            startSeed(seed)
            val bb = TT.randomByteArray(31,seed.toInt())
            wal.walPutLong(8L,seed)
            wal.walPutByteArray(16L, bb, 0, bb.size)
            commitSeed(seed)
        }
    }

    override fun verifySeed(startSeed: Long, endSeed: Long, params: String): Long {
        val file = getTestDir().path+"/wal"
        val wal = WriteAheadLog(file, CC.DEFAULT_FILE_VOLUME_FACTORY, 0L, false)
        var lastLong:Long?=null
        var lastBB:ByteArray?=null
        wal.replayWAL(object: WriteAheadLog.WALReplay by WriteAheadLog.NOREPLAY{
            override fun writeLong(offset: Long, value: Long) {
                lastLong=value
            }

            override fun writeRecord(recid: Long, walId: Long, vol: Volume, volOffset: Long, length: Int) {
                val bb = ByteArray(length)
                vol.getData(volOffset,bb,0,bb.size)
                lastBB = bb
            }
        })

        if(lastLong==null){
            assertNull(lastBB)
            return endSeed+10
        }

        assertTrue(lastLong!! in endSeed-1..endSeed)
        assertArrayEquals(TT.randomByteArray(31,lastLong!!.toInt()), lastBB)

        return endSeed+10
    }

    @Test fun run(){
        if(TT.shortTest())
            return
        run(this)
    }
}