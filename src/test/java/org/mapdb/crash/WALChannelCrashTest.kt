package org.mapdb.crash

import org.junit.Test
import java.io.*
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import org.junit.Assert.*
import org.mapdb.util.DataIO
import org.mapdb.TT


/**
 * Created by jan on 3/16/16.
 */
class WALChannelCrashTest: CrashJVM(){


    override fun doInJVM(startSeed: Long, params: String) {
        val f = File(getTestDir().path, "aaa")
        val out = FileChannel.open(f.toPath(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)
        val b = ByteBuffer.allocate(8)
        var seed = startSeed
        while(true){
            seed++
            startSeed(seed)
            b.rewind()
            b.putLong(seed)
            var written = 0;
            while(written<8){
                written+=out.write(b)
            }
            out.force(false)
            commitSeed(seed)
        }

    }

    override fun verifySeed(startSeed: Long, endSeed: Long, params: String): Long {
        val f = getTestDir().path+"/aaa"
        val ins = BufferedInputStream(FileInputStream(f))
        val b = ByteArray(8)
        var lastSeed = 0L
        while(true){
            try{
                DataIO.readFully(ins, b)
                lastSeed = DataIO.getLong(b,0)
            }catch(e: IOException){
                break
            }
        }
        assertTrue(lastSeed == endSeed || lastSeed==endSeed+1)

        File(f).delete()
        return endSeed+10
    }

    @Test fun run(){
        if(TT.shortTest())
            return
        run(this, killDelay = 300)
    }
}

