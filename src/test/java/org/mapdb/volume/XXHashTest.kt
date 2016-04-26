package org.mapdb.volume

import net.jpountz.xxhash.XXHashFactory
import org.junit.Test
import java.util.*
import org.junit.Assert.assertEquals

/**
 * Tests XXHashing
 */
class XXHashTest{

    @Test fun stream_compatible(){
        val b = ByteArray(1000)
        Random().nextBytes(b)
        val seed = 1L

        val s = XXHashFactory.safeInstance().newStreamingHash64(seed)
        val h = XXHashFactory.safeInstance().hash64()

        //general compatibility
        val totalHash = h.hash(b,0,b.size,seed);
        s.update(b, 0, b.size)
        assertEquals(totalHash, s.value)

        //split in middle
        s.reset()
        s.update(b, 0, 500)
        s.update(b, 500, b.size-500)
        assertEquals(totalHash, s.value)

        //update 10 bytes at a time
        s.reset()
        for(offset in 0 until b.size step 10){
            s.update(b, offset, 10)
        }
        assertEquals(totalHash, s.value)

    }
}