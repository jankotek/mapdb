package org.mapdb

import org.junit.Assert.*
import org.junit.Test
import java.io.File

/**
 * Created by jan on 3/22/16.
 */
class StoreWALTest: StoreDirectAbstractTest() {

    override fun openStore(file: File): StoreWAL {
        return StoreWAL.make(file=file.path)
    }

    override fun openStore(): StoreWAL {
        return StoreWAL.make()
    }


    @Test override fun delete_after_close(){
        val dir = TT.tempDir()
        val store = StoreWAL.make(dir.path+"/aa",deleteFilesAfterClose = true)
        store.put(11, Serializer.INTEGER)
        store.commit()
        store.put(11, Serializer.INTEGER)
        store.commit()
        assertNotEquals(0, dir.listFiles().size)
        store.close()
        assertEquals(0, dir.listFiles().size)
    }
}