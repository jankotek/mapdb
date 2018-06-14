package org.mapdb.store

import com.google.common.io.Files
import java.io.File
import java.nio.channels.FileChannel


class StoreDirectTest: FileStoreTest(){

    override fun openStore(f: File): StoreDirect {
        if(!f.exists())
            f.writeBytes(ByteArray(0))
        val b = Files.map(f, FileChannel.MapMode.READ_WRITE, StoreDirect.blockSize*100)
        return StoreDirect(b=b)
    }


}