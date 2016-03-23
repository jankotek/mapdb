package org.mapdb

import org.junit.Assert.*
import java.io.File

/**
 * Created by jan on 3/22/16.
 */
class StoreWALTest: StoreReopenTest() {

    override fun openStore(file: File): Store {
        return StoreWAL.make(file=file.path)
    }

    override fun openStore(): Store {
        return StoreWAL.make()
    }

}