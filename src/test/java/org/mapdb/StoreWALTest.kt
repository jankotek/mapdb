package org.mapdb

import org.junit.Assert.*
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

}