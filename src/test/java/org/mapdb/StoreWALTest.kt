package org.mapdb

import org.junit.Assert.*

/**
 * Created by jan on 3/22/16.
 */
class StoreWALTest: StoreTest() {
    override fun openStore(): Store {
        return StoreWAL.make()
    }

}