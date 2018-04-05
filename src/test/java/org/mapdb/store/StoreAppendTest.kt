package org.mapdb.store

import java.io.File

class StoreAppendTest : StoreTest() {

    override fun openStore(): MutableStore {
        val f = File.createTempFile("mapdb","adasdsa")
        f.delete()

        return StoreAppend(file=f.toPath())
    }

}