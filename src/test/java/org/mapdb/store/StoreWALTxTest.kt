package org.mapdb.store

import org.mapdb.StoreTx

class StoreWALTxTest:StoreTxTest(){

    override fun open(): StoreTx {
        return StoreWAL.make()
    }

}