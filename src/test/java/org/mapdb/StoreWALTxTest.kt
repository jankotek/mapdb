package org.mapdb

class StoreWALTxTest:StoreTxTest(){

    override fun open(): StoreTx {
        return StoreWAL.make()
    }

}