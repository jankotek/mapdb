package org.mapdb.util

import org.mapdb.ser.Serializers
import org.mapdb.store.MutableStore

class RecidRecord(
        private val store: MutableStore,
        private val rootRecid:Long
    ){

    fun set(recid:Long){
        store.update(rootRecid, Serializers.RECID, recid)
    }

    fun get():Long{
        return store.get(rootRecid, Serializers.RECID)!!
    }

}