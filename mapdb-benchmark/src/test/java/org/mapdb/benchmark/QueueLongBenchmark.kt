package org.mapdb.benchmark

import org.junit.Test
import org.mapdb.*


class QueueLongBenchmark{

    @Test fun insert(){
        val db = DB(store=StoreDirect.make(), storeOpened = false)
        val map = db.hashMap("aa", keySerializer = Serializer.LONG, valueSerializer = Serializer.LONG).create()
        val max = 1e7.toLong()

//        while(true){
//            for(i in 1L .. max){
//                map.put(i,i)
//            }
//        }

    }
}
