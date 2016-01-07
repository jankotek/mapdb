package org.mapdb.benchmark

import org.junit.Test
import org.mapdb.IndexTreeLongLongMap

class IndexTreeLongLongMapBench {

    @Test fun create(){
        Bench.bench{
            val indexTree = IndexTreeLongLongMap.make()
            Bench.stopwatch {
                for(i in 0L..1e6.toLong()){
                    indexTree.put(i,i*10)
                }
            }
        }
    }

}