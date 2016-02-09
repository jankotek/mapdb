package org.mapdb.benchmark

import org.junit.Test
import org.mapdb.*

class SortedTableMapBenchmark{

    val size = HTreeMapBenchmark.size

    @Test fun get(){
        val consumer = SortedTableMap.import(
                keySerializer = Serializer.INTEGER,
                valueSerializer = Serializer.INTEGER,
                volume = Volume.ByteArrayVol.FACTORY.makeVolume(null, false)
        )
        for(i in 0 until size ){
            consumer.take(Pair(i,i))
        }
        val map = consumer.finish()

        Bench.bench("SortedTableMapBenchmark_get") {
            Bench.stopwatch {
                for (i in 0 until size) {
                    map[i]
                }
            }
        }


    }
}
