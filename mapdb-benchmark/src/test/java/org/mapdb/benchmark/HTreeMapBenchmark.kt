package org.mapdb.benchmark

import org.junit.Test
import org.junit.Assert.*
import org.mapdb.*
import java.util.concurrent.ConcurrentHashMap

class HTreeMapBenchmark{

    val size = 1e7.toInt()


    @Test fun ConcurrentHashMap() {
        run(ConcurrentHashMap(size), "ConcurrentHashMap", size)
    }

    @Test fun MapDB20() {
        run(org.mapdb20.DBMaker.memoryDB()
                .transactionDisable()
                .allocateStartSize(1024 * 1024 * 512)
                .make()
                .hashMap("map", org.mapdb20.Serializer.INTEGER, org.mapdb20.Serializer.INTEGER),
                "MapDB2_HTreeMap", size)
    }

    @Test fun bench(){
        val bools = booleanArrayOf(true,false)

        for(keyInline in bools )
        for(valueInline in bools )
        {
            val name = "HTreeMap_keyInline=${keyInline}_valueInline=${valueInline}"
            var maker =
                    DBMaker
                            .memoryDB()
                            .allocateStartSize(1024*1024*512)
                            .make()
                            .hashMap("map")
                            .keySerializer(Serializer.INTEGER)
                            .valueSerializer(Serializer.INTEGER)
//                            .layout(0,6,4)

            if(valueInline)
                maker = maker.valueInline()


            val map = maker.create()
            run(map, name, size)
        }
    }


    fun run(map:MutableMap<Int?,Int?>, name:String, size:Int){

        Bench.bench(name+"_insert") {
            Bench.stopwatch {
                for (i in 0 until size) {
                    map.put(i, i)
                }
            }
        }

        Bench.bench(name+"_get") {
            Bench.stopwatch {
                for (i in 0 until size) {
                    map[i]
                }
            }
        }

        Bench.bench(name+"_update") {
            Bench.stopwatch {
                for (i in 0 until size) {
                    map.put(i, i*10)
                }
            }
        }

    }
}