package org.mapdb.issues

import org.junit.Assert.assertEquals
import org.junit.Test
import org.mapdb.*
import org.mapdb.DBMaker.StoreType.*
import org.mapdb.tree.BTreeMap
import org.mapdb.tree.HTreeMap
import java.io.Closeable
import java.util.*
import java.util.stream.IntStream

@org.junit.runner.RunWith(org.junit.runners.Parameterized::class)
class ParallelMaps(val fab:()-> MutableMap<Any,Any>) {

    companion object {

        @org.junit.runners.Parameterized.Parameters
        @JvmStatic
        fun params(): Iterable<Any> {
            val ret = ArrayList<Any>()
            val bools = booleanArrayOf(true, false)

            for(store in DBMaker.StoreType.values())
            for(intSer in bools)
            for(counter in bools)
            for(externalVals in bools) {

                val db = {when (store) {
                    fileMMap -> DBMaker.tempFileDB().fileMmapEnable()
                    fileRaf -> DBMaker.tempFileDB()
                    fileChannel -> DBMaker.tempFileDB().fileChannelEnable()
                    onheap -> DBMaker.heapDB()
                    bytearray -> DBMaker.memoryDB()
                    directbuffer -> DBMaker.memoryDirectDB()
                }.make()}

                // hashMap
                ret.add({
                    var maker = db().hashMap("aa")
                    if(intSer)
                        maker.keySerializer(Serializer.INTEGER).valueSerializer(Serializer.INTEGER)
                    if(counter)
                        maker.counterEnable()
                    maker.create()
                })
                for(nodeSize in intArrayOf(4,6,12,32,128,1024)){
                    ret.add({
                        var maker =  db().treeMap("map").maxNodeSize(nodeSize)
                        if(intSer)
                            maker.keySerializer(Serializer.INTEGER).valueSerializer(Serializer.INTEGER)
                        if(counter)
                            maker.counterEnable()
                        maker.create()
                    })
                }

            }
            return ret.map{arrayOf(it)}
        }
    }

    @Test
    fun main() {
        if (TT.shortTest())
            return

        for (i in 0..99) {
            testing()
        }
    }

    private fun testing() {

        val tmp = fab();

        if(tmp is ConcurrencyAware)
            tmp.assertThreadSafe()

        val size = 1000
        IntStream.rangeClosed(1, size).parallel().forEach { i -> tmp.put(i, 11) }

        assertEquals(size, tmp.size)

        if(tmp is Verifiable)
            tmp.verify()
        if(tmp is BTreeMap)
            tmp.store.verify()
        if(tmp is HTreeMap)
            tmp.stores.toSet().forEach { it.verify() }

        if(tmp is Closeable)
            tmp.close()
    }
}
