package org.mapdb

import org.eclipse.collections.impl.map.mutable.primitive.LongIntHashMap
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.util.*
import org.junit.Assert.*


/**
 * Randomly allocates long stacks
 */
@RunWith(Parameterized::class)
class StoreDirect_LongStackAllocTest(
    val data: Config
){

    data class Config(
        val period:Int,
        val periodSize:Int,
        val randomSeed:Long,
        val maxRecSize:Int,
        val largeSizePlus:Int,
        val largeSizeProbability:Double,
        val largeSizeMultiple:Double,
        val updateProb:Double
    )

    companion object{

        @Parameterized.Parameters
        @JvmStatic
        fun params():Iterable<Any>{
            val ret = ArrayList<Any>()
            for(period in intArrayOf(1,6,20))
            for(periodSize in longArrayOf(16,  160512, 1600,
                    StoreDirectJava.LONG_STACK_MAX_SIZE, StoreDirectJava.LONG_STACK_MIN_SIZE, StoreDirectJava.LONG_STACK_PREF_SIZE ).toSet())
            for(randomSeed in 0L..0)
            for(maxRecSize in intArrayOf(1024, 64, 1024*32))
            for(largeSizePlus in intArrayOf(0, 128*1024*1024))
            for(largeSizeProbability in doubleArrayOf(0.0, 0.3, 0.9))
            for(largeSizeMultiple in doubleArrayOf(1.0, 0.3))
            for(updateProb in doubleArrayOf(0.1, 0.6))
            {
                val data = Config(
                    period = period,
                    periodSize = periodSize.toInt(),
                    randomSeed = randomSeed,
                    maxRecSize = maxRecSize,
                    largeSizePlus = largeSizePlus,
                    largeSizeProbability = largeSizeProbability,
                    largeSizeMultiple = largeSizeMultiple,
                    updateProb = updateProb
                )

                ret.add(arrayOf(data))

                if(TT.shortTest())
                    return ret;
            }

            return ret;
        }
    }

    @Test fun run(){
        val size = 200000
        val r = Random(data.randomSeed)

        val store = StoreDirect.make(isThreadSafe = false, concShift = 0)

        val recids = LongIntHashMap()

        loop@
        for(i in 0 until size){
            var ba:ByteArray? = null;
            var periodRecid:Long = 0
            if( i % data.period == 0) {
                ba = TT.randomByteArray(data.periodSize, seed = r.nextInt())
                periodRecid = store.put(ba,Serializer.BYTE_ARRAY_NOSIZE)
            }

            var size2 = r.nextInt(1600)
            if(r.nextInt(1000)<1000*data.largeSizeProbability)
                size2 = ((size2 + data.largeSizePlus) * data.largeSizeMultiple).toInt()

            if(recids.isEmpty.not() && r.nextInt(1000)<1000*data.updateProb){
                //do update
                val recid = recids.keySet().longIterator().next()
                val sizeOld = recids.get(recid)

                //compare old
                val old = store.get(recid, Serializer.BYTE_ARRAY_NOSIZE)!!
                assertEquals(sizeOld, old.size)
                TT.assertAllZero(old)

                store.update(recid, ByteArray(size2), Serializer.BYTE_ARRAY_NOSIZE)
                recids.put(recid, size2)
            }else{
                //do insert instead
                val recid = store.put(ByteArray(size2), Serializer.BYTE_ARRAY_NOSIZE)
                recids.put(recid, size2)
            }

            if(ba!=null){
                val ba2 = store.get(periodRecid, Serializer.BYTE_ARRAY_NOSIZE)
                assertTrue(Arrays.equals(ba, ba2))
                store.delete(periodRecid, Serializer.BYTE_ARRAY_NOSIZE)
            }

            if(store.fileTail>1024*1024*512)
                break@loop
        }

        store.verify()
        recids.forEachKeyValue { recid, size ->
            val old = store.get(recid, Serializer.BYTE_ARRAY_NOSIZE)!!
            assertEquals(size, old.size)
            TT.assertAllZero(old)
        }
    }


}
