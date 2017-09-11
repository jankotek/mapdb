package org.mapdb.tree

import io.kotlintest.matchers.fail
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.mapdb.*
import org.mapdb.serializer.GroupSerializer
import org.mapdb.store.StoreTrivial
import java.io.IOException
import java.util.*


@RunWith(Parameterized::class)
class BTreeMap_PumpTest(
        val mapMaker2:()->DB.TreeMapMaker<Any?, Any?>
) {

    companion object {

        @Parameterized.Parameters
        @Throws(IOException::class)
        @JvmStatic
        fun params(): Iterable<Any> {
            val ret = ArrayList<Any>()

            val bools = if(TT.shortTest()) TT.boolsFalse else TT.bools

            for(valueInline in bools)
            for(otherComparator in bools)
            for(small in bools)
            for(generic in bools)
            for(storeType in 0..2)
            for(isThreadSafe in bools)
            for(counter in bools){
                ret += {
                    val db: DB = when (storeType) {
                        0 -> {
                            val d = DBMaker.heapDB()
                            if (!isThreadSafe) d.concurrencyDisable()
                            d.make()
                        }
                        1 -> DB(StoreTrivial(), isThreadSafe = isThreadSafe, storeOpened = false)
                        2 -> {
                            val d = DBMaker.memoryDB()
                            if (!isThreadSafe) d.concurrencyDisable()
                            d.make()
                        }
                        else -> throw AssertionError()
                    }
                    val keySer = if (otherComparator) {
                        // map should use Comparator for comparations, not this serializers
                        object : GroupSerializer<Int> by Serializer.INTEGER {
                            override fun valueArrayBinaryGet(input: DataInput2?, keysLen: Int, pos: Int): Int {
                                fail("should not be used")
                            }

                            //TODO this needs more testing
//                        override fun valueArrayBinarySearch(key: Int?, input: DataInput2?, keysLen: Int, comparator: Comparator<*>?): Int {
//                            fail()
//                        }

                            override fun compare(first: Int?, second: Int?): Int {
                                fail("should not be used")
                            }

                            override fun valueArraySearch(keys: Any?, key: Int?): Int {
                                fail("should not be used")
                            }

//                        override fun valueArraySearch(keys: Any?, key: Int?, comparator: Comparator<*>?): Int {
//                            fail()
//                        }
                        }
                    } else Serializer.INTEGER

                    val m =
                            if (generic) db.treeMap("aa")
                            else db.treeMap("aa", keySer, Serializer.STRING)


                    val comparator = Serializer.INTEGER

                    m.comparator(comparator)

                    if (small) m.maxNodeSize(6)
                    if (!valueInline) m.valuesOutsideNodesEnable()

                    if (counter) m.counterEnable()

                    m
                }
            }

            return ret
        }
    }

    @Test fun test(){
        val limit = (100 + TT.testScale()*1e5).toInt()
        val mapMaker = mapMaker2()
        val sink = mapMaker.createFromSink()
        (0 until limit).forEach {
            sink.put(it, it.toString())
        }
        val map = sink.create() as BTreeMap

        (0 until limit).forEach {
            assert(it.toString()==map[it])
        }

        val iter = map.entryIterator()
        (0 until limit).forEach {
            val e = iter.next()
            assert(it == e.key)
            assert(it.toString() == e.value)
        }

        val iterRev = map.descendingKeyIterator()
        (limit-1 downTo 0).forEach {
            assert(it == iterRev.next())
        }

        assert(map.size == limit)
        assert(map.count{true} == limit)

        map.verify()
    }

}