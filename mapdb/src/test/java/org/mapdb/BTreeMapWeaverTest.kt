package org.mapdb

import com.google.testing.threadtester.*
import org.junit.Test
import java.util.concurrent.atomic.AtomicInteger
import org.junit.Assert.*


class BTreeMapWeaverTest {

    val DEBUG = false;

    fun classes() = listOf(HTreeMap::class.java,  IndexTreeLongLongMap::class.java, IndexTreeListJava::class.java)

    companion object{
        fun mapCreate():BTreeMap<Int?,Int?>{
            val map = BTreeMap.make(keySerializer=Serializer.INTEGER, valueSerializer=Serializer.INTEGER)
            for(i in 0 until 100){
                map.put(i, i*10)
            }
            return map;
        }
    }

    @Test fun putIfAbsent() {
        if(TT.shortTest())
            return;

        class PutIfAbsent {

            var map = mapCreate()
            val counter = AtomicInteger()

            @ThreadedBefore
            fun before() {
                map = mapCreate()
            }

            @ThreadedMain
            fun main() {
                val old = map.putIfAbsent(1000, 1000)
                if(old!=null)
                    counter.addAndGet(old)
            }

            @ThreadedSecondary
            fun secondary() {
                val old = map.putIfAbsent(1000, 1000)
                if(old!=null)
                    counter.addAndGet(old)
            }

            @ThreadedAfter
            fun after() {
                assertEquals(1000, counter.get())
                assertEquals(101, map!!.size)
                assertTrue(map.contains(1000))
            }

        }

        val runner = AnnotatedTestRunner()
        runner.setMethodOption(MethodOption.ALL_METHODS, null)
        runner.setDebug(DEBUG)
        runner.runTests(PutIfAbsent::class.java, classes())
    }


    @Test fun putIfAbsentBoolean() {
        if(TT.shortTest())
            return;


        class PutIfAbsent {

            var map = mapCreate()
            val counter = AtomicInteger()

            @ThreadedBefore
            fun before() {
                map = mapCreate()
            }

            @ThreadedMain
            fun main() {
                if (map.putIfAbsentBoolean(1000, 1000))
                    counter.incrementAndGet()
            }

            @ThreadedSecondary
            fun secondary() {
                if (map.putIfAbsentBoolean(1000, 1000))
                    counter.incrementAndGet()
            }

            @ThreadedAfter
            fun after() {
                assertEquals(1, counter.get())
                assertEquals(101, map!!.size)
                assertTrue(map.contains(1000))
            }

        }

        val runner = AnnotatedTestRunner()
        runner.setMethodOption(MethodOption.ALL_METHODS, null)
        runner.setDebug(DEBUG)
        runner.runTests(PutIfAbsent::class.java, classes())
    }

    @Test fun remove() {
        if(TT.shortTest())
            return;

        class Remove{

            var map = mapCreate()
            val counter = AtomicInteger()

            @ThreadedBefore
            fun before() {
                map = mapCreate()
            }

            @ThreadedMain
            fun main() {
                val old = map.remove(1)
                if(old!=null)
                    counter.addAndGet(old)
            }

            @ThreadedSecondary
            fun secondary() {
                val old = map.remove(1)
                if(old!=null)
                    counter.addAndGet(old)
            }

            @ThreadedAfter
            fun after() {
                assertEquals(10, counter.get())
                assertEquals(99, map.size)
                assertTrue(map.containsKey(1).not())
            }

        }

        val runner = AnnotatedTestRunner()
        runner.setMethodOption(MethodOption.ALL_METHODS, null)
        runner.setDebug(DEBUG)
        runner.runTests(Remove::class.java, classes())
    }



    @Test fun remove2() {
        if(TT.shortTest())
            return;

        class Remove2{

            var map = mapCreate()
            val counter = AtomicInteger()

            @ThreadedBefore
            fun before() {
                map = mapCreate()
            }

            @ThreadedMain
            fun main() {
                if(map.remove(1,10))
                    counter.incrementAndGet()
            }

            @ThreadedSecondary
            fun secondary() {
                if(map.remove(1,10))
                    counter.incrementAndGet()
            }

            @ThreadedAfter
            fun after() {
                assertEquals(1, counter.get())
                assertEquals(99, map.size)
                assertTrue(map.containsKey(1).not())
            }

        }

        val runner = AnnotatedTestRunner()
        runner.setMethodOption(MethodOption.ALL_METHODS, null)
        runner.setDebug(DEBUG)
        runner.runTests(Remove2::class.java, classes())
    }


    @Test fun replace2() {
        if(TT.shortTest())
            return;

        class Weaved{

            var map = mapCreate()
            val counter = AtomicInteger()

            @ThreadedBefore
            fun before() {
                map = mapCreate()
            }

            @ThreadedMain
            fun main() {
                if(map.replace(1, 10, 111))
                    counter.incrementAndGet()
            }

            @ThreadedSecondary
            fun secondary() {
                if(map.replace(1, 10, 111))
                    counter.incrementAndGet()
            }

            @ThreadedAfter
            fun after() {
                assertEquals(1, counter.get())
                assertEquals(100, map.size)
                assertEquals(111, map[1])
            }

        }

        val runner = AnnotatedTestRunner()
        runner.setMethodOption(MethodOption.ALL_METHODS, null)
        runner.setDebug(DEBUG)
        runner.runTests(Weaved::class.java, classes())
    }

    @Test fun replace() {
        if(TT.shortTest())
            return;

        class Weaved{

            var map = mapCreate()
            val counter = AtomicInteger()

            @ThreadedBefore
            fun before() {
                map = mapCreate()
            }

            @ThreadedMain
            fun main() {
                val old = map.replace(1, 111)
                if(old!=null)
                    counter.addAndGet(old)
            }

            @ThreadedSecondary
            fun secondary() {
                val old = map.replace(1, 111)
                if(old!=null)
                    counter.addAndGet(old)
            }

            @ThreadedAfter
            fun after() {
                assertEquals(121, counter.get())
                assertEquals(100, map.size)
                assertEquals(111, map[1])
            }

        }

        val runner = AnnotatedTestRunner()
        runner.setMethodOption(MethodOption.ALL_METHODS, null)
        runner.setDebug(DEBUG)
        runner.runTests(Weaved::class.java, classes())
    }


}