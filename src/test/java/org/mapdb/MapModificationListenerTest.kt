package org.mapdb

import org.junit.Assert
import org.junit.Test
import org.mapdb.serializer.Serializers
import org.mapdb.tree.BTreeMap
import org.mapdb.tree.HTreeMap
import java.util.*

/**
 * Tests map with modification listener
 */

abstract class MapModificationListenerTest:MapModificationListener<Int,String> {

    abstract fun makeMap(): DBConcurrentMap<Int, String>

    val map = makeMap()

    var lcounter: Int = 0;
    var lkey: Int? = null;
    var loldValue: String? = null
    var lnewValue: String? = null
    var lexpired: Boolean? = null

    init {
        map.put(1, "1")
        map.put(2, "2")
        map.put(3, "3")
        lcounter=0
        lkey = null
        loldValue=null
        lnewValue=null
        lexpired=null
    }


    fun assertListener(counter: Int, key: Int?, oldVal: String?, newValue: String?, expired: Boolean?) {
        Assert.assertEquals(counter, lcounter)
        Assert.assertEquals(key, lkey)
        Assert.assertEquals(oldVal, loldValue)
        Assert.assertEquals(newValue, lnewValue)
        Assert.assertEquals(expired, lexpired)
    }

    override fun modify(key: Int, oldValue: String?, newValue: String?, triggered: Boolean) {
        lcounter++
        this.lkey = key
        this.loldValue = oldValue
        this.lnewValue = newValue
        this.lexpired = triggered
    }


    @Test fun listener_put() {
        map.put(0, "0")
        assertListener(1, 0, null, "0", false)
        map.put(0, "1")
        assertListener(2, 0, "0", "1", false)
        map.put(0, "1")
        assertListener(3, 0, "1", "1", false)
    }

    @Test fun listener_put_all() {
        map.putAll(mapOf(Pair(0, "0")))
        assertListener(1, 0, null, "0", false)
        map.putAll(mapOf(Pair(0, "1")))
        assertListener(2, 0, "0", "1", false)
        map.putAll(mapOf(Pair(0, "1")))
        assertListener(3, 0, "1", "1", false)
    }

    @Test fun listener_remove() {
        map.remove(1)
        assertListener(1, 1, "1", null, false)
        map.remove(1)
        assertListener(1, 1, "1", null, false)
    }

    @Test fun listener_clear() {

        map.clear()
        Assert.assertEquals(3, lcounter)
        Assert.assertTrue(lkey in 1..3)
        map.clear()
        Assert.assertEquals(3, lcounter)
    }

    @Test fun listener_putIfAbsent(){

        map.putIfAbsent(1, "2")
        assertListener(0, null, null, null, null)
        map.putIfAbsent(0, "0")
        assertListener(1, 0, null, "0", false)
    }

    @Test fun listener_putIfAbsentBoolean(){

        map.putIfAbsentBoolean(1, "2")
        assertListener(0, null, null, null, null)
        map.putIfAbsentBoolean(0, "0")
        assertListener(1, 0, null, "0", false)
    }


    @Test fun listener_remove2() {
        map.remove(1, "0")
        assertListener(0, null, null, null, null)
        map.remove(1, "1")
        assertListener(1, 1, "1", null, false)
    }

    @Test fun listener_replace() {
        map.replace(1, "0","2")
        assertListener(0, null, null, null, null)
        map.replace(1, "1","2")
        assertListener(1, 1, "1", "2", false)
    }

    @Test fun listener_replace2() {
        map.replace(0, "0")
        assertListener(0, null, null, null, null)
        map.replace(1, "2")
        assertListener(1, 1, "1", "2", false)
    }

    @Test fun listener_iter_remove() {

        val iter = map.keys.iterator()
        val key = iter.next()
        assertListener(0, null, null, null, null)
        iter.remove()
        assertListener(1, key, key.toString(), null, false)
    }

    class HTreeMapModListenerTest:MapModificationListenerTest(){
        override fun makeMap(): DBConcurrentMap<Int, String>  = HTreeMap.make(
                keySerializer = Serializers.INTEGER, valueSerializer = Serializers.STRING,
                modificationListeners = arrayOf(this as MapModificationListener<Int, String>))

    }

    class BTreeMapModListenerTest:MapModificationListenerTest(){
        override fun makeMap(): DBConcurrentMap<Int, String>  = BTreeMap.make(
                keySerializer = Serializers.INTEGER, valueSerializer = Serializers.STRING,
                modificationListeners = arrayOf(this as MapModificationListener<Int, String>))

    }

    @Test fun mapListenersBig() {
        val test = makeMap();

        val max = Math.min(100.0, Math.max(1e8, Math.pow(4.0, TT.testScale().toDouble()))).toInt()
        val r = Random()
        for (i in 0..max - 1) {
            val k = r.nextInt(max / 100)
            val v = ""+(k * 1000)
            var vold: String? = null

            if (test.containsKey(k)) {
                vold = v+"XXX"
                test.put(k, vold)
            }

            test.put(k, v)
            assertListener(lcounter, k, vold, v, false)

            val m = i % 20
            if (m == 1) {
                test.remove(k)
                assertListener(lcounter, k, v,null, false)
            } else if (m == 2) {
                test.put(k, ""+(i * 20))
                assertListener(lcounter, k, v, ""+(i * 20), false)
            } else if (m == 3 && !test.containsKey(i + 1)) {
                test.putIfAbsent(i + 1, ""+(i + 2))
                assertListener(lcounter, i+1, null, ""+(i + 2), false)
            } else if (m == 4) {
                test.remove(k, v)
                assertListener(lcounter, k, v, null, false)
            } else if (m == 5) {
                test.replace(k, v, ""+(i * i))
                assertListener(lcounter, k, v, ""+(i*i), false)
            } else if (m == 5) {
                test.replace(k, ""+(i * i))
                assertListener(lcounter, k, v, ""+(i*i), false)
            }
        }
    }

}


