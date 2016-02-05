package org.mapdb

import org.junit.Assert
import org.junit.Test
import java.util.*

/**
 * Tests map with modification listener
 */

abstract class MapModificationListenerTest:MapModificationListener<Int,String> {

    abstract fun makeMap(): MapExtra<Int, String>

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

    class HTreeMapTest:MapModificationListenerTest(){
        override fun makeMap(): MapExtra<Int, String>  = HTreeMap.make(
                keySerializer = Serializer.INTEGER, valueSerializer = Serializer.STRING,
                modificationListeners = arrayOf(this as MapModificationListener<Int, String>))

    }

}


