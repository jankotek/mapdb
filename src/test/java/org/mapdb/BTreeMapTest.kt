package org.mapdb

import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet
import org.junit.Test
import org.mapdb.BTreeMapJava.*
import org.mapdb.serializer.GroupSerializer
import java.util.*
import java.util.concurrent.CopyOnWriteArraySet
import kotlin.test.*

class BTreeMapTest {

    val keyser = Serializer.JAVA
    val COMPARATOR = keyser

    @Test fun node_search() {
        val node = Node(
                DIR + LEFT,
                60L,
                arrayOf(10, 20, 30, 40),
                longArrayOf(10L, 20L, 30L, 40L)
        )


        assertEquals(10L, findChild(keyser, node, COMPARATOR, 1))
        assertEquals(10L, findChild(keyser, node, COMPARATOR, 10))
        assertEquals(20L, findChild(keyser, node, COMPARATOR, 11))
        assertEquals(20L, findChild(keyser, node, COMPARATOR, 20))
        assertEquals(40L, findChild(keyser, node, COMPARATOR, 40))
        assertEquals(60L, findChild(keyser, node, COMPARATOR, 41))
    }

    @Test fun node_search2() {
        val node = Node(
                DIR,
                60L,
                arrayOf(10, 20, 30, 40),
                longArrayOf(10L, 20L, 30L)
        )

        assertEquals(10L, findChild(keyser, node, COMPARATOR, 1)) //TODO this should not happen on non LeftEdge, throw corruption error?
        assertEquals(10L, findChild(keyser, node, COMPARATOR, 10))
        assertEquals(10L, findChild(keyser, node, COMPARATOR, 11))
        assertEquals(10L, findChild(keyser, node, COMPARATOR, 20))
        assertEquals(20L, findChild(keyser, node, COMPARATOR, 21))
        assertEquals(20L, findChild(keyser, node, COMPARATOR, 25))
        assertEquals(20L, findChild(keyser, node, COMPARATOR, 30))
        assertEquals(30L, findChild(keyser, node, COMPARATOR, 31))
        assertEquals(30L, findChild(keyser, node, COMPARATOR, 40))
        assertEquals(60L, findChild(keyser, node, COMPARATOR, 41))
    }

    @Test fun node_search3() {
        val node = Node(
                DIR + RIGHT,
                0,
                arrayOf(10, 20, 30, 40),
                longArrayOf(10L, 20L, 30L, 40L)
        )

        assertEquals(10L, findChild(keyser, node, COMPARATOR, 1)) //TODO this should not happen on non LeftEdge, throw corruption error?
        assertEquals(10L, findChild(keyser, node, COMPARATOR, 10))
        assertEquals(10L, findChild(keyser, node, COMPARATOR, 11))
        assertEquals(10L, findChild(keyser, node, COMPARATOR, 20))
        assertEquals(20L, findChild(keyser, node, COMPARATOR, 21))
        assertEquals(20L, findChild(keyser, node, COMPARATOR, 30))
        assertEquals(30L, findChild(keyser, node, COMPARATOR, 31))
        assertEquals(30L, findChild(keyser, node, COMPARATOR, 40))
        assertEquals(40L, findChild(keyser, node, COMPARATOR, 41))
        assertEquals(40L, findChild(keyser, node, COMPARATOR, 50))
    }

    @Test fun node_search4() {
        val node = Node(
                DIR + LEFT + RIGHT,
                0L,
                arrayOf(10, 20, 30, 40),
                longArrayOf(10L, 20L, 30L, 40L, 50L)
        )

        assertEquals(10L, findChild(keyser, node, COMPARATOR, 1))
        assertEquals(10L, findChild(keyser, node, COMPARATOR, 10))
        assertEquals(20L, findChild(keyser, node, COMPARATOR, 11))
        assertEquals(20L, findChild(keyser, node, COMPARATOR, 20))
        assertEquals(40L, findChild(keyser, node, COMPARATOR, 40))
        assertEquals(50L, findChild(keyser, node, COMPARATOR, 41))
        assertEquals(50L, findChild(keyser, node, COMPARATOR, 50))
    }

    @Test fun findValue() {
        val node = Node(
                LAST_KEY_DOUBLE,
                10L,
                arrayOf(10, 20, 30, 40),
                arrayOf(2, 3, 4)
        )

        assertEquals(-1, keyser.valueArraySearch(node.keys, 5, COMPARATOR))
        assertEquals(-2, keyser.valueArraySearch(node.keys, 15, COMPARATOR))
        assertEquals(-3, keyser.valueArraySearch(node.keys, 22, COMPARATOR))
        assertEquals(0, keyser.valueArraySearch(node.keys, 10, COMPARATOR))
        assertEquals(1, keyser.valueArraySearch(node.keys, 20, COMPARATOR))
        assertEquals(2, keyser.valueArraySearch(node.keys, 30, COMPARATOR))
        assertEquals(3, keyser.valueArraySearch(node.keys, 40, COMPARATOR))
        assertEquals(-5, keyser.valueArraySearch(node.keys, 50, COMPARATOR))

    }

    @Test fun leafGet() {
        val node = Node(
                LAST_KEY_DOUBLE,
                10L,
                arrayOf(10, 20, 30, 40),
                arrayOf(2, 3, 4)
        )

        assertEquals(null, leafGet(node, COMPARATOR, 10, keyser, keyser))
        assertEquals(2, leafGet(node, COMPARATOR, 20, keyser, keyser))
        assertEquals(null, leafGet(node, COMPARATOR, 21, keyser, keyser))
        assertEquals(3, leafGet(node, COMPARATOR, 30, keyser, keyser))
        assertEquals(4, leafGet(node, COMPARATOR, 40, keyser, keyser))
        assertEquals(LINK, leafGet(node, COMPARATOR, 41, keyser, keyser))
        assertEquals(LINK, leafGet(node, COMPARATOR, 50, keyser, keyser))
    }

    @Test fun leafGetLink() {
        val node = Node(
                0,
                10L,
                arrayOf(10, 20, 30, 40, 50),
                arrayOf(2, 3, 4)
        )

        assertEquals(null, leafGet(node, COMPARATOR, 10, keyser, keyser))
        assertEquals(2, leafGet(node, COMPARATOR, 20, keyser, keyser))
        assertEquals(null, leafGet(node, COMPARATOR, 21, keyser, keyser))
        assertEquals(3, leafGet(node, COMPARATOR, 30, keyser, keyser))
        assertEquals(4, leafGet(node, COMPARATOR, 40, keyser, keyser))
        assertEquals(null, leafGet(node, COMPARATOR, 41, keyser, keyser))
        assertEquals(null, leafGet(node, COMPARATOR, 50, keyser, keyser))
        assertEquals(LINK, leafGet(node, COMPARATOR, 51, keyser, keyser))
    }

    @Test fun flags() {
        val node = Node(
                RIGHT + LEFT,
                0L,
                arrayOf<Any>(),
                arrayOf<Any>()
        )

        assertTrue(node.isRightEdge)
        assertEquals(1, node.intRightEdge())
        assertTrue(node.isLeftEdge)
        assertEquals(1, node.intLeftEdge())
        assertTrue(node.isDir.not())
        assertEquals(0, node.intDir())
        assertTrue(node.isLastKeyDouble.not())
        assertEquals(0, node.intLastKeyTwice())

        val node2 = Node(
                DIR,
                111L,
                arrayOf(1),
                longArrayOf()
        )

        assertTrue(node2.isRightEdge.not())
        assertEquals(0, node2.intRightEdge())
        assertTrue(node2.isLeftEdge.not())
        assertEquals(0, node2.intLeftEdge())
        assertTrue(node2.isDir)
        assertEquals(1, node2.intDir())
        assertTrue(node2.isLastKeyDouble.not())
        assertEquals(0, node2.intLastKeyTwice())

    }

    @Test fun getRoot() {
        val node = Node(
                LEFT + RIGHT,
                0L,
                arrayOf(20, 30, 40),
                arrayOf(2, 3, 4)
        )

        val map = BTreeMap.make<Int, Int>()
        val rootRecid = map.store.get(map.rootRecidRecid, Serializer.RECID)!!
        map.store.update(rootRecid, node, map.nodeSerializer)

        assertEquals(null, map[19])
        assertEquals(2, map[20])
        assertEquals(null, map[21])
        assertEquals(3, map[30])
        assertEquals(4, map[40])
        assertEquals(null, map[41])
    }


    @Test fun getRootLink() {
        val map = BTreeMap.make<Int, Int>()
        val node2 = Node(
                RIGHT,
                0L,
                arrayOf(50, 60, 70),
                arrayOf(6, 7)
        )

        val node1 = Node(
                LEFT + LAST_KEY_DOUBLE,
                map.store.put(node2, map.nodeSerializer),
                arrayOf(20, 30, 40, 50),
                arrayOf(2, 3, 4, 5)
        )

        val rootRecid = map.store.get(map.rootRecidRecid, Serializer.RECID)!!
        map.store.update(rootRecid, node1, map.nodeSerializer)

        assertEquals(null, map[19])
        assertEquals(null, map[21])
        assertEquals(null, map[41])

        for (i in 2..7)
            assertEquals(i, map[i * 10])
    }

    @Test fun getRootMultiLink() {
        val map = BTreeMap.make<Int, Int>()
        val node3 = Node(
                RIGHT,
                0L,
                arrayOf(70, 80, 90),
                arrayOf(8, 9)
        )

        val node2 = Node(
                LAST_KEY_DOUBLE,
                map.store.put(node3, map.nodeSerializer),
                arrayOf(50, 60, 70),
                arrayOf(6, 7)
        )

        val node1 = Node(
                LEFT + LAST_KEY_DOUBLE,
                map.store.put(node2, map.nodeSerializer),
                arrayOf(20, 30, 40, 50),
                arrayOf(2, 3, 4, 5)
        )


        val rootRecid = map.store.get(map.rootRecidRecid, Serializer.RECID)!!
        map.store.update(rootRecid, node1, map.nodeSerializer)

        for (i in 2..9) {
            assertEquals(null, map[i * 10 + 1])
            assertEquals(null, map[i * 10 - 1])
            assertEquals(i, map[i * 10])
        }
    }


    @Test fun getMid() {
        //root starts as middle leaf node, that is not valid BTreeMap structure
        val node = Node(
                0,
                111L,
                arrayOf(10, 20, 30, 40, 40),
                arrayOf(2, 3, 4)
        )

        val map = BTreeMap.make<Int, Int>()
        val rootRecid = map.store.get(map.rootRecidRecid, Serializer.RECID)!!
        map.store.update(rootRecid, node, map.nodeSerializer)

        assertEquals(null, map[10])
        assertEquals(null, map[19])
        assertEquals(2, map[20])
        assertEquals(null, map[21])
        assertEquals(3, map[30])
        assertEquals(4, map[40])
        assertFailsWith(DBException.GetVoid::class) {
            assertEquals(null, map[41])
        }
    }


    @Test fun getMidLink() {
        //root starts as middle leaf node, that is not valid BTreeMap structure
        val map = BTreeMap.make<Int, Int>()
        val node2 = Node(
                RIGHT,
                0L,
                arrayOf(50, 60, 70),
                arrayOf(6, 7)
        )

        val node1 = Node(
                LAST_KEY_DOUBLE,
                map.store.put(node2, map.nodeSerializer),
                arrayOf(10, 20, 30, 40, 50),
                arrayOf(2, 3, 4, 5)
        )

        val rootRecid = map.store.get(map.rootRecidRecid, Serializer.RECID)!!
        map.store.update(rootRecid, node1, map.nodeSerializer)

        assertEquals(null, map[10])
        assertEquals(null, map[19])
        assertEquals(null, map[21])
        assertEquals(null, map[41])

        for (i in 2..7)
            assertEquals(i, map[i * 10])
    }

    @Test fun getMidMultiLink() {
        //root starts as middle leaf node, that is not valid BTreeMap structure
        val map = BTreeMap.make<Int, Int>()
        val node3 = Node(
                RIGHT,
                0L,
                arrayOf(70, 80, 90),
                arrayOf(8, 9)
        )

        val node2 = Node(
                LAST_KEY_DOUBLE,
                map.store.put(node3, map.nodeSerializer),
                arrayOf(50, 60, 70),
                arrayOf(6, 7)
        )

        val node1 = Node(
                LAST_KEY_DOUBLE,
                map.store.put(node2, map.nodeSerializer),
                arrayOf(10, 20, 30, 40, 50),
                arrayOf(2, 3, 4, 5)
        )


        val rootRecid = map.store.get(map.rootRecidRecid, Serializer.RECID)!!
        map.store.update(rootRecid, node1, map.nodeSerializer)

        assertEquals(null, map[10])
        for (i in 2..9) {
            assertEquals(null, map[i * 10 + 1])
            assertEquals(null, map[i * 10 - 1])
            assertEquals(i, map[i * 10])
        }
    }

    @Test fun getTree() {
        val map = BTreeMap.make<Int, Int>()

        val node3 = Node(
                RIGHT,
                0L,
                arrayOf(70, 80, 90),
                arrayOf(8, 9)
        )
        val recid3 = map.store.put(node3, map.nodeSerializer)

        val node2 = Node(
                LAST_KEY_DOUBLE,
                recid3,
                arrayOf(50, 60, 70),
                arrayOf(6, 7)
        )
        val recid2 = map.store.put(node2, map.nodeSerializer)

        val node1 = Node(
                LEFT + LAST_KEY_DOUBLE,
                recid2,
                arrayOf(20, 30, 40, 50),
                arrayOf(2, 3, 4, 5)
        )
        val recid1 = map.store.put(node1, map.nodeSerializer)

        val dir = Node(
                DIR + LEFT + RIGHT,
                0L,
                arrayOf(50, 70),
                longArrayOf(recid1, recid2, recid3)
        )
        val rootRecid = map.store.get(map.rootRecidRecid, Serializer.RECID)!!
        map.store.update(rootRecid, dir, map.nodeSerializer)

        for (i in 2..9) {
            assertEquals(null, map[i * 10 + 1])
            assertEquals(null, map[i * 10 - 1])
            assertEquals(i, map[i * 10])
        }
    }

    @Test fun update_Tree() {
        for (i in 19..91) {

            val map = BTreeMap.make<Int, Int>()

            val node3 = Node(
                    RIGHT,
                    0L,
                    arrayOf(70, 80, 90),
                    arrayOf(8, 9)
            )
            val recid3 = map.store.put(node3, map.nodeSerializer)

            val node2 = Node(
                    LAST_KEY_DOUBLE,
                    recid3,
                    arrayOf(50, 60, 70),
                    arrayOf(6, 7)
            )
            val recid2 = map.store.put(node2, map.nodeSerializer)

            val node1 = Node(
                    LEFT + LAST_KEY_DOUBLE,
                    recid2,
                    arrayOf(20, 30, 40, 50),
                    arrayOf(2, 3, 4, 5)
            )
            val recid1 = map.store.put(node1, map.nodeSerializer)

            val dir = Node(
                    DIR + LEFT + RIGHT,
                    0L,
                    arrayOf(50, 70),
                    longArrayOf(recid1, recid2, recid3)
            )
            val rootRecid = map.store.get(map.rootRecidRecid, Serializer.RECID)!!
            map.store.update(rootRecid, dir, map.nodeSerializer)
            map.verify()


            map.put(i, i * 100)
            assertEquals(i * 100, map[i])
            map.verify()
        }
    }

    @Test fun randomInsert() {
        val map = BTreeMap.make(
                keySerializer = Serializer.INTEGER,
                valueSerializer = Serializer.INTEGER,
                maxNodeSize = 8
        )

        var r = Random(1)
        val ref = IntHashSet()
        for (i in 0..1000) {
            val key = r.nextInt(10000)
            ref.add(key)
            map.put(key, key * 100)
            map.verify()
            ref.forEach { key2 ->
                assertEquals(key2 * 100, map[key2])
            }
        }
    }

    @Test fun randomInsert_returnVal() {
        val map = BTreeMap.make(
                keySerializer = Serializer.INTEGER,
                valueSerializer = Serializer.INTEGER,
                maxNodeSize = 8
        )

        var r = Random(1)
        val ref = IntHashSet()
        for (i in 0..1000) {
            val key = r.nextInt(10000)
            ref.add(key)
            map.put(key, key * 100 + i - 1)
            map.verify()
            ref.forEach { key2 ->
                assertEquals(key2 * 100 + i - 1, map[key2])
                assertEquals(key2 * 100 + i - 1, map.put(key2, key2 * 100 + i))
            }
        }
    }

    @Test fun randomInsert_delete() {
        val map = BTreeMap.make(
                keySerializer = Serializer.INTEGER,
                valueSerializer = Serializer.INTEGER,
                maxNodeSize = 8
        )

        var r = Random(1)
        val ref = IntHashSet()
        for (i in 0..1000) {
            val key = r.nextInt(10000)
            ref.add(key)
            map.put(key, key * 100)
        }

        val removed = IntHashSet()

        ref.forEach { key ->
            assertEquals(key * 100, map[key])
            assertEquals(key * 100, map.remove(key))
            assertEquals(null, map[key])
            assertEquals(null, map.remove(key))
            removed.add(key)


            for (i in 0..10000) {
                if (!ref.contains(i) && !removed.contains(i)) {
                    assertEquals(null, map[i])
                }
            }
            map.verify()
        }

        ref.forEach { key ->
            assertEquals(null, map[key])
        }
        map.verify()
    }

    @Test fun iterate() {
        val map = BTreeMap.make(
                keySerializer = Serializer.INTEGER,
                valueSerializer = Serializer.INTEGER,
                maxNodeSize = 8
        )

        var r = Random(1)
        val ref = IntHashSet()
        for (i in 0..1000) {
            val key = r.nextInt(10000)
            ref.add(key)
            map.put(key, key * 100)
        }

        val iter = map.entries.iterator()
        while (iter.hasNext()) {
            val next = iter.next()
            assertTrue(ref.remove(next.key!!))
            assertEquals(next.key!! * 100, next.value!!)
        }
        assertFalse(iter.hasNext())
        assertFailsWith(NoSuchElementException::class) {
            iter.next()
        }

        assertTrue(ref.isEmpty)
    }


    /* check that empty leaf nodes are skipped during iteration */
    @Test fun iterate_remove() {
        val map = BTreeMap.make(
                keySerializer = Serializer.INTEGER,
                valueSerializer = Serializer.INTEGER,
                maxNodeSize = 8
        )

        var r = Random(1)
        val ref = CopyOnWriteArraySet<Int>()
        for (i in 0..1000) {
            val key = r.nextInt(10000)
            ref.add(key)
            map.put(key, key * 100)
        }

        // remove keys from ref, iterator should always return all entries in ref
        for (key in ref) {
            ref.remove(key)
            assertEquals(key * 100, map.remove(key))

            val otherRef = CopyOnWriteArraySet<Int>()
            val iter = map.entries.iterator()
            while (iter.hasNext()) {
                otherRef.add(iter.next().key!!)
            }
            //sort, ensure it equals
            val sortedRef = TreeSet<Int>(ref)
            val sortedOtherRef = TreeSet<Int>(otherRef)
            assertEquals(sortedRef, sortedOtherRef)
        }

    }

    @Test fun descending_leaf_iterator() {
        val map = BTreeMap.make<Int, Int>()

        var iter = map.descendingLeafIterator(null)

        assertTrue(iter.hasNext())
        assertTrue(iter.next().isEmpty(map.keySerializer))
        assertFalse(iter.hasNext())
        assertFailsWith(NoSuchElementException::class) {
            iter.next();
        }

    }

    @Test fun descending_leaf_iterator_singleNode() {
        val map = BTreeMap.make<Int, Int>()

        val nodeRecid = map.store.put(
                Node(LEFT + RIGHT, 0, arrayOf(1), arrayOf(10)),
                map.nodeSerializer
        )

        map.store.update(
                map.rootRecidRecid,
                nodeRecid,
                Serializer.RECID
        )

        var iter = map.descendingLeafIterator(null)

        assertTrue(iter.hasNext())
        assertEquals(1, (iter.next().keys as Array<Any>)[0])
        assertFalse(iter.hasNext())
        assertFailsWith(NoSuchElementException::class) {
            iter.next();
        }

    }


    @Test fun descending_leaf_iterator_threeChild() {
        val map = BTreeMap.make<Int, Int>()

        val node3 = Node(
                RIGHT,
                0L,
                arrayOf(70, 80, 90),
                arrayOf(8, 9)
        )
        val recid3 = map.store.put(node3, map.nodeSerializer)

        val node2 = Node(
                LAST_KEY_DOUBLE,
                recid3,
                arrayOf(50, 60, 70),
                arrayOf(6, 7)
        )
        val recid2 = map.store.put(node2, map.nodeSerializer)

        val node1 = Node(
                LEFT + LAST_KEY_DOUBLE,
                recid2,
                arrayOf(20, 30, 40, 50),
                arrayOf(2, 3, 4, 5)
        )
        val recid1 = map.store.put(node1, map.nodeSerializer)

        val dir = Node(
                DIR + LEFT + RIGHT,
                0L,
                arrayOf(50, 70),
                longArrayOf(recid1, recid2, recid3)
        )
        val rootRecid = map.store.get(map.rootRecidRecid, Serializer.RECID)!!
        map.store.update(rootRecid, dir, map.nodeSerializer)
        map.verify()

        var iter = map.descendingLeafIterator(null)

        assertTrue(iter.hasNext())
        assertEquals(70, (iter.next().keys as Array<Any>)[0])

        assertTrue(iter.hasNext())
        assertEquals(50, (iter.next().keys as Array<Any>)[0])

        assertTrue(iter.hasNext())
        assertEquals(20, (iter.next().keys as Array<Any>)[0])

        assertFalse(iter.hasNext())
        assertFailsWith(NoSuchElementException::class) {
            iter.next();
        }

    }

    @Test fun descending_leaf_iterator_linkedChild_right() {
        val map = BTreeMap.make<Int, Int>()

        val node3 = Node(
                RIGHT,
                0L,
                arrayOf(70, 80, 90),
                arrayOf(8, 9)
        )
        val recid3 = map.store.put(node3, map.nodeSerializer)

        val node2 = Node(
                LAST_KEY_DOUBLE,
                recid3,
                arrayOf(50, 60, 70),
                arrayOf(6, 7)
        )
        val recid2 = map.store.put(node2, map.nodeSerializer)

        val node1 = Node(
                LEFT + LAST_KEY_DOUBLE,
                recid2,
                arrayOf(20, 30, 40, 50),
                arrayOf(2, 3, 4, 5)
        )
        val recid1 = map.store.put(node1, map.nodeSerializer)

        val dir = Node(
                DIR + LEFT + RIGHT,
                0L,
                arrayOf(50),
                longArrayOf(recid1, recid2)
        )
        val rootRecid = map.store.get(map.rootRecidRecid, Serializer.RECID)!!
        map.store.update(rootRecid, dir, map.nodeSerializer)
        map.verify()

        var iter = map.descendingLeafIterator(null)

        assertTrue(iter.hasNext())
        assertEquals(70, (iter.next().keys as Array<Any>)[0])

        assertTrue(iter.hasNext())
        assertEquals(50, (iter.next().keys as Array<Any>)[0])

        assertTrue(iter.hasNext())
        assertEquals(20, (iter.next().keys as Array<Any>)[0])

        assertFalse(iter.hasNext())
        assertFailsWith(NoSuchElementException::class) {
            iter.next();
        }

    }

    @Test fun descending_leaf_iterator_large() {
        val map = BTreeMap.make<Int, Int>(maxNodeSize = 6)
        for (i in 1..100)
            map.put(i, i)

        val ref = ArrayList<Any>()
        val iter = map.descendingLeafIterator(null)
        var lastVal = -1
        while (iter.hasNext()) {
            val values = iter.next().values as Array<Any>
            values.forEach { ref.add(it) }

            val currentVal = values[0] as Int
            if(lastVal!=-1 && currentVal >= lastVal){
                throw AssertionError()
            }
            lastVal = currentVal
        }

        assertEquals(100, ref.size)
        for (i in 1..100){
            assertTrue(ref.contains(i))
        }
    }

    @Test fun descendingNodeIterator_one() {
        val map = BTreeMap.make<Int, Int>()

        val node3 = Node(
                RIGHT,
                0L,
                arrayOf(70, 80, 90),
                arrayOf(8, 9)
        )
        val recid3 = map.store.put(node3, map.nodeSerializer)

        val node2 = Node(
                LAST_KEY_DOUBLE,
                recid3,
                arrayOf(50, 60, 70),
                arrayOf(6, 7)
        )
        val recid2 = map.store.put(node2, map.nodeSerializer)

        val node1 = Node(
                LEFT + LAST_KEY_DOUBLE,
                recid2,
                arrayOf(20, 30, 40, 50),
                arrayOf(2, 3, 4, 5)
        )
        val recid1 = map.store.put(node1, map.nodeSerializer)

        val dir = Node(
                DIR + LEFT + RIGHT,
                0L,
                arrayOf(50, 70),
                longArrayOf(recid1, recid2, recid3)
        )
        val rootRecid = map.store.get(map.rootRecidRecid, Serializer.RECID)!!
        map.store.update(rootRecid, dir, map.nodeSerializer)
        map.verify()

        fun checkNode(key:Int, expectedLowKey:Int?) {
            var iter = map.descendingLeafIterator(key)
            if(expectedLowKey==null){
                assertFalse(iter.hasNext())
                return
            }
            var key2 = expectedLowKey as Int;
            assertTrue(iter.hasNext())
            assertEquals(expectedLowKey, (iter.next().keys as Array<Any>)[0])

            while(iter.hasNext()){
                val node = iter.next()
                val lowKey = (node.keys as Array<Any>)[0] as Int
                if(key2<=lowKey)
                    throw AssertionError()
                key2 = lowKey
            }
        }

        for(key in 71..100){
            checkNode(key, 70)
        }

        for(key in 51..70){
            checkNode(key, 50)
        }
    }

}