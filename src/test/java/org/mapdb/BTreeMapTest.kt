@file:Suppress("CAST_NEVER_SUCCEEDS")

package org.mapdb

import org.eclipse.collections.api.list.primitive.MutableLongList
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet
import org.fest.reflect.core.Reflection
import org.junit.Assert.*
import org.junit.Test
import org.mapdb.BTreeMapJava.*
import org.mapdb.StoreAccess.calculateFreeSize
import java.io.IOException
import java.math.BigInteger
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.ConcurrentSkipListSet
import java.util.concurrent.CopyOnWriteArraySet
import java.util.concurrent.atomic.AtomicInteger

@Suppress("UNCHECKED_CAST")
class BTreeMapTest {

    val keyser = Serializer.ELSA
    val COMPARATOR = keyser

    val BTreeMap<*,*>.nodeSerializer:Serializer<Node>
        get() = Reflection.method("getNodeSerializer").`in`(this).invoke() as Serializer<Node>


    val BTreeMap<*,*>.leftEdges:MutableLongList
        get() = Reflection.method("getLeftEdges").`in`(this).invoke() as MutableLongList

    val BTreeMap<*,*>.locks: ConcurrentHashMap<Long, Long>
        get() = Reflection.method("getLocks").`in`(this).invoke() as ConcurrentHashMap<Long, Long>


    fun BTreeMap<*,*>.loadLeftEdges(): MutableLongList =
            Reflection.method("loadLeftEdges")
                    .`in`(this)
                    .invoke() as MutableLongList



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
                arrayOf(1,1),
                longArrayOf(1)
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
        TT.assertFailsWith(DBException.GetVoid::class.java) {
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
            map.leftEdges.clear()
            map.leftEdges.addAll(map.loadLeftEdges())
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
        TT.assertFailsWith(NoSuchElementException::class.java) {
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
        TT.assertFailsWith(NoSuchElementException::class.java) {
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
        TT.assertFailsWith(NoSuchElementException::class.java) {
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
        map.leftEdges.clear()
        map.leftEdges.addAll(map.loadLeftEdges())
        map.verify()

        var iter = map.descendingLeafIterator(null)

        assertTrue(iter.hasNext())
        assertEquals(70, (iter.next().keys as Array<Any>)[0])

        assertTrue(iter.hasNext())
        assertEquals(50, (iter.next().keys as Array<Any>)[0])

        assertTrue(iter.hasNext())
        assertEquals(20, (iter.next().keys as Array<Any>)[0])

        assertFalse(iter.hasNext())
        TT.assertFailsWith(NoSuchElementException::class.java) {
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
        map.leftEdges.clear()
        map.leftEdges.addAll(map.loadLeftEdges())
        map.verify()

        var iter = map.descendingLeafIterator(null)

        assertTrue(iter.hasNext())
        assertEquals(70, (iter.next().keys as Array<Any>)[0])

        assertTrue(iter.hasNext())
        assertEquals(50, (iter.next().keys as Array<Any>)[0])

        assertTrue(iter.hasNext())
        assertEquals(20, (iter.next().keys as Array<Any>)[0])

        assertFalse(iter.hasNext())
        TT.assertFailsWith(NoSuchElementException::class.java) {
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
        map.leftEdges.clear()
        map.leftEdges.addAll(map.loadLeftEdges())
        map.verify()

        fun checkNode(key:Int, expectedLowKey:Int?) {
            var iter = map.descendingLeafIterator(key)
            if(expectedLowKey==null){
                assertFalse(iter.hasNext())
                return
            }
            var key2:Int = expectedLowKey
            assertTrue(iter.hasNext())
            assertEquals(expectedLowKey, (iter.next().keys as Array<Any>)[0])

            while(iter.hasNext()){
                val node = iter.next()
                val lowKey = (node.keys as Array<Any>)[0] as Int
                if(key2 <= lowKey)
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

    @Test fun prefix_submap(){
        val map = BTreeMap.make(
                keySerializer = Serializer.BYTE_ARRAY,
                valueSerializer = Serializer.BYTE_ARRAY)
        for(b1 in Byte.MIN_VALUE..Byte.MAX_VALUE)
        for(b2 in Byte.MIN_VALUE..Byte.MAX_VALUE){
            val b = byteArrayOf(b1.toByte(),b2.toByte())
            map.put(b,b)
        }

        val prefixSubmap = map.prefixSubMap(byteArrayOf(4))
        assertEquals(256, prefixSubmap.size)
        val iter = prefixSubmap.keys.iterator()
        for(i in 0..127){
            assertTrue(iter.hasNext())
            assertArrayEquals(byteArrayOf(4, (i  and 0xFF).toByte()), iter.next())
        }

        for(i in -128..-1){
            assertTrue(iter.hasNext())
            assertArrayEquals(byteArrayOf(4, (i  and 0xFF).toByte()), iter.next())
        }
        assertFalse(iter.hasNext())
    }


    @Test fun lock(){
        if(TT.shortTest())
            return
        val map = BTreeMap.make<Int, Int>()
        var counter = 0

        TT.fork(20, { _->
            map.lock(10L)
            val c = counter
            Thread.sleep(100)
            counter=c+1
            map.unlock(10L)
        })

        assertEquals(20, counter)
    }


    @Test
    fun issue695(){
        val sink = DBMaker.memoryDB().make().treeMap("a",
                Serializer.BYTE_ARRAY,
                Serializer.STRING).createFromSink()
        TT.assertFailsWith(DBException.NotSorted::class.java) {
            for (key in 120L..131) {
                sink.put(BigInteger.valueOf(key).toByteArray(), "value" + key)
            }
            sink.create()
        }
    }

    @Test fun external_value(){
        val b = BTreeMap.make(
                keySerializer = Serializer.INTEGER,
                valueSerializer = Serializer.STRING,
                valueInline = false)
        b.put(1, "1")

        val rootRecid = b.store.get(b.rootRecidRecid, Serializer.RECID)!!
        val node = b.store.get(rootRecid, b.nodeSerializer)!!
        assertArrayEquals(arrayOf(1), b.keySerializer.valueArrayToArray(node.keys))
        //value is long array
        assertEquals(1, Serializer.RECID.valueArraySize(node.values))
        val valueRecid = Serializer.RECID.valueArrayGet(node.values, 0)
        val value = b.store.get(valueRecid, Serializer.STRING)
        assertEquals("1", value)
    }


    @Test fun issue_38() {
        val max = 100+50000 * TT.testScale()
        val map = DBMaker.memoryDB().make().treeMap("test").create() as MutableMap<Int, Array<String?>>

        for (i in 0..max - 1) {
            map.put(i, arrayOfNulls<String>(5))

        }

        var i = 0
        while (i < max) {
            assertTrue(Arrays.equals(arrayOfNulls<String>(5), map.get(i)))
            assertTrue(map.get(i).toString().contains("[Ljava.lang.String"))
            i = i + 1000
        }
    }



    @Test fun findSmaller() {

        val m = DBMaker.memoryDB().make().treeMap("test").create() as NavigableMap<Int, String>

        run {
            var i = 0
            while (i < 10000) {
                m.put(i, "aa" + i)
                i += 3
            }
        }

        run {
            var i = 0
            while (i < 10000) {
                val s = i - i % 3
                val e = m.floorEntry(i)
                assertEquals(s, if (e != null) e.key else null)
                i += 1
            }
        }

        assertEquals(9999, m.floorEntry(100000).key)

        assertNull(m.lowerEntry(0))
        var i = 1
        while (i < 10000) {
            var s: Int? = i - i % 3
            if (s == i) s -= 3
            val e = m.lowerEntry(i)
            assertEquals(s, if (e != null) e.key else null)
            i += 1
        }
        assertEquals(9999, m.lowerEntry(100000).key)
    }

    @Test fun NoSuchElem_After_Clear() {
        //      bug reported by :	Lazaros Tsochatzidis
        //        But after clearing the tree using:
        //
        //        public void Delete() {
        //            db.getTreeMap("Names").clear();
        //            db.compact();
        //        }
        //
        //        every next call of getLastKey() leads to the exception "NoSuchElement". Not
        //        only the first one...

        val db = DBMaker.memoryDB().make()
        val m = db.treeMap("name").create() as NavigableMap<String,String>
        try {
            m.lastKey()
            fail()
        } catch (e: NoSuchElementException) {
        }

        m.put("aa", "aa")
        assertEquals("aa", m.lastKey())
        m.put("bb", "bb")
        assertEquals("bb", m.lastKey())
        db.treeMap("name").open().clear()
        db.store.compact()
        try {
            val key = m.lastKey()
            fail(key.toString())
        } catch (e: NoSuchElementException) {
        }

        m.put("aa", "aa")
        assertEquals("aa", m.lastKey())
        m.put("bb", "bb")
        assertEquals("bb", m.lastKey())
    }

    @Test fun mod_listener_lock() {
        val db = DBMaker.memoryDB().make()
        val counter = AtomicInteger()
        var m:BTreeMap<String,String>? = null;
        var rootRecid = 0L
        m = db.treeMap("name", Serializer.STRING, Serializer.STRING)
                .modificationListener(object : MapModificationListener<String,String> {
                    override fun modify(key: String, oldValue: String?, newValue: String?, triggered: Boolean) {
                        assertTrue(m!!.locks.get(rootRecid) == Thread.currentThread().id)
                        assertEquals(1, m!!.locks.size)
                        counter.incrementAndGet()
                    }
                })
                .create()
        rootRecid = db.store.get(m.rootRecidRecid, Serializer.RECID)!!

        m.put("aa", "aa")
        m.put("aa", "bb")
        m.remove("aa")

        m.put("aa", "aa")
        m.remove("aa", "aa")
        m.putIfAbsent("aa", "bb")
        m.replace("aa", "bb", "cc")
        m.replace("aa", "cc")

        assertEquals(8, counter.get())
    }


    @Test fun concurrent_last_key() {
        val db = DBMaker.memoryDB().make()
        val m = db.treeMap("name", Serializer.INTEGER, Serializer.INTEGER).create()

        //fill
        val c = 1000000 * TT.testScale()
        for (i in 0..c) {
            m.put(i, i)
        }

        val t = object : Thread() {
            override fun run() {
                for (i in c downTo 0) {
                    m.remove(i)
                }
            }
        }
        t.run()
        while (t.isAlive) {
            assertNotNull(m.lastKey())
        }
    }

    @Test fun concurrent_first_key() {
        val db = DBMaker.memoryDB().make()
        val m = db.treeMap("name", Serializer.INTEGER, Serializer.INTEGER).create()

        //fill
        val c = 1000000 * TT.testScale()
        for (i in 0..c) {
            m.put(i, i)
        }

        val t = object : Thread() {
            override fun run() {
                for (i in 0..c) {
                    m.remove(c)
                }
            }
        }
        t.run()
        while (t.isAlive) {
            assertNotNull(m.firstKey())
        }
    }


    @Test fun WriteDBInt_lastKey() {
        val numberOfRecords = 1000

        /* Creates connections to MapDB */
        val db1 = DBMaker.memoryDB().make()


        /* Creates maps */
        val map1 = db1.treeMap("column1", Serializer.INTEGER, Serializer.INTEGER).create()

        /* Inserts initial values in maps */
        for (i in 0..numberOfRecords - 1) {
            map1.put(i, i)
        }


        assertEquals((numberOfRecords - 1) as Any, map1.lastKey())

        map1.clear()

        /* Inserts some values in maps */
        for (i in 0..9) {
            map1.put(i, i)
        }

        assertEquals(10, map1.size.toLong())
        assertFalse(map1.isEmpty())
        assertEquals(9 as Any, map1.lastKey())
        assertEquals(9 as Any, map1.lastEntry()!!.value)
        assertEquals(0 as Any, map1.firstKey())
        assertEquals(0 as Any, map1.firstEntry()!!.value)
    }

    @Test fun WriteDBInt_lastKey_set() {
        val numberOfRecords = 1000

        /* Creates connections to MapDB */
        val db1 = DBMaker.memoryDB().make()


        /* Creates maps */
        val map1 = db1.treeSet("column1",Serializer.INTEGER).create()

        /* Inserts initial values in maps */
        for (i in 0..numberOfRecords - 1) {
            map1.add(i)
        }


        assertEquals((numberOfRecords - 1) as Any, map1.last())

        map1.clear()

        /* Inserts some values in maps */
        for (i in 0..9) {
            map1.add(i)
        }

        assertEquals(10, map1.size.toLong())
        assertFalse(map1.isEmpty())
        assertEquals(9 as Any, map1.last())
        assertEquals(0 as Any, map1.first())
    }

    @Test fun WriteDBInt_lastKey_middle() {
        val numberOfRecords = 1000

        /* Creates connections to MapDB */
        val db1 = DBMaker.memoryDB().make()


        /* Creates maps */
        val map1 = db1.treeMap("column1", Serializer.INTEGER, Serializer.INTEGER).create()

        /* Inserts initial values in maps */
        for (i in 0..numberOfRecords - 1) {
            map1.put(i, i)
        }


        assertEquals((numberOfRecords - 1) as Any, map1.lastKey())

        map1.clear()

        /* Inserts some values in maps */
        for (i in 100..109) {
            map1.put(i, i)
        }

        assertEquals(10, map1.size.toLong())
        assertFalse(map1.isEmpty())
        assertEquals(109 as Any, map1.lastKey())
        assertEquals(109 as Any, map1.lastEntry()!!.value)
        assertEquals(100 as Any, map1.firstKey())
        assertEquals(100 as Any, map1.firstEntry()!!.value)
    }

    @Test fun WriteDBInt_lastKey_set_middle() {
        val numberOfRecords = 1000

        /* Creates connections to MapDB */
        val db1 = DBMaker.memoryDB().make()


        /* Creates maps */
        val map1 = db1.treeSet("column1", Serializer.INTEGER).create()

        /* Inserts initial values in maps */
        for (i in 0..numberOfRecords - 1) {
            map1.add(i)
        }


        assertEquals((numberOfRecords - 1) as Any, map1.last())

        map1.clear()

        /* Inserts some values in maps */
        for (i in 100..109) {
            map1.add(i)
        }

        assertEquals(10, map1.size.toLong())
        assertFalse(map1.isEmpty())
        assertEquals(109 as Any, map1.last())
        assertEquals(100 as Any, map1.first())
    }


    @Test fun randomStructuralCheck() {
        val r = Random()
        val map = DBMaker.memoryDB().make().treeMap("aa")
                .keySerializer(Serializer.INTEGER).valueSerializer(Serializer.INTEGER).create()

        val max = 100000 * TT.testScale()

        for (i in 0..max * 10 - 1) {
            map.put(r.nextInt(max), r.nextInt())
        }

        map.verify()
    }


    @Test
    fun large_node_size() {
        if (TT.shortTest())
            return
        for (i in intArrayOf(10, 200, 6000)) {

            val max = i * 100
            val f = TT.tempFile()
            var db = DBMaker.fileDB(f).fileMmapEnableIfSupported().make()
            var m = db.treeMap("map").maxNodeSize(i)
                    .keySerializer(Serializer.INTEGER)
                    .valueSerializer(Serializer.INTEGER).create()

            for (j in 0..max - 1) {
                m.put(j, j)
            }

            db.close()
            db = DBMaker.fileDB(f).fileDeleteAfterClose().fileMmapEnableIfSupported().make()
            m = db.treeMap("map", Serializer.INTEGER, Serializer.INTEGER).open()

            for (j in 0..max - 1) {
                assertEquals(j, m.get(j))
            }
            db.close()
            f.delete()
        }
    }


    @Test fun issue403_store_grows_with_values_outside_nodes() {
        val f = TT.tempFile()
        val db = DBMaker.fileDB(f).closeOnJvmShutdown().make()

        val id2entry = db.treeMap("id2entry")
                .valueSerializer(Serializer.BYTE_ARRAY)
                .keySerializer(Serializer.LONG).valuesOutsideNodesEnable()
                .create()

        val store = db.store as StoreDirect
        var b = TT.randomByteArray(10000)
        id2entry.put(11L, b)
        val size = store.getTotalSize() - store.calculateFreeSize()
        for (i in 0..99) {
            val b2 = TT.randomByteArray(10000)
            assertArrayEquals(b, id2entry.put(11L, b2))
            b = b2
        }
        assertEquals(size, store.getTotalSize() - store.calculateFreeSize())

        for (i in 0..99) {
            val b2 = TT.randomByteArray(10000)
            assertArrayEquals(b, id2entry.replace(11L, b2))
            b = b2
        }
        assertEquals(size, store.getTotalSize() - store.calculateFreeSize())

        for (i in 0..99) {
            val b2 = TT.randomByteArray(10000)
            assertTrue((id2entry as MutableMap<Long, ByteArray>).replace(11L, b, b2))
            b = b2
        }
        assertEquals(size, store.getTotalSize() - store.calculateFreeSize())


        db.close()
        f.delete()
    }


    @Test fun setLong() {
        val k = DBMaker.heapDB().make().treeSet("test").create() as KeySet<Int>
        k.add(11)
        assertEquals(1, k.sizeLong())
    }


    @Test(expected = NullPointerException::class)
    fun testNullKeyInsertion() {
        val map = DBMaker.memoryDB().make().treeMap("map").create()
        map.put(null, "NULL VALUE")
        fail("A NullPointerException should have been thrown since the inserted key was null")
    }

    @Test(expected = NullPointerException::class)
    fun testNullValueInsertion() {
        val map = DBMaker.memoryDB().make().treeMap("map").create() as MutableMap<Any,Any?>
        map.put(1, null)
        fail("A NullPointerException should have been thrown since the inserted key value null")
    }

    @Test fun testUnicodeCharacterKeyInsertion() {
        val map = DBMaker.memoryDB().make().treeMap("map").create() as MutableMap<Any,Any>
        map.put('\u00C0', '\u00C0')

        assertEquals("unicode character value entered against the unicode character key could not be retrieved",
                '\u00C0', map.get('\u00C0'))
    }


    @Test @Throws(IOException::class, ClassNotFoundException::class)
    fun serialize_clone() {
        val m:MutableMap<Int,Int> = DBMaker
                .memoryDB()
                .make()
                .treeMap("map", Serializer.INTEGER, Serializer.INTEGER)
                .createOrOpen()

        for (i in 0..999) {
            m.put(i, i * 10)
        }

        val m2 = TT.cloneJavaSerialization(m)
        assertEquals(ConcurrentSkipListMap::class.java, m2.javaClass)
        assertTrue(m2.entries.containsAll(m.entries))
        assertTrue(m.entries.containsAll(m2.entries))
    }

    @Test @Throws(IOException::class, ClassNotFoundException::class)
    fun serialize_set_clone() {
        val m = DBMaker.memoryDB().make().treeSet("map", Serializer.INTEGER).createOrOpen()
        for (i in 0..999) {
            m.add(i)
        }

        val m2 = TT.cloneJavaSerialization(m)
        assertEquals(ConcurrentSkipListSet::class.java, m2.javaClass)
        assertTrue(m2.containsAll(m))
        assertTrue(m.containsAll(m2))
    }

    @Test fun external_value_null_after_delete(){
        val map = BTreeMap.make(
                keySerializer = Serializer.INTEGER,
                valueSerializer = Serializer.INTEGER,
                valueInline = false)
        map.put(1,1);
        val rootRecid = map.store.get(map.rootRecidRecid, Serializer.RECID)!!
        val rootNode = map.store.get(rootRecid, map.nodeSerializer)!!
        val valueRecid =  rootNode.children[0]

        assertEquals(1, map.store.get(valueRecid, map.valueSerializer))
        map.remove(1)
        assertEquals(null, map.store.get(valueRecid, map.valueSerializer))
    }

}