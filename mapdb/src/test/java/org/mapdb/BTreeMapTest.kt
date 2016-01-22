package org.mapdb

import org.junit.Test
import org.mapdb.BTreeMapJava.*
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.test.failsWith

class BTreeMapTest {


    @Test fun node_search() {
        val node = Node(
                DIR + LEFT,
                60L,
                arrayOf(10, 20, 30, 40),
                longArrayOf(10L, 20L, 30L, 40L)
        )

        assertEquals(10L, findChild(node, COMPARATOR, 1))
        assertEquals(10L, findChild(node, COMPARATOR, 10))
        assertEquals(20L, findChild(node, COMPARATOR, 11))
        assertEquals(20L, findChild(node, COMPARATOR, 20))
        assertEquals(40L, findChild(node, COMPARATOR, 40))
        assertEquals(60L, findChild(node, COMPARATOR, 41))
    }

    @Test fun node_search2() {
        val node = Node(
                DIR,
                60L,
                arrayOf(10, 20, 30, 40),
                longArrayOf(10L, 20L, 30L)
        )

        assertEquals(10L, findChild(node, COMPARATOR, 1)) //TODO this should not happen on non LeftEdge, throw corruption error?
        assertEquals(10L, findChild(node, COMPARATOR, 10))
        assertEquals(10L, findChild(node, COMPARATOR, 11))
        assertEquals(10L, findChild(node, COMPARATOR, 20))
        assertEquals(20L, findChild(node, COMPARATOR, 21))
        assertEquals(20L, findChild(node, COMPARATOR, 25))
        assertEquals(20L, findChild(node, COMPARATOR, 30))
        assertEquals(30L, findChild(node, COMPARATOR, 31))
        assertEquals(30L, findChild(node, COMPARATOR, 40))
        assertEquals(60L, findChild(node, COMPARATOR, 41))
    }

    @Test fun node_search3() {
        val node = Node(
                DIR + RIGHT,
                0,
                arrayOf(10, 20, 30, 40),
                longArrayOf(10L, 20L, 30L, 40L)
        )

        assertEquals(10L, findChild(node, COMPARATOR, 1)) //TODO this should not happen on non LeftEdge, throw corruption error?
        assertEquals(10L, findChild(node, COMPARATOR, 10))
        assertEquals(10L, findChild(node, COMPARATOR, 11))
        assertEquals(10L, findChild(node, COMPARATOR, 20))
        assertEquals(20L, findChild(node, COMPARATOR, 21))
        assertEquals(20L, findChild(node, COMPARATOR, 30))
        assertEquals(30L, findChild(node, COMPARATOR, 31))
        assertEquals(30L, findChild(node, COMPARATOR, 40))
        assertEquals(40L, findChild(node, COMPARATOR, 41))
        assertEquals(40L, findChild(node, COMPARATOR, 50))
    }

    @Test fun node_search4() {
        val node = Node(
                DIR + LEFT + RIGHT,
                0L,
                arrayOf(10, 20, 30, 40),
                longArrayOf(10L, 20L, 30L, 40L, 50L)
        )

        assertEquals(10L, findChild(node, COMPARATOR, 1))
        assertEquals(10L, findChild(node, COMPARATOR, 10))
        assertEquals(20L, findChild(node, COMPARATOR, 11))
        assertEquals(20L, findChild(node, COMPARATOR, 20))
        assertEquals(40L, findChild(node, COMPARATOR, 40))
        assertEquals(50L, findChild(node, COMPARATOR, 41))
        assertEquals(50L, findChild(node, COMPARATOR, 50))
    }

    @Test fun findValue() {
        val node = Node(
                LAST_KEY_DOUBLE,
                10L,
                arrayOf(10, 20, 30, 40, 40),
                arrayOf(2, 3, 4)
        )

        assertEquals(-1, findValue(node, COMPARATOR, 5))
        assertEquals(-2, findValue(node, COMPARATOR, 15))
        assertEquals(-3, findValue(node, COMPARATOR, 22))
        assertEquals(0, findValue(node, COMPARATOR, 10))
        assertEquals(1, findValue(node, COMPARATOR, 20))
        assertEquals(2, findValue(node, COMPARATOR, 30))
        assertEquals(3, findValue(node, COMPARATOR, 40))
        assertEquals(-6, findValue(node, COMPARATOR, 50))

    }

    @Test fun leafGet() {
        val node = Node(
                0,
                10L,
                arrayOf(10, 20, 30, 40, 40),
                arrayOf(2, 3, 4)
        )

        assertEquals(null, leafGet(node, COMPARATOR, 10))
        assertEquals(2, leafGet(node, COMPARATOR, 20))
        assertEquals(null, leafGet(node, COMPARATOR, 21))
        assertEquals(3, leafGet(node, COMPARATOR, 30))
        assertEquals(4, leafGet(node, COMPARATOR, 40))
        assertEquals(LINK, leafGet(node, COMPARATOR, 41))
        assertEquals(LINK, leafGet(node, COMPARATOR, 50))
    }

    @Test fun leafGetLink() {
        val node = Node(
                0,
                10L,
                arrayOf(10, 20, 30, 40, 50),
                arrayOf(2, 3, 4)
        )

        assertEquals(null, leafGet(node, COMPARATOR, 10))
        assertEquals(2, leafGet(node, COMPARATOR, 20))
        assertEquals(null, leafGet(node, COMPARATOR, 21))
        assertEquals(3, leafGet(node, COMPARATOR, 30))
        assertEquals(4, leafGet(node, COMPARATOR, 40))
        assertEquals(null, leafGet(node, COMPARATOR, 41))
        assertEquals(null, leafGet(node, COMPARATOR, 50))
        assertEquals(LINK, leafGet(node, COMPARATOR, 51))
    }

    @Test fun flags() {
        val node = Node(
                RIGHT + LEFT,
                0L,
                arrayOf(),
                arrayOf<Object>()
        )

        assertTrue(node.isRightEdge)
        assertEquals(1, node.intRightEdge())
        assertTrue(node.isLeftEdge)
        assertEquals(1, node.intLeftEdge())
        assertTrue(node.isDir.not())
        assertEquals(0, node.intDir())
        assertTrue(node.isLastKeyDouble.not())
        assertEquals(0, node.intLastKeyDouble())

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
        assertEquals(0, node2.intLastKeyDouble())

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
        map.store.update(rootRecid, node, map.nodeSer)

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
                LEFT,
                map.store.put(node2, map.nodeSer),
                arrayOf(20, 30, 40, 50, 50),
                arrayOf(2, 3, 4, 5)
        )

        val rootRecid = map.store.get(map.rootRecidRecid, Serializer.RECID)!!
        map.store.update(rootRecid, node1, map.nodeSer)

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
                0,
                map.store.put(node3, map.nodeSer),
                arrayOf(50, 60, 70, 70),
                arrayOf(6, 7)
        )

        val node1 = Node(
                LEFT,
                map.store.put(node2, map.nodeSer),
                arrayOf(20, 30, 40, 50, 50),
                arrayOf(2, 3, 4, 5)
        )


        val rootRecid = map.store.get(map.rootRecidRecid, Serializer.RECID)!!
        map.store.update(rootRecid, node1, map.nodeSer)

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
        map.store.update(rootRecid, node, map.nodeSer)

        assertEquals(null, map[10])
        assertEquals(null, map[19])
        assertEquals(2, map[20])
        assertEquals(null, map[21])
        assertEquals(3, map[30])
        assertEquals(4, map[40])
        assertFailsWith(DBException.GetVoid::class.java) {
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
                0,
                map.store.put(node2, map.nodeSer),
                arrayOf(10, 20, 30, 40, 50, 50),
                arrayOf(2, 3, 4, 5)
        )

        val rootRecid = map.store.get(map.rootRecidRecid, Serializer.RECID)!!
        map.store.update(rootRecid, node1, map.nodeSer)

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
                0,
                map.store.put(node3, map.nodeSer),
                arrayOf(50, 60, 70, 70),
                arrayOf(6, 7)
        )

        val node1 = Node(
                0,
                map.store.put(node2, map.nodeSer),
                arrayOf(10, 20, 30, 40, 50, 50),
                arrayOf(2, 3, 4, 5)
        )


        val rootRecid = map.store.get(map.rootRecidRecid, Serializer.RECID)!!
        map.store.update(rootRecid, node1, map.nodeSer)

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
        val recid3 = map.store.put(node3, map.nodeSer)

        val node2 = Node(
                0,
                recid3,
                arrayOf(50, 60, 70, 70),
                arrayOf(6, 7)
        )
        val recid2 = map.store.put(node2, map.nodeSer)

        val node1 = Node(
                LEFT,
                recid2,
                arrayOf(20, 30, 40, 50, 50),
                arrayOf(2, 3, 4, 5)
        )
        val recid1 = map.store.put(node1, map.nodeSer)

        val dir = Node(
                DIR + LEFT + RIGHT,
                0L,
                arrayOf(50, 70),
                longArrayOf(recid1, recid2, recid3)
        )
        val rootRecid = map.store.get(map.rootRecidRecid, Serializer.RECID)!!
        map.store.update(rootRecid, dir, map.nodeSer)

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
            val recid3 = map.store.put(node3, map.nodeSer)

            val node2 = Node(
                    0,
                    recid3,
                    arrayOf(50, 60, 70, 70),
                    arrayOf(6, 7)
            )
            val recid2 = map.store.put(node2, map.nodeSer)

            val node1 = Node(
                    LEFT,
                    recid2,
                    arrayOf(20, 30, 40, 50, 50),
                    arrayOf(2, 3, 4, 5)
            )
            val recid1 = map.store.put(node1, map.nodeSer)

            val dir = Node(
                    DIR + LEFT + RIGHT,
                    0L,
                    arrayOf(50, 70),
                    longArrayOf(recid1, recid2, recid3)
            )
            val rootRecid = map.store.get(map.rootRecidRecid, Serializer.RECID)!!
            map.store.update(rootRecid, dir, map.nodeSer)
            map.verify()


            map.put(i, i * 100)
            assertEquals(i * 100, map[i])
            map.verify()
        }

    }
}