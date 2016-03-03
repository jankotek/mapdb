package org.mapdb

import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList
import java.util.*
import org.mapdb.BTreeMapJava.*
import org.mapdb.serializer.GroupSerializer

/**
 * Data streaming
 */
object Pump{

    abstract class Consumer<E,R>{

        internal var rootRecidRecid:Long? = null
        internal var counter = 0L

        abstract fun take(e:E)
        abstract fun finish():R

        fun takeAll(i:Iterable<E>){
            takeAll(i.iterator())
        }

        fun takeAll(i:Iterator<E>){
            while(i.hasNext())
                take(i.next())
        }

    }

    fun <K,V> treeMap(
            store:Store,
            keySerializer:GroupSerializer<K>,
            valueSerializer:GroupSerializer<V>,
            comparator:Comparator<K> = keySerializer,
            leafNodeSize:Int = CC.BTREEMAP_MAX_NODE_SIZE*3/4,
            dirNodeSize:Int = CC.BTREEMAP_MAX_NODE_SIZE*3/4
    ): Consumer<Pair<K,V>,Unit>{

        var prevKey:K? = null

        class DirData {
            var leftEdge = LEFT
            var keys = ArrayList<K>()
            var child = LongArrayList()
            var nextDirLink = 0L
        }

        return object: Consumer<Pair<K,V>,Unit>(){

            val dirStack = LinkedList<DirData>()

            val keys = ArrayList<K>()
            val values = ArrayList<V>()
            var leftEdgeLeaf = LEFT
            var nextLeafLink = 0L

            val nodeSer = NodeSerializer(keySerializer, valueSerializer)

            override fun take(e: Pair<K, V>) {
                if(prevKey!=null && comparator.compare(prevKey, e.first)>=0){
                    throw DBException.NotSorted()
                }
                prevKey = e.first
                counter++

                keys.add(e.first)
                values.add(e.second)

                if(keys.size<leafNodeSize)
                    return

                //allocate recid for next link
                val link = store.preallocate()

                //save node
                val node = BTreeMapJava.Node(
                        leftEdgeLeaf + LAST_KEY_DOUBLE,
                        link,
                        keySerializer.valueArrayFromArray(keys.toArray()),
                        valueSerializer.valueArrayFromArray(values.toArray()),
                        keySerializer, valueSerializer
                )
                if(nextLeafLink==0L){
                    nextLeafLink = store.put(node, nodeSer)
                }else {
                    store.update(nextLeafLink, node, nodeSer)
                }

                //modify dirStack
                val lastKey = keys.last()
                var keyFromLowerLevel= lastKey
                var childFromLowerLevel = nextLeafLink
                nextLeafLink = link

                //prepare for next leaf node
                keys.clear()
                keys.add(lastKey)
                leftEdgeLeaf = 0

                values.clear()

                // traverse dirStack and save nodes which are too big
                for(dir in dirStack){

                    //integrate stuff from lower node
                    dir.keys.add(keyFromLowerLevel)
                    dir.child.add(childFromLowerLevel)

                    if(dir.keys.size<dirNodeSize)
                        return

                    //save dir node
                    val link = store.preallocate()
                    val dirNode = Node(
                            dir.leftEdge + DIR,
                            link,
                            keySerializer.valueArrayFromArray(dir.keys.toArray()),
                            dir.child.toArray(),
                            keySerializer, valueSerializer
                    )
                    //save dir
                    if(dir.nextDirLink==0L){
                        dir.nextDirLink = store.put(dirNode, nodeSer)
                    }else {
                        store.update(dir.nextDirLink, dirNode, nodeSer)
                    }

                    //prepare empty node
                    val lastKey = dir.keys.last()

                    keyFromLowerLevel = lastKey
                    childFromLowerLevel = dir.nextDirLink

                    dir.keys.clear()
                    dir.keys.add(lastKey)
                    dir.child.clear()
                    dir.leftEdge = 0
                    dir.nextDirLink = link
                }

                //dir stack overflowed, so add new entry on top
                if(keyFromLowerLevel!=null && childFromLowerLevel!=null){
                    val dir = DirData()
                    dir.keys.add(keyFromLowerLevel)
                    dir.child.add(childFromLowerLevel)
                    dirStack.add(dir)
                }

            }

            override fun finish() {
                //close leaf node
                val endLeaf = BTreeMapJava.Node(
                    leftEdgeLeaf + RIGHT,
                    0L,
                    keySerializer.valueArrayFromArray(keys.toArray()),
                    valueSerializer.valueArrayFromArray(values.toArray()),
                    keySerializer, valueSerializer
                )
                if(nextLeafLink==0L){
                    nextLeafLink = store.put(endLeaf, nodeSer)
                }else {
                    store.update(nextLeafLink, endLeaf, nodeSer)
                }

                //is leaf the only node?
                if(leftEdgeLeaf!=0){
                    //yes, close
                    rootRecidRecid = store.put(nextLeafLink, Serializer.RECID)
                    return
                }

                //finish dir stack
                if(CC.ASSERT && dirStack.last.leftEdge==0)
                    throw AssertionError() //more then one node at top level

                var childFromLowerLevel = nextLeafLink
                for(dir in dirStack){
                    dir.child.add(childFromLowerLevel)
                    val dirNode = Node(
                        dir.leftEdge + RIGHT + DIR,
                        0L,
                        keySerializer.valueArrayFromArray(dir.keys.toArray()),
                        dir.child.toArray(),
                        keySerializer, valueSerializer
                    )
                    //save node
                    if(dir.nextDirLink==0L){
                        dir.nextDirLink = store.put(dirNode, nodeSer)
                    }else {
                        store.update(dir.nextDirLink, dirNode, nodeSer)
                    }
                    childFromLowerLevel = dir.nextDirLink;
                }
                rootRecidRecid = store.put(childFromLowerLevel, Serializer.RECID)
          }
        }
    }

}