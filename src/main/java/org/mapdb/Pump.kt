package org.mapdb

import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList
import org.mapdb.BTreeMapJava.*
import org.mapdb.serializer.GroupSerializer
import java.util.*

/**
 * Data streaming
 */
object Pump{

    abstract class Sink<E,R>{

        //TODO make protected
        internal var rootRecidRecid:Long? = null
        internal var counter = 0L

        abstract fun put(e:E)
        abstract fun create():R

        fun putAll(i:Iterable<E>){
            putAll(i.iterator())
        }

        fun putAll(i:Iterator<E>){
            while(i.hasNext())
                put(i.next())
        }

    }

    fun <K,V> treeMap(
            store:Store,
            keySerializer:GroupSerializer<K>,
            valueSerializer:GroupSerializer<V>,
            comparator:Comparator<K> = keySerializer,
            leafNodeSize:Int = CC.BTREEMAP_MAX_NODE_SIZE*3/4,
            dirNodeSize:Int = CC.BTREEMAP_MAX_NODE_SIZE*3/4,
            hasValues:Boolean=true,
            valueInline:Boolean = true
    ): Sink<Pair<K,V>,Unit>{

        var prevKey:K? = null

        class DirData {
            var leftEdge = LEFT
            var keys = ArrayList<K>()
            var child = LongArrayList()
            var nextDirLink = 0L
        }

        return object: Sink<Pair<K,V>,Unit>(){

            val dirStack = LinkedList<DirData>()

            val keys = ArrayList<K>()
            val values = if(hasValues) ArrayList<V>() else null
            var leftEdgeLeaf = LEFT
            var nextLeafLink = 0L

            val nodeSer = NodeSerializer(keySerializer, comparator,
                    if(valueInline)valueSerializer else Serializer.RECID)

            fun nodeValues():Any {
                return if(!hasValues) keys.size
                    else if(valueInline){
                        //values stored in node
                        valueSerializer.valueArrayFromArray(values!!.toArray())
                    } else {
                        //each value in separate record
                        values!!.map{store.put(it, valueSerializer)}.toLongArray()
                    }
            }


            override fun put(e: Pair<K, V>) {
                if(prevKey!=null && comparator.compare(prevKey, e.first)>=0){
                    throw DBException.NotSorted()
                }
                prevKey = e.first
                counter++

                keys.add(e.first)
                values?.add(e.second)

                if(keys.size<leafNodeSize)
                    return

                //allocate recid for next link
                val link = store.preallocate()

                //save node
                val node = BTreeMapJava.Node(
                        leftEdgeLeaf + LAST_KEY_DOUBLE,
                        link,
                        keySerializer.valueArrayFromArray(keys.toArray()),
                        nodeValues()

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

                values?.clear()

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
                            dir.child.toArray()
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

            override fun create() {
                //close leaf node
                val endLeaf = BTreeMapJava.Node(
                    leftEdgeLeaf + RIGHT,
                    0L,
                    keySerializer.valueArrayFromArray(keys.toArray()),
                    nodeValues()
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
                        dir.child.toArray()
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