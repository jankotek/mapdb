package org.mapdb

import com.gs.collections.api.stack.primitive.LongStack
import com.gs.collections.impl.set.mutable.primitive.LongHashSet
import com.gs.collections.impl.stack.mutable.primitive.LongArrayStack
import org.mapdb.BTreeMapJava.*
import java.util.*

/**
 * Concurrent sorted BTree Map
 */
class BTreeMap<K,V>(
        val keySerializer:Serializer<K>,
        val valueSerializer:Serializer<V>,
        val rootRecidRecid:Long,
        val store:Store,
        val maxNodeSize:Int
):Verifiable{

    companion object{
        fun <K,V> make(
                keySerializer:Serializer<K> = Serializer.JAVA as Serializer<K>,
                valueSerializer:Serializer<V> = Serializer.JAVA as Serializer<V>,
                store:Store = StoreTrivial(),
                rootRecidRecid: Long = //insert recid of new empty node
                    store.put(
                        store.put(
                            Node(LEFT+RIGHT, 0L, arrayOf(), arrayOf<Any>()),
                            NodeSerializer(keySerializer, valueSerializer)),
                        Serializer.RECID),
                maxNodeSize:Int=32) =
            BTreeMap(
                    keySerializer = keySerializer,
                    valueSerializer = valueSerializer,
                    store = store,
                    rootRecidRecid = rootRecidRecid,
                    maxNodeSize = maxNodeSize
            )
    }

    internal val nodeSer = NodeSerializer(keySerializer, valueSerializer);

    internal val rootRecid:Long
        get() = store.get(rootRecidRecid, Serializer.RECID)
                ?: throw DBException.DataCorruption("Root Recid not found");

    operator fun get(key:K):V?{
        var current =  rootRecid
        var A = getNode(current)

        //dive into bottom
        while(A.isDir){
            current =  findChild(A, COMPARATOR, key)
            A = getNode(current)
        }

        //follow link until necessary
        var ret = leafGet(A,COMPARATOR, key)
        while(LINK==ret){
            current = A.link;
            A = getNode(current)
            ret = leafGet(A,COMPARATOR, key)
        }
        return ret as V?;
    }

    fun put(key:K, value:V):V?{
        var completed = false
        val stack = LongArrayStack()

        var current = rootRecid

        var A = getNode(current)
        while(A.isDir){
            var t = current
            current = findChild(A, COMPARATOR, key)
            if(current!=A.link){
                stack.push(t)
            }
            A = getNode(current)
        }

        var level = 1
        do{

            leafLink@ while(true){
                lock(current)

                A = getNode(current)

                //follow link, until key is higher than highest key in node
                if(!A.isRightEdge && COMPARATOR.compare(key, A.keys[A.keys.size-1])>0){
                    //key is greater, load next link
                    unlock(current)
                    current = A.link
                    A = getNode(current)
                    continue@leafLink
                }
                break@leafLink
            }

            //current node is locked, and its highest value is higher/equal to key
            var pos = findValue(A, COMPARATOR, key)
            if(pos>=0){
                pos = pos-1+A.intLeftEdge();
                //key exist in node, just update
                val values = (A.values as Array<Any>).clone()
                val oldValue = values[pos]
                values[pos] = value as Any;
                A = Node(A.flags.toInt(), A.link, A.keys, values)
                store.update(current, A, nodeSer)
                unlock(current)
                return oldValue as V
            }

            //key does not exist, node must be expanded, is it safe to insert without splitting?
            if(A.keys.size < maxNodeSize){
                A = copyAddKey(A, -pos-1, key, value)
                store.update(current, A, nodeSer)
                unlock(current)
                return null
            }

        }while(!completed)

        return null
    }

    private fun copyAddKey(a: Node, insertPos: Int, key: K, value: V): Node {
        val keys = arrayPut(a.keys, insertPos, key)

        val valuesInsertPos = insertPos-1+a.intLeftEdge();
        val values = arrayPut(a.values as Array<Any>, valuesInsertPos, value)

        return Node(a.flags.toInt(), a.link, keys, values)
    }


    fun lock(nodeRecid:Long){

    }

    fun unlock(nodeRecid:Long){

    }

    override fun verify() {
        val rootRecid = rootRecid
        val node = getNode(rootRecid)

        val knownNodes = LongHashSet.newSetWith(rootRecid)

        verifyRecur(node, left=true, right=true, knownNodes=knownNodes)
    }


    private fun verifyRecur(node:Node, left:Boolean, right:Boolean, knownNodes:LongHashSet){
        if(left!=node.isLeftEdge)
            throw AssertionError("left does not match $left")
        if(right!=node.isRightEdge)
            throw AssertionError("right does not match $right")

        //check keys are sorted, no duplicates
        for(i in 1 until node.keys.size){
            val compare = COMPARATOR.compare(node.keys[i-1], node.keys[i])
            val cresult = if(i==node.keys.size-1) 1 else 0
            if(compare>=cresult)
                throw AssertionError("Not sorted: "+Arrays.toString(node.keys))
        }

        //iterate over child
        if(node.isDir){
            val child = node.values as LongArray
            for(i in 0 until child.size){
                val recid = child[i]

                if(knownNodes.contains(recid))
                    throw AssertionError()
                knownNodes.add(recid)
                val node = getNode(recid)
                verifyRecur(node, left = (i==0), right= (child.size==i+1), knownNodes = knownNodes)

                //TODO follow link until next node is found
                //val linkEnd = if(i==child.size-1) 0L else child[i+1]
            }
        }
    }

    private fun getNode(nodeRecid:Long) =
            store.get(nodeRecid, nodeSer)
                ?: throw DBException.DataCorruption("Node not found")

}